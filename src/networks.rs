//! Module for the network related types.

pub(crate) mod amqp;
pub(crate) mod utils;

use crate::{
	config::{AsbConfig, NetworkKind, ReliabilityQos, WireFormat},
	error::CalError,
	networks::{
		amqp::{ChanCb, ConnCb},
		utils::{AsbConnStatus, AsbStatusListener, PossessCtr, StatusCallbackManager},
	},
};
use amqp::{AmqpConsumer, open_args_for_net};
use amqprs::{
	BasicProperties,
	channel::{
		BasicCancelArguments, BasicConsumeArguments, BasicPublishArguments,
		ExchangeDeclareArguments, QueueBindArguments, QueueDeclareArguments,
	},
	connection::Connection,
};
use crossbeam_channel::{RecvTimeoutError, TryRecvError};
use crossbeam_ring_channel::{RingReceiver, RingSender};
use serde::{Deserialize, Serialize};
use std::{
	any,
	collections::{HashMap, hash_map},
	marker::PhantomData,
	ops::Deref,
	sync::{Arc, Mutex},
	time::{Duration, Instant},
};
use tokio::sync::Notify;

/// Manages the transport-specific data and lifetime.
pub enum AsbNetMode {
	Amqp(Arc<amqp::AmqpAsb>, Arc<Notify>),
	Null,
}
impl Drop for AsbNetMode {
	fn drop(&mut self) {
		match self {
			AsbNetMode::Amqp(asb, n) => {
				asb.rt_handle.block_on(async {
					// Close channel and connection.
					_ = asb.chan.clone().close().await;
					_ = asb.conn.clone().close().await;
				});

				// Notify
				n.notify_waiters();
			}
			_ => {}
		};
	}
}

/// Manages and maintains a single ASB connection.
// TODO: Status var shared with background thread.
pub struct AsbConnection {
	/// The transport-specific things.
	net: AsbNetMode,
	/// Map of topic name to (count, topic_type). Mutex for flexibility.
	topics: Mutex<HashMap<String, (PossessCtr, any::TypeId)>>,
	status_manager: Arc<StatusCallbackManager>,
}
impl AsbConnection {
	pub fn connect(net_name: &str, config: &AsbConfig) -> Result<Self, CalError> {
		let Some(network) = config.networks.get(net_name) else {
			return Err(CalError::config_err(format_args!(
				"Missing network config for {net_name}"
			)));
		};

		// Create status manager so networks can clone as needed.
		let status_manager = Arc::new(StatusCallbackManager::new());
		// Always starts as Normal since this function will return an error if it
		// would be in any other state upon completion.
		status_manager.set_status(AsbConnStatus::Normal);

		match network.kind {
			NetworkKind::Amqp => {
				// Create current thread flavor runtime for now.
				// TODO: Consider feature or config to choose runtime flavor.
				let rt = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()?;
				let rt_handle = rt.handle().clone();

				// Check configuration for exchange and durability parameter.
				let exchange = match network.params.get("exchange") {
					Some(toml::Value::String(ex)) if !ex.is_empty() => Some(ex.to_owned()),
					Some(_) => {
						return Err(CalError::config_err(format_args!(
							"AMQP parameter \"exchange\" must be a non-empty string."
						)));
					}
					None => None,
				};
				let durable = match network.params.get("durable_exchange") {
					Some(toml::Value::Boolean(ex)) => *ex,
					Some(_) => {
						return Err(CalError::config_err(format_args!(
							"AMQP parameter \"durable_exchange\" must be a boolean."
						)));
					}
					None => true,
				};

				// Prepare callbacks
				let conn_cb = ConnCb {
					status_manager: status_manager.clone(),
				};
				let chan_cb = ChanCb {
					status_manager: status_manager.clone(),
				};

				// Open the connection and create a single channel for everything.
				let open_args = open_args_for_net(&network)?;
				let (conn, chan) = rt.block_on(async {
					let conn = Connection::open(&open_args).await?;
					conn.register_callback(conn_cb).await?;
					let chan = conn.open_channel(None).await?;
					chan.register_callback(chan_cb).await?;
					chan.flow(true).await?; // Kickstart traffic flowing

					// If config has exchange name, create direct exchange.
					if let Some(ref ex) = exchange {
						let declare_args = ExchangeDeclareArguments::of_type(
							ex,
							amqprs::channel::ExchangeType::Direct,
						)
						.durable(durable)
						.finish();

						chan.exchange_declare(declare_args).await?;
					}

					Ok::<_, amqprs::error::Error>((conn, chan))
				})?;

				// Spawn background thread to drive the tokio runtime.
				let notifier = Arc::new(Notify::new());
				let bg_notifier = notifier.clone();
				std::thread::spawn(move || rt.block_on(bg_notifier.notified()));

				Ok(AsbConnection {
					net: AsbNetMode::Amqp(
						Arc::new(amqp::AmqpAsb {
							rt_handle,
							conn,
							chan,
							exchange,
						}),
						notifier,
					),
					topics: Default::default(),
					status_manager,
				})
			}
			NetworkKind::Null => Ok(AsbConnection {
				net: AsbNetMode::Null,
				topics: Default::default(),
				status_manager,
			}),
		}
	}

	/* Reader/Writer */

	fn get_topic_ctr<T: 'static>(&self, topic: &str) -> Result<PossessCtr, CalError> {
		// Lock the map first
		let mut map = self.topics.lock().unwrap();
		let entry = map.entry(topic.to_owned());

		// Check entry and return counter if valid, otherwise return error.
		match entry {
			hash_map::Entry::Occupied(o) => {
				let (count, ty) = o.get();

				// If the type does not match, then it will only be allowed if counter is
				// unique.
				if any::TypeId::of::<T>() != *ty && !count.is_unique() {
					return Err(CalError::ill_err("Type mismatch with existing readers"));
				}

				// Valid reader, so clone the counter for the reader.
				Ok(count.clone())
			}
			hash_map::Entry::Vacant(v) => {
				let counter = PossessCtr::new();
				v.insert((counter.clone(), any::TypeId::of::<T>()));
				Ok(counter)
			}
		}
	}

	pub fn create_reader<T: for<'de> Deserialize<'de> + Send + Sync + 'static>(
		&self,
		topic: &str,
		config: &AsbConfig,
		svc_name: &str,
	) -> Result<AsbReader<T>, CalError> {
		// Check whether topic exists, and if so, ensure that `T` matches.
		let counter = self.get_topic_ctr::<T>(topic)?;

		// Get the config for this service
		let service_cfg = config.services.service.get(svc_name);

		// Check for the wire format.
		let wire_format = service_cfg
			.and_then(|cfg| {
				cfg.wire_format
					.as_ref()
					.or(config.services.default_wire_format.as_ref())
			})
			.ok_or(CalError::config_err(format_args!(
				"No wire format specified for topic {topic} under service {svc_name}."
			)))?;

		// Get the QoS config for `topic`, or use the default.
		let qos = service_cfg
			.and_then(|cfg| {
				// Try to get the a QoS name to lookup.
				cfg.topics
					.get(topic)
					.and_then(|tcfg| tcfg.qos.as_ref())
					// otherwise use the service-level qos
					.or(cfg.qos.as_ref())
					// otherwise use the default
					.or(config.services.default_qos.as_ref())
			})
			// Map name to actual [QosSettings], error if it's not in the config.
			// Use the default QoS otherwise.
			.map_or(Ok(Default::default()), |name| {
				config
					.qos
					.get(name)
					.map(|q| *q)
					.ok_or(CalError::config_err(format_args!(
						"Could not find QoS settings for {name}"
					)))
			})?;

		// Get the topic name for the bus.
		let topic_name = service_cfg
			.and_then(|cfg| {
				// Try to get the bus topic
				cfg.topics
					.get(topic)
					.and_then(|tcfg| tcfg.bus_topic.as_ref())
					.map(|s| s.as_str())
			})
			.unwrap_or(topic);

		// Do the network-specific setup for a reader.
		match &self.net {
			AsbNetMode::Amqp(asb, _) => {
				// If no exchange specified use topic name, otherwise let the broker name
				// it.
				let queue_name = match asb.exchange.is_some() {
					true => "",
					false => topic_name,
				};

				// Prepare declare queue args.
				// If `auto_delete` desired, then `exclusive` must be true to avoid error
				// with RabbitMQ due to deprecated combination.
				let declare_args = QueueDeclareArguments::new(queue_name)
					.exclusive(true)
					.auto_delete(true)
					.finish();

				// Determine ACK based on QoS. True means server assumes delivery.
				let auto_ack = match qos.reliability {
					ReliabilityQos::BestEffort => true,
					ReliabilityQos::Reliable => false,
				};

				// Create the ring buffer for the reader and consumer.
				// Buffer size is max(qos, 1) since size of 0 is invalid.
				let (prod, cons) = crossbeam_ring_channel::ring_bounded(qos.buffer.max(1));
				let all_senders = Arc::new(Mutex::new(vec![(0, prod)]));
				let consumer = AmqpConsumer {
					format: *wire_format,
					buffers: all_senders.clone(),
					qos,
					last_received: None,
				};

				// Do all the actual network stuff here and save tag for deleting consumer.
				let tag = asb.rt_handle.block_on(async {
					// Declare queue
					// Safety: We do not set `no_wait` above.
					let res = asb.chan.queue_declare(declare_args).await?.unwrap();

					// Prepare the consumer arguments for the new queue. Use returned result
					// to guarantee queue name is correct.
					let consume_args = BasicConsumeArguments::new(&res.0, "")
						.auto_ack(auto_ack)
						.finish();

					// If an exchange is specified, bind queue to it.
					if let Some(ref ex) = asb.exchange {
						let args = QueueBindArguments::new(&res.0, &ex, topic_name);
						asb.chan.queue_bind(args).await?;
					}

					// Create consumer for topic (subscribe).
					let tag = asb.chan.basic_consume(consumer, consume_args).await?;

					Ok::<_, amqprs::error::Error>(tag)
				})?;

				Ok(AsbReader {
					buffer: cons,
					expiration: qos.expiration,
					all_senders,
					my_sender_id: 0,
					net: Arc::new(AsbReaderNet::Amqp(asb.clone(), tag)),
					callback_handle: None,
					listeners: Default::default(),
					counter,
				})
			}
			AsbNetMode::Null => {
				// Construct empty ring buffer since null does nothing.
				let (_, cons) = crossbeam_ring_channel::ring_bounded(0);

				Ok(AsbReader {
					buffer: cons,
					expiration: qos.expiration,
					all_senders: Default::default(),
					my_sender_id: 0,
					net: Arc::new(AsbReaderNet::Null),
					callback_handle: None,
					listeners: Default::default(),
					counter,
				})
			}
		}
	}

	pub fn create_writer<T: 'static>(
		&self,
		topic: &str,
		config: &AsbConfig,
		svc_name: &str,
	) -> Result<AsbWriter<T>, CalError> {
		// Check whether topic exists, and if so, ensure that `T` matches.
		let counter = self.get_topic_ctr::<T>(topic)?;

		// Get the config for this service
		let service_cfg = config.services.service.get(svc_name);

		// Check for the wire format.
		let wire_format = service_cfg
			.and_then(|cfg| {
				cfg.wire_format
					.as_ref()
					.or(config.services.default_wire_format.as_ref())
			})
			.ok_or(CalError::config_err(format_args!(
				"No wire format specified for topic {topic} under service {svc_name}."
			)))?;

		// Get the topic name for the bus.
		let topic_name = service_cfg
			.and_then(|cfg| {
				// Try to get the bus topic
				cfg.topics
					.get(topic)
					.and_then(|tcfg| tcfg.bus_topic.as_ref())
					.map(|s| s.as_str())
			})
			.unwrap_or(topic);

		match &self.net {
			AsbNetMode::Amqp(asb, _) => {
				let exchange_name = asb
					.exchange
					.as_ref()
					.map(|s| s.as_ref())
					.unwrap_or_default();

				// Create the publish parameters
				let props = BasicProperties::default();
				let args = BasicPublishArguments::new(exchange_name, topic_name);

				Ok(AsbWriter {
					net: AsbWriterNet::Amqp(asb.clone(), props, args),
					format: *wire_format,
					counter,
					_asb: PhantomData,
				})
			}
			AsbNetMode::Null => Ok(AsbWriter {
				net: AsbWriterNet::Null,
				// No default for [WireFormat] so just picking Xml since it's the first.
				format: WireFormat::Xml,
				counter,
				_asb: PhantomData,
			}),
		}
	}

	/* Status functions */
	pub fn get_status(&self) -> AsbConnStatus {
		self.status_manager.get_status()
	}

	pub fn add_status_listener(&self, fun: impl AsbStatusListener) -> u32 {
		self.status_manager.add_listener(fun)
	}

	pub fn remove_status_listener(&self, id: u32) -> bool {
		self.status_manager.remove_listener(id)
	}
}

/// Types that are capable of being used as a listener for [AsbReader].
pub trait AsbListener<T>: Send + 'static {
	/// This function is called anytime the associated reader receives a message.
	fn on_msg(&self, msg: Arc<T>);
}
/// Convenience implementation for simple listeners.
impl<T, F: Fn(Arc<T>) + Send + 'static> AsbListener<T> for F {
	/// Simply calls this closure.
	fn on_msg(&self, msg: Arc<T>) {
		self(msg);
	}
}
/// Convenience implementation for shared listeners.
impl<M, T: AsbListener<M> + Sync> AsbListener<M> for Arc<T> {
	/// Simply calls the function defined on the inner type `T`.
	fn on_msg(&self, msg: Arc<M>) {
		self.deref().on_msg(msg);
	}
}

/// Provides messages received from the ASB through a polling interface.
///
/// **IMPORTANT**: If the network type is "null" then every read will error.
pub struct AsbReader<T> {
	/// The buffer this reader reads from.
	buffer: RingReceiver<(Instant, Arc<T>)>,
	/// Optional expiration time after which older messages should be discarded.
	expiration: Option<Duration>,
	/// Shared with consumer for this topic. `u32` is random to identify sender
	/// for this [AsbReader].
	all_senders: Arc<Mutex<Vec<(u32, RingSender<(Instant, Arc<T>)>)>>>,
	my_sender_id: u32,
	/// Arc so that any unsubscribes happen only after last reader drops.
	net: Arc<AsbReaderNet>,
	/// Whether this reader has registered listeners and should disallow `read()`.
	callback_handle: Option<PossessCtr>,
	/// All registered listeners keyed by a random number.
	listeners: Arc<Mutex<Vec<(u32, Box<dyn AsbListener<T>>)>>>,
	/// Not used, simply holds so [AsbConnection] can track topic usage.
	counter: PossessCtr,
}
impl<T> AsbReader<T> {
	fn callback_mode_error(&self) -> Result<(), CalError> {
		match self.callback_handle.is_some() {
			true => Err(CalError::ill_err("Reader has active listeners")),
			false => Ok(()),
		}
	}
	/// Read the next message from the buffer or block until there is one.
	pub fn read(&self) -> Result<Arc<T>, CalError> {
		// Error if in callback mode.
		self.callback_mode_error()?;

		let err = CalError::other_err("Reader closed unexpectedly");

		// Do actual read.
		if let Some(expiration) = self.expiration {
			loop {
				// Try to receive message
				let Ok((t, msg)) = self.buffer.recv() else {
					return Err(err);
				};

				// If within the expiration window, then ok to return message.
				if t.elapsed() <= expiration {
					return Ok(msg);
				}
			}
		} else {
			// Receive and ignore timestamp.
			self.buffer.recv().map(|(_, m)| m).map_err(|_| err)
		}
	}

	/// Read the next message from the buffer or block until one is received or `timeout` is reached.
	pub fn read_timeout(&self, timeout: Duration) -> Result<Option<Arc<T>>, CalError> {
		// Error if in callback mode.
		self.callback_mode_error()?;

		// The error to return if the buffer is closed.
		let err = CalError::other_err("Reader closed unexpectedly");

		// Split logic depending on whether there is an expiration check.
		if let Some(expiration) = self.expiration {
			let start = Instant::now();
			let mut remaining = timeout;

			while !remaining.is_zero() {
				match self.buffer.recv_timeout(remaining) {
					Ok((t, msg)) => {
						// If within the expiration window, then ok to return message.
						if t.elapsed() <= expiration {
							return Ok(Some(msg));
						} else {
							remaining = timeout.saturating_sub(start.elapsed());
						}
					}
					// If timeout, we should return without error. Else return error.
					Err(e) => match e {
						RecvTimeoutError::Timeout => break,
						_ => return Err(err),
					},
				};
			}

			Ok(None)
		} else {
			// If no expiration, ignore the timestamp and just return next message.
			match self.buffer.recv_timeout(timeout) {
				Ok(m) => Ok(Some(m.1)),
				Err(e) => match e {
					RecvTimeoutError::Timeout => Ok(None),
					_ => Err(CalError::other_err("Reader closed unexpectedly")),
				},
			}
		}
	}

	/// Read the next message from the buffer if there is one. Does not block.
	pub fn try_read(&self) -> Result<Option<Arc<T>>, CalError> {
		// Error if in callback mode.
		self.callback_mode_error()?;

		// The error to return if the buffer is closed.
		let err = CalError::other_err("Reader closed unexpectedly");

		// Split logic depending on whether there is an expiration check.
		if let Some(expiration) = self.expiration {
			loop {
				match self.buffer.try_recv() {
					Ok((t, msg)) => {
						// If within the expiration window, then ok to return message.
						if t.elapsed() <= expiration {
							return Ok(Some(msg));
						}
					}
					// If timeout, we should return without error. Else return error.
					Err(e) => match e {
						TryRecvError::Empty => return Ok(None),
						_ => return Err(err),
					},
				};
			}
		} else {
			// If no expiration, ignore the timestamp and just return next message.
			match self.buffer.try_recv() {
				Ok(m) => Ok(Some(m.1)),
				Err(e) => match e {
					TryRecvError::Empty => Ok(None),
					_ => Err(err),
				},
			}
		}
	}
}
impl<T: Send + Sync + 'static> AsbReader<T> {
	/// Register a function to be called whenever a new message is received.
	///
	/// **IMPORTANT**: All listeners on this reader share a thread.
	// Implementation notes:
	//
	// Using `recv_timeout` to ensure the bg thread stops in a timely manner isn't
	// the prettiest solution, however it currently seems the most sensible. Below
	// are two alternatives considered, though both are similar in implementation.
	//
	// # Alternative 1 (swapping ring buffers)
	// This approach would involve the creation of a new ringbuffer whose sender
	// replaces the current reader's sender, and whose receiver goes to the
	// thread. When [remove_listener] goes to stop the thread, it simply undoes
	// this swap and drops the bg thread sender. This would allow the bg thread to
	// use [recv] in a loop with no extra conditionals, since it will error as
	// soon as the sender is dropped in [remove_listener]. However, this would
	// also mean that some messages may be lost if they were in the bg thread's
	// buffer before they could be dispatched. If [remove_listener] were to read
	// and forward all remaining messages to the reader's own buffer, it would
	// prolong the time that the [Mutex] for `all_senders` is held which is not
	// desirable. In the alternate case that it releases the lock to do so, some
	// messages may be missed entirely; if the reader added its sender back before
	// copying, message ordering would be broken.
	//
	// # Alternative 2 (additional ring buffer)
	// This approach is a simpler version of alternative 1, because it would take
	// the new ringbuffer and simply add the sender to the `all_senders` list.
	// This avoids all of the message loss problems, however it arguably breaks
	// the ordering again because there are now messages in the reader's buffer
	// that were already processed by the bg thread, leading to duplicates until
	// they are read/overwritten. This also obviously increases the length of
	// `all_senders`, potentially up to double if every reader is used with
	// listeners.
	pub fn add_listener(&mut self, fun: impl AsbListener<T>) -> u32 {
		// Add function to listeners vec.
		let id = rand::random();
		{
			let mut listeners = self.listeners.lock().unwrap();
			listeners.push((id, Box::new(fun)));
		}

		// Start background thread if we haven't already
		let bg_listeners = self.listeners.clone();
		let receiver = self.buffer.clone();
		let expire = self.expiration;
		if self.callback_handle.is_none() {
			let handle = PossessCtr::new();
			self.callback_handle = Some(handle.clone());
			std::thread::spawn(move || {
				// TODO: Do some basic profiling to find a good value for this.
				//       Shorter makes loop more "busy" but gives better reaction in cases
				//       of infrequent message reception, longer does the opposite.
				const RECV_TMOUT: Duration = Duration::from_millis(100);

				// Have conditional outside of loop since this can be a very hot loop.
				if let Some(expiration) = expire {
					loop {
						// When reader drops its counter, this will trigger to stop the loop.
						if handle.is_unique() {
							break;
						}

						// Receive on timeout so above conditional is checked periodically.
						match receiver.recv_timeout(RECV_TMOUT) {
							Ok((t, msg)) => {
								if t.elapsed() <= expiration {
									for l in bg_listeners.lock().unwrap().iter_mut() {
										l.1.on_msg(msg.clone());
									}
								}
							}
							// If disconnected break loop.
							Err(RecvTimeoutError::Disconnected) => break,
							_ => {}
						}
					}
				} else {
					loop {
						// When reader drops its counter, this will trigger to stop the loop.
						if handle.is_unique() {
							break;
						}

						// Receive on timeout so above conditional is checked periodically.
						match receiver.recv_timeout(RECV_TMOUT) {
							Ok((_, msg)) => {
								for l in bg_listeners.lock().unwrap().iter() {
									l.1.on_msg(msg.clone());
								}
							}
							// If disconnected break loop.
							Err(RecvTimeoutError::Disconnected) => break,
							_ => {}
						}
					}
				}
			});
		}

		// Return ID to user so they can remove listener later
		id
	}

	/// Remove the listener identified with `id`, returning `true` if it exists.
	pub fn remove_listener(&mut self, id: u32) -> bool {
		let mut listeners = self.listeners.lock().unwrap();
		if let Some(idx) = listeners.iter().position(|b| b.0 == id) {
			_ = listeners.swap_remove(idx);

			// If no more listeners, empty callback handle so bg thread will stop and
			// reads can continue as normal.
			if listeners.is_empty() {
				_ = self.callback_handle.take();
			}

			true
		} else {
			false
		}
	}
}
impl<T> Clone for AsbReader<T> {
	fn clone(&self) -> Self {
		// Create the ring buffer
		let (prod, cons) = crossbeam_ring_channel::ring_bounded(self.buffer.capacity());
		let my_sender_id = rand::random();

		// Add producer to shared vec
		{
			let mut buffers = self.all_senders.lock().unwrap();
			buffers.push((my_sender_id, prod));
		}

		AsbReader {
			buffer: cons,
			expiration: self.expiration,
			all_senders: self.all_senders.clone(),
			my_sender_id,
			net: self.net.clone(),
			callback_handle: None,
			listeners: Arc::new(Mutex::new(Vec::new())),
			counter: self.counter.clone(),
		}
	}
}
impl<T> Drop for AsbReader<T> {
	fn drop(&mut self) {
		// Simply remove sender for this reader
		{
			let mut buffers = self.all_senders.lock().unwrap();
			// This conditional should never fail, but do proper checking just in case.
			if let Some(idx) = buffers.iter().position(|b| b.0 == self.my_sender_id) {
				buffers.swap_remove(idx);
			}
		}
	}
}

/// Holds all network-specific data to manage the reader/subscriber.
pub enum AsbReaderNet {
	// .1 is consumer tag
	Amqp(Arc<amqp::AmqpAsb>, String),
	Null,
}
impl Drop for AsbReaderNet {
	fn drop(&mut self) {
		match self {
			AsbReaderNet::Amqp(asb, tag) => {
				let cancel = BasicCancelArguments::new(tag);
				_ = asb.rt_handle.block_on(asb.chan.basic_cancel(cancel));
			}
			AsbReaderNet::Null => {}
		}
	}
}

/// Publishes messages to the ASB on the topic specified during construction.
#[derive(Clone)]
pub struct AsbWriter<T> {
	net: AsbWriterNet,
	format: WireFormat,
	counter: PossessCtr,
	_asb: PhantomData<T>,
}
#[derive(Clone)]
pub enum AsbWriterNet {
	Amqp(Arc<amqp::AmqpAsb>, BasicProperties, BasicPublishArguments),
	Null,
}
impl<T: Serialize> AsbWriter<T> {
	/// Publishes `msg` to the topic specified in [create_writer()](AsbConnection::create_writer).
	pub fn write(&self, msg: &T) -> Result<(), CalError> {
		match &self.net {
			AsbWriterNet::Amqp(asb, props, args) => {
				let data = crate::msg_serde::serialize_msg(&self.format, msg)?;

				Ok(asb.rt_handle.block_on(asb.chan.basic_publish(
					props.clone(),
					data,
					args.clone(),
				))?)
			}
			AsbWriterNet::Null => Ok(()),
		}
	}
}
