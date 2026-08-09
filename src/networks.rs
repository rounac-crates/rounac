//! Module for the network related types.

mod amqp;
mod loopback;
mod mqtt;
pub(crate) mod utils;

use crate::{
	config::{AsbConfig, NetworkConfig, NetworkKind, ReliabilityQos, WireFormat},
	error::CalError,
	networks::{
		amqp::{ChanCb, ConnCb},
		mqtt::MqttAsb,
		utils::{
			AsbConnStatus, AsbStatusListener, PossessCtr, ReaderRingMaster, StatusCallbackManager,
		},
	},
};
use amqp::{AmqpConsumer, open_args_for_net};
use amqprs::{
	BasicProperties,
	channel::{
		BasicConsumeArguments, BasicPublishArguments, ExchangeDeclareArguments, QueueBindArguments,
		QueueDeclareArguments,
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
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, AtomicU16, Ordering},
	},
	time::{Duration, Instant},
};
use tokio::sync::Notify;

/// Manages the transport-specific data and lifetime.
enum AsbNetMode {
	Amqp(Arc<amqp::AmqpAsb>, Arc<Notify>),
	Mqtt(Arc<mqtt::MqttAsb>, Arc<Notify>),
	Loopback(Arc<loopback::LoopbackAsb>),
	Null,
}
impl AsbNetMode {
	/// Create new object with variant [Amqp].
	fn new_amqp(
		config: &NetworkConfig,
		status_manager: &Arc<StatusCallbackManager>,
	) -> Result<Self, CalError> {
		// Create current thread flavor runtime for now.
		// TODO: Consider feature or config to choose runtime flavor.
		let rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()?;
		let rt_handle = rt.handle().clone();

		// Check configuration for exchange and durability parameter.
		let exchange = match config.params.get("exchange") {
			Some(toml::Value::String(ex)) if !ex.is_empty() => Some(ex.to_owned()),
			Some(_) => {
				return Err(CalError::config_err(format_args!(
					"AMQP parameter \"exchange\" must be a non-empty string."
				)));
			}
			None => None,
		};
		let durable = match config.params.get("durable_exchange") {
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
		let open_args = open_args_for_net(config)?;
		let (conn, chan) = rt.block_on(async {
			let conn = Connection::open(&open_args).await?;
			conn.register_callback(conn_cb).await?;
			let chan = conn.open_channel(None).await?;
			chan.register_callback(chan_cb).await?;
			chan.flow(true).await?; // Kickstart traffic flowing

			// If config has exchange name, create direct exchange.
			if let Some(ref ex) = exchange {
				let declare_args =
					ExchangeDeclareArguments::of_type(ex, amqprs::channel::ExchangeType::Direct)
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

		Ok(AsbNetMode::Amqp(
			Arc::new(amqp::AmqpAsb::new(rt_handle, conn, chan, exchange)),
			notifier,
		))
	}

	/// Create new object with variant [Mqtt].
	fn new_mqtt(
		config: &NetworkConfig,
		status_manager: &Arc<StatusCallbackManager>,
	) -> Result<Self, CalError> {
		// Create current thread flavor runtime for now.
		// TODO: Consider feature or config to choose runtime flavor.
		let rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()?;
		let rt_handle = rt.handle().clone();

		// Get the MQTT options from config.
		let opts = mqtt::get_mqtt_opts(config)?;

		// Create the client and event loop (nothing is sent yet).
		// Docs recommend [AsyncClient] capacity of 0.
		let (client, mut evt_loop) = rumqttc::AsyncClient::new(opts, 0);
		let mqtt_asb = Arc::new(MqttAsb::new(rt_handle, client));

		// Make bg clones.
		let bg_mqtt_asb = mqtt_asb.clone();
		let (init_send, init_recv) = std::sync::mpsc::sync_channel(0);
		let bg_status_mgr = status_manager.clone();

		// Spawn the message handling task on the runtime. This ensures it gets a
		// worker thread (in the case of multi-threaded).
		_ = rt.spawn(async move {
			loop {
				match evt_loop.poll().await {
					Ok(rumqttc::Event::Incoming(evt)) => {
						match evt {
							rumqttc::Incoming::ConnAck(_) => {
								init_send.send(None).unwrap();
							}
							rumqttc::Incoming::Publish(p) => {
								// Distribute message to readers.
								bg_mqtt_asb.handle_msg(&p.topic, &p.payload);
							}
							_ => {}
						}
					}
					// This gets sent by [Drop] impl, so stop loop.
					Ok(rumqttc::Event::Outgoing(rumqttc::Outgoing::Disconnect)) => {
						break;
					}
					// Generally don't care about outgoing events.
					Ok(rumqttc::Event::Outgoing(_)) => {}
					Err(e) => {
						_ = init_send.send(Some(e));
						bg_mqtt_asb.shutdown();
						bg_status_mgr.set_status(AsbConnStatus::Failed);
						break;
					}
				}
			}
		});

		// Spawn background thread to drive the tokio runtime.
		let notifier = Arc::new(Notify::new());
		let bg_notifier = notifier.clone();
		std::thread::spawn(move || rt.block_on(bg_notifier.notified()));

		// If bad connection, stop bg thread and return error.
		if let Some(err) = init_recv.recv().unwrap() {
			notifier.notify_waiters();

			return Err(CalError::net_err(err));
		}

		Ok(AsbNetMode::Mqtt(mqtt_asb, notifier))
	}

	pub fn new_loopback(
		config: &NetworkConfig,
		status_manager: &Arc<StatusCallbackManager>,
	) -> Result<Self, CalError> {
		Ok(AsbNetMode::Loopback(Arc::new(loopback::LoopbackAsb::new())))
	}
}
impl Drop for AsbNetMode {
	fn drop(&mut self) {
		match self {
			AsbNetMode::Amqp(asb, n) => {
				asb.shutdown();

				asb.rt_handle.block_on(async {
					// Close channel and connection.
					_ = asb.chan.clone().close().await;
					_ = asb.conn.clone().close().await;
				});

				// Notify
				n.notify_waiters();
			}
			AsbNetMode::Mqtt(asb, n) => {
				asb.shutdown();

				asb.rt_handle.block_on(async {
					_ = asb.client.disconnect().await;
				});

				n.notify_waiters();
			}
			AsbNetMode::Loopback(asb) => asb.shutdown(),
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
			NetworkKind::Amqp => Ok(AsbConnection {
				net: AsbNetMode::new_amqp(network, &status_manager)?,
				topics: Default::default(),
				status_manager,
			}),
			NetworkKind::Mqtt => Ok(AsbConnection {
				net: AsbNetMode::new_mqtt(network, &status_manager)?,
				topics: Default::default(),
				status_manager,
			}),
			NetworkKind::Loopback => Ok(AsbConnection {
				net: AsbNetMode::new_loopback(network, &status_manager)?,
				topics: Default::default(),
				status_manager,
			}),
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
					.copied()
					.ok_or(CalError::config_err(format_args!(
						"Could not find QoS settings for {name}"
					)))
			})?;

		// Get the topic name for the bus.
		let bus_topic = service_cfg
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
				let reader_ringmaster: Arc<ReaderRingMaster<T>> = match asb.get_clone_for(bus_topic)
				{
					// There is a reader somewhere.
					// SAFETY: [get_topic_ctr] at start of fn ensures TypeId of `T` matches
					//         any readers on this topic, therefore [`ReaderRingMaster<T>`]
					//         is exactly the same as stored in `asb`.
					Some(r) => r.into_arc_any().downcast().unwrap(),
					// First of its kind for this topic.
					None => {
						// If no exchange specified use topic name, otherwise let the broker name
						// it.
						let queue_name = match asb.exchange.is_some() {
							true => "",
							false => bus_topic,
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

						// Create consumer.
						let consumer = AmqpConsumer {
							qos,
							topic: bus_topic.to_string(),
							asb: asb.clone(),
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
								let args = QueueBindArguments::new(&res.0, ex, bus_topic);
								asb.chan.queue_bind(args).await?;
							}

							// Create consumer for topic (subscribe).
							let tag = asb.chan.basic_consume(consumer, consume_args).await?;

							Ok::<_, amqprs::error::Error>(tag)
						})?;

						let rng_mstr = Arc::new(ReaderRingMaster::new(*wire_format, qos));

						// Add ringmaster to bg thread
						asb.readers.write().unwrap().insert(
							bus_topic.to_string(),
							(rng_mstr.clone(), AtomicU16::new(0), tag),
						);

						rng_mstr
					}
				};

				// Increment the ASB reader counter.
				// SAFETY: Above match statement ensures there is an entry for `bus_topic`.
				asb.add_reader(bus_topic).unwrap();

				// Create the ring buffer for the reader and consumer.
				// Buffer size is max(qos, 1) since size of 0 is invalid.
				let (prod, cons) = crossbeam_ring_channel::ring_bounded(qos.buffer.max(1));
				let my_sender_id = reader_ringmaster.add_sender(prod);

				Ok(AsbReader {
					buffer: cons,
					expiration: qos.expiration,
					ringmaster: reader_ringmaster,
					my_sender_id,
					net: Arc::new(AsbReaderNet::Amqp(asb.clone(), bus_topic.to_string())),
					callbacks_active: Arc::new(AtomicBool::default()),
					listeners: Default::default(),
					counter,
				})
			}
			AsbNetMode::Mqtt(asb, _) => {
				let reader_ringmaster: Arc<ReaderRingMaster<T>> = match asb.get_clone_for(bus_topic)
				{
					// There is a reader somewhere.
					// SAFETY: [get_topic_ctr] at start of fn ensures TypeId of `T` matches
					//         any readers on this topic, therefore [`ReaderRingMaster<T>`]
					//         is exactly the same as stored in `asb`.
					Some(r) => r.into_arc_any().downcast().unwrap(),
					// First of its kind for this topic.
					None => {
						// Choose QoS accordingly.
						let mqtt_qos = match qos.reliability {
							ReliabilityQos::BestEffort => rumqttc::QoS::AtMostOnce,
							ReliabilityQos::Reliable => rumqttc::QoS::AtLeastOnce,
						};

						// Subscribe to topic
						_ = asb
							.rt_handle
							.block_on(asb.client.subscribe(bus_topic, mqtt_qos));
						// TODO: Figure out how to wait till SubAck is received.

						let rng_mstr = Arc::new(ReaderRingMaster::new(*wire_format, qos));

						// Add ringmaster to bg thread
						asb.readers.write().unwrap().insert(
							bus_topic.to_string(),
							(rng_mstr.clone(), AtomicU16::new(0), bus_topic.to_string()),
						);

						rng_mstr
					}
				};

				// Increment the ASB reader counter.
				// SAFETY: Above match statement ensures there is an entry for `bus_topic`.
				asb.add_reader(bus_topic).unwrap();

				let (prod, cons) = crossbeam_ring_channel::ring_bounded(qos.buffer.max(1));
				let my_sender_id = reader_ringmaster.add_sender(prod);

				Ok(AsbReader {
					buffer: cons,
					expiration: qos.expiration,
					ringmaster: reader_ringmaster,
					my_sender_id,
					net: Arc::new(AsbReaderNet::Mqtt(asb.clone(), bus_topic.to_string())),
					callbacks_active: Default::default(),
					listeners: Default::default(),
					counter,
				})
			}
			AsbNetMode::Loopback(asb) => {
				let ringmaster = Arc::new(ReaderRingMaster::new(*wire_format, qos));
				let (prod, cons) = crossbeam_ring_channel::ring_bounded(qos.buffer.max(1));
				let my_sender_id = ringmaster.add_sender(prod);
				asb.add_reader(bus_topic).unwrap();

				Ok(AsbReader {
					buffer: cons,
					expiration: qos.expiration,
					ringmaster,
					my_sender_id,
					net: Arc::new(AsbReaderNet::Loopback(asb.clone())),
					callbacks_active: Default::default(),
					listeners: Default::default(),
					counter,
				})
			}
			AsbNetMode::Null => {
				let ringmaster = Arc::new(ReaderRingMaster::new(*wire_format, qos));

				// Construct empty ring buffer since null does nothing.
				let (_, cons) = crossbeam_ring_channel::ring_bounded(0);

				Ok(AsbReader {
					buffer: cons,
					expiration: qos.expiration,
					ringmaster,
					my_sender_id: 0,
					net: Arc::new(AsbReaderNet::Null),
					callbacks_active: Arc::new(AtomicBool::default()),
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
		let bus_topic = service_cfg
			.and_then(|cfg| {
				// Try to get the bus topic
				cfg.topics
					.get(topic)
					.and_then(|tcfg| tcfg.bus_topic.as_ref())
					.map(|s| s.as_str())
			})
			.unwrap_or(topic);

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
					.copied()
					.ok_or(CalError::config_err(format_args!(
						"Could not find QoS settings for {name}"
					)))
			})?;

		match &self.net {
			AsbNetMode::Amqp(asb, _) => {
				let exchange_name = asb
					.exchange
					.as_ref()
					.map(|s| s.as_ref())
					.unwrap_or_default();

				// Create the publish parameters
				let props = BasicProperties::default();
				let args = BasicPublishArguments::new(exchange_name, bus_topic);

				Ok(AsbWriter {
					net: AsbWriterNet::Amqp(asb.clone(), props, args),
					format: *wire_format,
					_counter: counter,
					_asb: PhantomData,
				})
			}
			AsbNetMode::Mqtt(asb, _) => {
				let mqtt_qos = match qos.reliability {
					ReliabilityQos::BestEffort => rumqttc::QoS::AtMostOnce,
					ReliabilityQos::Reliable => rumqttc::QoS::AtLeastOnce,
				};

				Ok(AsbWriter {
					net: AsbWriterNet::Mqtt(asb.clone(), mqtt_qos, bus_topic.to_string()),
					format: *wire_format,
					_counter: counter,
					_asb: PhantomData,
				})
			}
			AsbNetMode::Loopback(asb) => Ok(AsbWriter {
				net: AsbWriterNet::Loopback(asb.clone(), bus_topic.to_string()),
				format: WireFormat::Xml,
				_counter: counter,
				_asb: PhantomData,
			}),
			AsbNetMode::Null => Ok(AsbWriter {
				net: AsbWriterNet::Null,
				// No default for [WireFormat] so just picking Xml since it's the first.
				format: WireFormat::Xml,
				_counter: counter,
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

/// Listener id with callback type.
type Listener<T> = (u32, Box<dyn AsbListener<T>>);
/// Reader id with send portion of ring buffer.
type ReaderSender<T> = (u32, RingSender<(Instant, Arc<T>)>);

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
	// TODO: Refactor this with [`Arc<ReaderRingMaster<T>>`].
	ringmaster: Arc<ReaderRingMaster<T>>,
	my_sender_id: u32,
	/// Arc so that any unsubscribes happen only after last reader drops.
	net: Arc<AsbReaderNet>,
	/// Whether this reader has registered listeners and should disallow `read()`.
	callbacks_active: Arc<AtomicBool>,
	/// All registered listeners keyed by a random number.
	listeners: Arc<Mutex<Vec<Listener<T>>>>,
	/// Not used, simply holds so [AsbConnection] can track topic usage.
	counter: PossessCtr,
}
impl<T> AsbReader<T> {
	fn callback_mode_error(&self) -> Result<(), CalError> {
		match self.in_callback_mode() {
			true => Err(CalError::ill_err("Reader has active listeners")),
			false => Ok(()),
		}
	}

	fn in_callback_mode(&self) -> bool {
		self.callbacks_active.load(Ordering::Acquire)
	}

	fn set_callback_mode(&self, active: bool) {
		self.callbacks_active.store(active, Ordering::Release);
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
	pub fn add_listener(&self, fun: impl AsbListener<T>) -> u32 {
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
		if !self.in_callback_mode() {
			self.set_callback_mode(true);
			let cb_mode = self.callbacks_active.clone();
			std::thread::spawn(move || {
				// TODO: Do some basic profiling to find a good value for this.
				//       Shorter makes loop more "busy" but gives better reaction in cases
				//       of infrequent message reception, longer does the opposite.
				const RECV_TMOUT: Duration = Duration::from_millis(100);

				// Have conditional outside of loop since this can be a very hot loop.
				if let Some(expiration) = expire {
					loop {
						// When reader disables callback mode, stop thread.
						if !cb_mode.load(Ordering::Relaxed) {
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
						// When reader disables callback mode, stop thread.
						if !cb_mode.load(Ordering::Relaxed) {
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
	pub fn remove_listener(&self, id: u32) -> bool {
		let mut listeners = self.listeners.lock().unwrap();
		if let Some(idx) = listeners.iter().position(|b| b.0 == id) {
			_ = listeners.swap_remove(idx);

			// If no more listeners, empty callback handle so bg thread will stop and
			// reads can continue as normal.
			if listeners.is_empty() {
				self.set_callback_mode(false);
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
		let my_sender_id = self.ringmaster.add_sender(prod);

		AsbReader {
			buffer: cons,
			expiration: self.expiration,
			ringmaster: self.ringmaster.clone(),
			my_sender_id,
			net: self.net.clone(),
			callbacks_active: Arc::new(AtomicBool::default()),
			listeners: Arc::new(Mutex::new(Vec::new())),
			counter: self.counter.clone(),
		}
	}
}
impl<T> Drop for AsbReader<T> {
	fn drop(&mut self) {
		self.ringmaster.remove_sender(self.my_sender_id);
	}
}

/// Holds all network-specific data to manage the reader/subscriber.
enum AsbReaderNet {
	Amqp(Arc<amqp::AmqpAsb>, String),
	// .2 is topic
	Mqtt(Arc<mqtt::MqttAsb>, String),
	Loopback(Arc<loopback::LoopbackAsb>),
	Null,
}
impl Drop for AsbReaderNet {
	fn drop(&mut self) {
		match self {
			AsbReaderNet::Amqp(asb, topic) => _ = asb.del_reader(topic),
			AsbReaderNet::Mqtt(asb, topic) => _ = asb.del_reader(&topic),
			_ => {}
		}
	}
}

/// Publishes messages to the ASB on the topic specified during construction.
#[derive(Clone)]
pub struct AsbWriter<T> {
	net: AsbWriterNet,
	format: WireFormat,
	/// Intentionally unused, this existing is what matters.
	_counter: PossessCtr,
	_asb: PhantomData<T>,
}
#[derive(Clone)]
enum AsbWriterNet {
	Amqp(Arc<amqp::AmqpAsb>, BasicProperties, BasicPublishArguments),
	// .2 is topic.
	Mqtt(Arc<mqtt::MqttAsb>, rumqttc::QoS, String),
	Loopback(Arc<loopback::LoopbackAsb>, String),
	Null,
}
impl<T: Serialize> AsbWriter<T> {
	/// Publishes `msg` to the topic specified in [create_writer()](AsbConnection::create_writer).
	pub fn write(&self, msg: &T) -> Result<(), CalError> {
		let data = crate::msg_serde::serialize_msg(&self.format, msg)?;

		match &self.net {
			AsbWriterNet::Amqp(asb, props, args) => Ok(asb
				.rt_handle
				.block_on(asb.chan.basic_publish(props.clone(), data, args.clone()))?),
			AsbWriterNet::Mqtt(asb, qos, topic) => asb.publish(topic, *qos, false, data),
			AsbWriterNet::Loopback(asb, topic) => asb.publish(&topic, &data),
			_ => Ok(()),
		}
	}
}
