//! Module for the network related types.

pub mod amqp;

use crate::{
	Topic,
	config::{AsbConfig, NetworkKind, WireFormat},
	error::CalError,
	networks::amqp::{ChanCb, ConnCb},
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
use crossbeam_ring_channel::RingReceiver;
use serde::{Deserialize, Serialize};
use std::{
	collections::HashMap,
	marker::PhantomData,
	sync::{Arc, Mutex},
	time::Duration,
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
// TODO: Also figure out how to track reader/writer topics. Is `Arc<Mutex<...>>` good enough?
pub struct AsbConnection {
	/// The transport-specific things.
	net: AsbNetMode,
}
impl AsbConnection {
	pub fn connect(net_name: &str, config: &AsbConfig) -> Result<Self, CalError> {
		let Some(network) = config.networks.get(net_name) else {
			return Err(CalError::config_err(format!(
				"Missing network config for {net_name}"
			)));
		};

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
						return Err(CalError::config_err(format!(
							"AMQP parameter \"exchange\" must be a non-empty string."
						)));
					}
					None => None,
				};
				let durable = match network.params.get("durable_exchange") {
					Some(toml::Value::Boolean(ex)) => *ex,
					Some(_) => {
						return Err(CalError::config_err(format!(
							"AMQP parameter \"durable_exchange\" must be a boolean."
						)));
					}
					None => true,
				};

				// Open the connection and create a single channel for everything.
				let open_args = open_args_for_net(&network)?;
				let (conn, chan) = rt.block_on(async {
					let conn = Connection::open(&open_args).await?;
					conn.register_callback(ConnCb).await?;
					let chan = conn.open_channel(None).await?;
					chan.register_callback(ChanCb).await?;
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
				})
			}
			NetworkKind::Null => Ok(AsbConnection {
				net: AsbNetMode::Null,
			}),
		}
	}

	pub fn create_reader<'a, T: for<'de> Deserialize<'de> + Send + Sync + 'static>(
		&'a self,
		topic: &Topic<T>,
		config: &AsbConfig,
		svc_name: &str,
	) -> Result<AsbReader<'a, T>, CalError> {
		// Check for the wire format first
		let default_wire_format = config.services.default_wire_format.as_ref();
		let wire_format = match config.services.service.get(svc_name) {
			// If the service config has a wire format, use that.
			Some(cfg) if cfg.wire_format.is_some() => Ok(cfg.wire_format.as_ref().unwrap()),
			// Otherwise try to use the default.
			_ => default_wire_format.ok_or(CalError::config_err(format!(
				"No wire format specified for topic {} under service {svc_name}.",
				&topic.name
			))),
		}?;

		match &self.net {
			AsbNetMode::Amqp(asb, _) => {
				// Create a queue for this topic
				// TODO: Check config for topic prefix and adjust `topic_name` accordingly.
				let topic_name = topic.name.clone();

				// If no exchange specified use topic name, otherwise let the broker name
				// it.
				let queue_name = match asb.exchange.is_some() {
					true => "",
					false => topic_name.as_str(),
				};

				// Prepare declare queue args.
				// If `auto_delete` desired, then `exclusive` must be true to avoid error
				// with RabbitMQ due to deprecated combination.
				let declare_args = QueueDeclareArguments::new(queue_name)
					.exclusive(true)
					.auto_delete(true)
					.finish();

				// Create the ring buffer for the reader and consumer.
				let (prod, cons) = crossbeam_ring_channel::ring_bounded(topic.qos.buffer);
				let consumer = AmqpConsumer {
					format: *wire_format,
					buffer: prod,
				};

				// Do all the actual network stuff here and save tag for deleting consumer.
				let tag = asb.rt_handle.block_on(async {
					// Declare queue
					// Safety: We do not set `no_wait` above.
					let res = asb.chan.queue_declare(declare_args).await?.unwrap();

					// Prepare the consumer arguments for the new queue. Use returned result
					// to guarantee queue name is correct.
					// TODO: Set auto_ack/no_ack depending on QoS (true for best effort, false for reliable).
					let consume_args = BasicConsumeArguments::new(&res.0, "");

					// If an exchange is specified, bind queue to it.
					if let Some(ref ex) = asb.exchange {
						let args = QueueBindArguments::new(&res.0, &ex, &topic_name);
						asb.chan.queue_bind(args).await?;
					}

					// Create consumer for topic (subscribe).
					let tag = asb.chan.basic_consume(consumer, consume_args).await?;

					Ok::<_, amqprs::error::Error>(tag)
				})?;

				Ok(AsbReader {
					buffer: cons,
					net: AsbReaderNet::Amqp(asb.clone(), tag),
					callback_mode: false,
					listeners: Mutex::new(HashMap::new()),
					_asb: PhantomData,
				})
			}
			AsbNetMode::Null => {
				// Construct empty ring buffer since null does nothing.
				let (_, cons) = crossbeam_ring_channel::ring_bounded(0);

				Ok(AsbReader {
					buffer: cons,
					net: AsbReaderNet::Null,
					callback_mode: false,
					listeners: Mutex::new(HashMap::new()),
					_asb: PhantomData,
				})
			}
		}
	}

	pub fn create_writer<'a, T>(
		&'a self,
		topic: &Topic<T>,
		config: &AsbConfig,
		svc_name: &str,
	) -> Result<AsbWriter<'a, T>, CalError> {
		// Check for the wire format first
		let default_wire_format = config.services.default_wire_format.as_ref();
		let wire_format = match config.services.service.get(svc_name) {
			// If the service config has a wire format, use that.
			Some(cfg) if cfg.wire_format.is_some() => Ok(cfg.wire_format.as_ref().unwrap()),
			// Otherwise try to use the default.
			_ => default_wire_format.ok_or(CalError::config_err(format!(
				"No wire format specified for topic {} under service {svc_name}.",
				&topic.name
			))),
		}?;

		match &self.net {
			AsbNetMode::Amqp(asb, _) => {
				// TODO: Check config for topic prefix and adjust `topic_name` accordingly.
				let topic_name = topic.name.clone();

				let exchange_name = asb
					.exchange
					.as_ref()
					.map(|s| s.as_ref())
					.unwrap_or_default();

				// Create the publish parameters
				let props = BasicProperties::default();
				let args = BasicPublishArguments::new(exchange_name, &topic_name);

				Ok(AsbWriter {
					net: AsbWriterNet::Amqp(asb.clone(), props, args),
					format: *wire_format,
					_asb: PhantomData,
				})
			}
			AsbNetMode::Null => Ok(AsbWriter {
				net: AsbWriterNet::Null,
				// No default for [WireFormat] so just picking Xml since it's the first.
				format: WireFormat::Xml,
				_asb: PhantomData,
			}),
		}
	}
}

/// Provides messages received from the ASB through a polling interface.
///
/// **IMPORTANT**: If the network type is "null" then all read methods will error.
pub struct AsbReader<'a, T> {
	buffer: RingReceiver<Arc<T>>,
	net: AsbReaderNet,
	/// Whether this reader has registered listeners and should disallow `read()`.
	// Option<JoinHandle<RingReceiver<T>>> - Pass RingReceiver back and forth, or just clone it.
	callback_mode: bool,
	/// All registered listeners keyed by a random number.
	listeners: Mutex<HashMap<u32, Box<dyn Fn(&T) + Send + Sync>>>,
	// Just used to tie lifetime of this object to the ASB.
	_asb: PhantomData<&'a T>,
}
impl<'a, T> AsbReader<'a, T> {
	/// Read the next message from the buffer or block until there is one.
	pub fn read(&self) -> Result<Arc<T>, CalError> {
		// Do actual read.
		self.buffer
			.recv()
			.map_err(|_| CalError::other_err("Reader closed unexpectedly".to_string()))
	}

	/// Read the next message from the buffer or block until one is received or `timeout` is reached.
	pub fn read_timeout(&self, timeout: Duration) -> Result<Option<Arc<T>>, CalError> {
		// Do actual read.
		match self.buffer.recv_timeout(timeout) {
			Ok(m) => Ok(Some(m)),
			Err(e) => match e {
				RecvTimeoutError::Timeout => Ok(None),
				_ => Err(CalError::other_err(
					"Reader closed unexpectedly".to_string(),
				)),
			},
		}
	}

	/// Read the next message from the buffer if there is one. Does not block.
	pub fn try_read(&self) -> Result<Option<Arc<T>>, CalError> {
		// Do actual read.
		match self.buffer.try_recv() {
			Ok(m) => Ok(Some(m)),
			Err(e) => match e {
				TryRecvError::Empty => Ok(None),
				_ => Err(CalError::other_err(
					"Reader closed unexpectedly".to_string(),
				)),
			},
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
pub struct AsbWriter<'a, T> {
	net: AsbWriterNet,
	format: WireFormat,
	_asb: PhantomData<&'a T>,
}
pub enum AsbWriterNet {
	Amqp(Arc<amqp::AmqpAsb>, BasicProperties, BasicPublishArguments),
	Null,
}
impl<'a, T: Serialize> AsbWriter<'a, T> {
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
