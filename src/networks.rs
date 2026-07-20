//! Module for the network related types.

pub mod amqp;

use crate::{
	Topic,
	config::{AsbConfig, NetworkKind, WireFormat},
	error::CalError,
};
use amqp::{AmqpConsumer, open_args_for_net};
use amqprs::{
	BasicProperties,
	callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
	channel::{
		BasicCancelArguments, BasicConsumeArguments, BasicPublishArguments, QueueDeclareArguments,
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
use tokio::runtime::Handle;

/// Manages the transport-specific data and lifetime.
// TODO: Refactor to struct to store Option<Handle> and a status var shared with background thread.
pub enum AsbConnection {
	Amqp(Handle, Arc<amqp::AmqpAsb>),
	Null,
}
impl Drop for AsbConnection {
	fn drop(&mut self) {
		match self {
			AsbConnection::Amqp(rt, asb) => {
				rt.block_on(async {
					// Close channel and connection, then join background thread.
					_ = asb.chan.clone().close().await;
					_ = asb.conn.clone().close().await;
				});
			}
			_ => {}
		};
	}
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
				let handle = rt.handle().clone();

				// Open the connection and create a single channel for everything.
				let open_args = open_args_for_net(&network)?;
				let a = rt.block_on(async {
					let conn = Connection::open(&open_args).await?;
					conn.register_callback(DefaultConnectionCallback).await?;
					let chan = conn.open_channel(None).await?;
					chan.register_callback(DefaultChannelCallback).await?;
					chan.flow(true).await?; // Kickstart traffic flowing

					// TODO: If config has exchange name, create direct exchange.

					let a = amqp::AmqpAsb { conn, chan };

					Ok::<_, amqprs::error::Error>(a)
				})?;

				// Spawn background thread to drive the tokio runtime.
				let conn_clone = a.conn.clone();
				std::thread::spawn(move || {
					rt.block_on(async {
						// Yield while connection is still active.
						// Connection gets dropped last so tokio runtime must live beyond that.
						while conn_clone.is_open() {
							// Yield a few times before re-checking channel to avoid saturation.
							// TODO: Tune number to see what effect it has.
							// NOTE: Perhaps a time-based condition would be better.
							for _ in 0..20 {
								tokio::task::yield_now().await;
							}
						}
					})
				});

				Ok(AsbConnection::Amqp(handle, Arc::new(a)))
			}
			NetworkKind::Null => Ok(AsbConnection::Null),
		}
	}

	pub fn create_reader<T: for<'de> Deserialize<'de> + Send + 'static>(
		&self,
		topic: &Topic<T>,
		config: &AsbConfig,
		svc_name: &str,
	) -> Result<AsbReader<T>, CalError> {
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

		match self {
			AsbConnection::Amqp(rt, a) => {
				// Create a queue for this topic
				// TODO: Check config for topic prefix and adjust `topic_name` accordingly.
				let topic_name = topic.name.clone();

				// Prepare arguments
				// If `auto_delete`, also require `exclusive` since RabbitMQ hard errors otherwise.
				let declare_args = QueueDeclareArguments::new(&topic_name)
					.exclusive(true)
					.auto_delete(true)
					.finish();

				// TODO: Create QueueBindArguments if exchange is used. Bind to default exchange is automatic from declaration.

				// Create consumer object for reader with mpsc channel or shared ring buffer.
				// amqprs::consumer::AsyncConsumer
				// TODO: Set auto_ack/no_ack depending on QoS (true for best effort, false for reliable).
				let consume_args = BasicConsumeArguments::new(&topic_name, "");

				// Create a ring buffer and split into producer and consumer.
				let (prod, cons) = crossbeam_ring_channel::ring_bounded(topic.qos.buffer);
				let consumer = AmqpConsumer {
					format: *wire_format,
					buffer: prod,
				};

				let tag = rt.block_on(async {
					// Declare queue
					a.chan.queue_declare(declare_args).await?;

					// TODO: Bind queue to exchange if necessary

					// Create consumer for topic (subscribe).
					let tag = a.chan.basic_consume(consumer, consume_args).await?;

					Ok::<_, amqprs::error::Error>(tag)
				})?;

				Ok(AsbReader {
					buffer: cons,
					net: AsbReaderNet::Amqp(rt.clone(), tag, a.clone()),
					callback_mode: false,
					listeners: Mutex::new(HashMap::new()),
				})
			}
			AsbConnection::Null => {
				// Construct empty ring buffer since null does nothing.
				let (_, cons) = crossbeam_ring_channel::ring_bounded(0);

				Ok(AsbReader {
					buffer: cons,
					net: AsbReaderNet::Null,
					callback_mode: false,
					listeners: Mutex::new(HashMap::new()),
				})
			}
		}
	}

	pub fn create_writer<T>(
		&self,
		topic: &Topic<T>,
		config: &AsbConfig,
		svc_name: &str,
	) -> Result<AsbWriter<T>, CalError> {
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

		match self {
			AsbConnection::Amqp(rt, asb) => {
				// TODO: Check config for topic prefix and adjust `topic_name` accordingly.
				let topic_name = topic.name.clone();

				// Create the publish parameters
				let props = BasicProperties::default();
				let args = BasicPublishArguments::new("", &topic_name);

				Ok(AsbWriter::Amqp(
					rt.clone(),
					asb.clone(),
					*wire_format,
					props,
					args,
					PhantomData,
				))
			}
			AsbConnection::Null => Ok(AsbWriter::Null),
		}
	}
}

/// Provides messages received from the ASB through a polling interface.
///
/// **IMPORTANT**: If the network type is "null" then all read methods will error.
pub struct AsbReader<T> {
	buffer: RingReceiver<T>,
	net: AsbReaderNet,
	/// Whether this reader has registered listeners and should disallow `read()`.
	// Option<JoinHandle<RingReceiver<T>>> - Pass RingReceiver back and forth, or just clone it.
	callback_mode: bool,
	/// All registered listeners keyed by a random number.
	listeners: Mutex<HashMap<u32, Box<dyn Fn(&T) + Send + Sync>>>,
}
impl<T> AsbReader<T> {
	/// Read the next message from the buffer or block until there is one.
	pub fn read(&self) -> Result<T, CalError> {
		self.buffer
			.recv()
			.map_err(|_| CalError::other_err("Reader error".to_string()))
	}

	/// Read the next message from the buffer or block until one is received or `timeout` is reached.
	pub fn read_timeout(&self, timeout: Duration) -> Result<Option<T>, CalError> {
		match self.buffer.recv_timeout(timeout) {
			Ok(m) => Ok(Some(m)),
			Err(e) => match e {
				RecvTimeoutError::Timeout => Ok(None),
				_ => Err(CalError::other_err("Reader error".to_string())),
			},
		}
	}

	/// Read the next message from the buffer if there is one. Does not block.
	pub fn try_read(&self) -> Result<Option<T>, CalError> {
		match self.buffer.try_recv() {
			Ok(m) => Ok(Some(m)),
			Err(e) => match e {
				TryRecvError::Empty => Ok(None),
				_ => Err(CalError::other_err("Reader error".to_string())),
			},
		}
	}
}

/// Holds all network-specific data to manage the reader/subscriber.
pub enum AsbReaderNet {
	Amqp(Handle, String, Arc<amqp::AmqpAsb>),
	Null,
}
impl Drop for AsbReaderNet {
	fn drop(&mut self) {
		match self {
			AsbReaderNet::Amqp(rt, tag, a) => {
				let cancel = BasicCancelArguments::new(tag);
				_ = rt.block_on(a.chan.basic_cancel(cancel));
			}
			AsbReaderNet::Null => {}
		}
	}
}

/// Publishes messages to the ASB on the topic specified during construction.
// TODO: Refactor to shrink tuple size and/or convert to struct with common
//       elements like `Handle` and `WireFormat`.
pub enum AsbWriter<T> {
	Amqp(
		Handle,
		Arc<amqp::AmqpAsb>,
		WireFormat,
		BasicProperties,
		BasicPublishArguments,
		PhantomData<T>,
	),
	Null,
}
impl<T: Serialize> AsbWriter<T> {
	pub fn write(&self, msg: &T) -> Result<(), CalError> {
		match self {
			AsbWriter::Amqp(rt, asb, format, props, args, _) => {
				let data = crate::msg_serde::serialize_msg(format, msg)?;

				Ok(rt.block_on(asb.chan.basic_publish(props.clone(), data, args.clone()))?)
			}
			AsbWriter::Null => Ok(()),
		}
	}
}
