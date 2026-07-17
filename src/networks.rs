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
use ringbuf::traits::{Consumer, Split};
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, sync::Arc};
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
					_ = asb.chan.clone().close();
					_ = asb.conn.clone().close();
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

					// TODO: If config has exchange name, create direct exchange.

					let a = amqp::AmqpAsb { conn, chan };

					Ok::<_, amqprs::error::Error>(a)
				})?;

				// Spawn background thread to drive the tokio runtime.
				let channel_clone = a.chan.clone();
				std::thread::spawn(move || {
					rt.block_on(async {
						// Yield while channel is still active.
						while channel_clone.is_open() {
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
	) -> Result<AsbReader<T>, CalError> {
		match self {
			AsbConnection::Amqp(rt, a) => {
				// Create a queue for this topic
				// TODO: Check config for topic prefix and adjust `topic_name` accordingly.
				let topic_name = topic.name.clone();

				// Prepare arguments
				// TODO: Check config for whether topic should be exclusive
				let declare_args = QueueDeclareArguments::transient_autodelete(&topic_name);

				// TODO: Create QueueBindArguments if exchange is used. Bind to default exchange is automatic from declaration.

				// Create consumer object for reader with mpsc channel or shared ring buffer.
				// amqprs::consumer::AsyncConsumer
				// TODO: Set auto_ack/no_ack depending on QoS (true for best effort, false for reliable).
				let consume_args = BasicConsumeArguments::new(&topic_name, "");

				// Create a ring buffer and split into producer and consumer.
				let (prod, cons) = ringbuf::HeapRb::<T>::new(topic.qos.buffer).split();
				let consumer = AmqpConsumer {
					// TODO: Fetch from config.
					format: WireFormat::Xml,
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

				Ok(AsbReader::Amqp(rt.clone(), tag, a.clone(), cons))
			}
			AsbConnection::Null => Ok(AsbReader::Null),
		}
	}

	pub fn create_writer<T>(
		&self,
		topic: &Topic<T>,
		config: &AsbConfig,
	) -> Result<AsbWriter<T>, CalError> {
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
					WireFormat::Xml,
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
pub enum AsbReader<T> {
	Amqp(Handle, String, Arc<amqp::AmqpAsb>, ringbuf::HeapCons<T>),
	Null,
}
impl<T> Drop for AsbReader<T> {
	fn drop(&mut self) {
		match self {
			AsbReader::Amqp(rt, tag, a, _) => {
				let cancel = BasicCancelArguments::new(tag);
				_ = rt.block_on(a.chan.basic_cancel(cancel));
			}
			AsbReader::Null => {}
		}
	}
}
impl<T> AsbReader<T> {
	/// Read the next message from the buffer.
	pub fn read(&mut self) -> Option<T> {
		match self {
			AsbReader::Amqp(_, _, _, buf) => buf.try_pop(),
			AsbReader::Null => None,
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
