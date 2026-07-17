//! Module for the network related types.

pub mod amqp;

use crate::{
	Topic,
	config::{
		AsbConfig,
		NetworkKind::{self, Amqp},
		WireFormat,
	},
	error::CalError,
};
use amqp::{AmqpConsumer, open_args_for_net};
use amqprs::{
	callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
	channel::{BasicCancelArguments, BasicConsumeArguments, Channel, QueueDeclareArguments},
	connection::Connection,
};
use ringbuf::{SharedRb, traits::Split};
use serde::Deserialize;
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};

pub enum AsbConnection {
	Amqp(Handle, Arc<amqp::AmqpAsb>),
	Null,
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
				// TODO: Refactor `AsbConnection` to struct so store things like this easier.
				let joiner = std::thread::spawn(move || {
					rt.block_on(async {
						// Infinitely yield
						loop {
							tokio::task::yield_now().await
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

				let a = AsbReader::Amqp(rt.clone(), tag, a.clone(), cons);

				Err(CalError::other_err("Not implemented".to_string()))
			}
			AsbConnection::Null => Ok(AsbReader::Null),
		}
	}
}

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

pub enum AsbWriter {}
