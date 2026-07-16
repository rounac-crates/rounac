//! Module for the network related types.

pub mod amqp;

use crate::{
	Topic,
	config::{AsbConfig, NetworkKind},
	error::CalError,
};
use amqp::open_args_for_net;
use amqprs::{
	callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
	channel::{BasicConsumeArguments, QueueDeclareArguments},
	connection::Connection,
};
use tokio::runtime::{Handle, Runtime};

pub enum AsbConnection {
	Amqp(Handle, amqp::AmqpAsb),
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
				let joiner = std::thread::spawn(move || rt.block_on(async { loop {} }));

				Ok(AsbConnection::Amqp(handle, a))
			}
			NetworkKind::Null => Ok(AsbConnection::Null),
		}
	}

	pub fn create_reader<T>(
		&self,
		topic: &Topic<T>,
		config: &AsbConfig,
	) -> Result<AsbReader, CalError> {
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

				rt.block_on(async {
					// Declare queue
					a.chan.queue_declare(declare_args).await?;

					// TODO: Bind queue to exchange if necessary

					// Create consumer for topic (subscribe).
					//let tag = a.chan.basic_consume(consumer, consume_args).await?;

					Ok::<_, amqprs::error::Error>(())
				})?;

				Err(CalError::other_err("Not implemented".to_string()))
			}
			AsbConnection::Null => Ok(AsbReader::Null),
		}
	}
}

pub enum AsbReader {
	Amqp,
	Null,
}

pub enum AsbWriter {}
