//! AMQPRS related utilities

use super::ReaderSender;
use crate::{
	config::{NetworkConfig, NetworkKind, QosSettings, ReliabilityQos, WireFormat},
	error::CalError,
	networks::utils::{AsbConnStatus, StatusCallbackManager},
};
use amqprs::{
	Ack, BasicProperties, Cancel, Close, CloseChannel, Deliver, Nack, Return,
	callbacks::{ChannelCallback, ConnectionCallback},
	channel::{BasicAckArguments, Channel},
	connection::{Connection, OpenConnectionArguments},
	consumer::AsyncConsumer,
	error::Error,
};
use async_trait::async_trait;
use serde::Deserialize;
use std::{
	sync::{Arc, Mutex},
	time::Instant,
};
use tokio::runtime::Handle;
use toml::Value;

/// Get the necessary config params to create AMQP connection for `net_name`.
pub fn open_args_for_net(network: &NetworkConfig) -> Result<OpenConnectionArguments, CalError> {
	// Verify this network is the correct type.
	if network.kind != NetworkKind::Amqp {
		return Err(CalError::config_err("Expected network kind \"amqp\"."));
	}

	// Get parameters
	let host = match network.params.get("host") {
		Some(Value::String(s)) => Ok(s),
		_ => Err(CalError::config_err("Expected string parameter \"host\".")),
	}?;
	let port = match network.params.get("port") {
		Some(Value::Integer(i)) => Ok(i),
		_ => Err(CalError::config_err("Expected integer parameter \"port\".")),
	}?;
	let user = match network.params.get("username") {
		Some(Value::String(s)) => Ok(s),
		_ => Err(CalError::config_err(
			"Expected string parameter \"username\".",
		)),
	}?;
	let pass = match network.params.get("password") {
		Some(Value::String(s)) => Ok(s),
		_ => Err(CalError::config_err(
			"Expected string parameter \"password\".",
		)),
	}?;

	Ok(OpenConnectionArguments::new(host, *port as u16, user, pass))
}

pub(crate) struct AmqpAsb {
	pub rt_handle: Handle,
	pub conn: Connection,
	pub chan: Channel,
	pub exchange: Option<String>,
}

pub struct AmqpConsumer<T> {
	pub format: WireFormat,
	/// Shared with each reader, but readers only modify during clone and drop.
	pub buffers: Arc<Mutex<Vec<ReaderSender<T>>>>,
	pub qos: QosSettings,
	pub last_received: Option<Instant>,
}

#[async_trait]
impl<T: for<'de> Deserialize<'de> + Send + Sync> AsyncConsumer for AmqpConsumer<T> {
	async fn consume(
		&mut self,
		chan: &Channel,
		deliver: Deliver,
		_: BasicProperties,
		data: Vec<u8>,
	) {
		// ACK first to broker knows we got the message.
		if self.qos.reliability == ReliabilityQos::Reliable {
			let ack_args = BasicAckArguments::new(deliver.delivery_tag(), false);

			// Try to ACK some number of times before giving up.
			const MAX_ACK_TRIES: usize = 2;
			for _ in 0..MAX_ACK_TRIES {
				if chan.basic_ack(ack_args.clone()).await.is_ok() {
					break;
				}
			}
		}

		// If `self.qos.time_based_filter` if set, check last receive time to avoid
		// deserializing if we aren't due for another message.
		if let Some(dur) = self.qos.time_based_filter {
			// Checking time since last receive else setting last receive to now.
			if let Some(last) = self.last_received
				&& last.elapsed() < dur
			{
				return;
			}

			self.last_received = Some(Instant::now());
		}

		// Deserialize message before sending to all readers.
		if let Ok(msg) = crate::msg_serde::deserialize_msg(&self.format, &data) {
			// Send to all ring buffers
			let arced: Arc<T> = Arc::new(msg);
			let now = Instant::now();
			for buffer in self.buffers.lock().unwrap().iter() {
				_ = buffer.1.send((now, arced.clone()));
			}
		}
	}
}

// TODO: If reconnection desired, `AmqpAsb` needs to be [RwLock]'d and [Arc]'d
//       plus track consumers (would require `dyn` over [AmqpConsumer]).
pub(crate) struct ConnCb {
	pub(crate) status_manager: Arc<StatusCallbackManager>,
}
#[async_trait]
impl ConnectionCallback for ConnCb {
	async fn close(&mut self, connection: &Connection, close: Close) -> Result<(), Error> {
		// TODO: Have a way to relay error condition to [AsbConnection].
		eprintln!(
			"ERROR: Connection({}) closed by server: {close}",
			connection.connection_name()
		);

		// If connection is closed, then ASB connection is in a failure state.
		// TODO: If reconnect logic is ever added for AMQP, change this to
		// `AsbConnStatus::Inoperable`.
		self.status_manager.set_status(AsbConnStatus::Failed);

		Ok(())
	}

	async fn blocked(&mut self, _: &Connection, _: String) {}

	async fn unblocked(&mut self, _: &Connection) {}

	async fn secret_updated(&mut self, _: &Connection) {}
}

// If desired to re-opening channels, see TODO on [ConnCb] for general
// refactors required.
pub(crate) struct ChanCb {
	pub(crate) status_manager: Arc<StatusCallbackManager>,
}
#[async_trait]
impl ChannelCallback for ChanCb {
	async fn close(&mut self, chan: &Channel, close_channel: CloseChannel) -> Result<(), Error> {
		// TODO: Have a way to relay error condition to [AsbConnection].
		eprintln!(
			"ERROR: Channel({}) closed by server: {close_channel}",
			chan.channel_id()
		);

		// If channel is closed, then ASB connection is active but reads/writes will
		// not work.
		self.status_manager.set_status(AsbConnStatus::Inoperable);

		Ok(())
	}

	async fn cancel(&mut self, chan: &Channel, cancel: Cancel) -> Result<(), Error> {
		// TODO: Have a way to relay error condition to [AsbReader] or [AsbConnection].
		eprintln!(
			"ERROR: Channel({}) consumer cancelled by server: {cancel:?}",
			chan.channel_id()
		);
		Ok(())
	}

	async fn flow(&mut self, _: &Channel, _: bool) -> Result<bool, Error> {
		Ok(true)
	}

	async fn publish_ack(&mut self, _: &Channel, _: Ack) {}

	async fn publish_nack(&mut self, _: &Channel, _: Nack) {
		// TODO: If topic QoS dictates reliable, figure out how to get writer to
		//       re-send if `nack.requeue` is false.
	}

	async fn publish_return(&mut self, _: &Channel, _: Return, _: BasicProperties, _: Vec<u8>) {}
}
