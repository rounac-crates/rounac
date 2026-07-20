//! AMQPRS related utilities

use crate::{
	config::{NetworkConfig, NetworkKind, WireFormat},
	error::CalError,
};
use amqprs::{
	Ack, BasicProperties, Cancel, Close, CloseChannel, Deliver, Nack, Return,
	callbacks::{ChannelCallback, ConnectionCallback},
	channel::Channel,
	connection::{Connection, OpenConnectionArguments},
	consumer::AsyncConsumer,
	error::Error,
};
use async_trait::async_trait;
use crossbeam_ring_channel::RingSender;
use serde::Deserialize;
use toml::Value;

/// Get the necessary config params to create AMQP connection for `net_name`.
pub fn open_args_for_net(network: &NetworkConfig) -> Result<OpenConnectionArguments, CalError> {
	// Verify this network is the correct type.
	if network.kind != NetworkKind::Amqp {
		return Err(CalError::config_err(format!(
			"Expected network kind \"amqp\"."
		)));
	}

	// Get parameters
	let host = match network.params.get("host") {
		Some(Value::String(s)) => Ok(s),
		_ => Err(CalError::config_err(format!(
			"Expected string parameter \"host\"."
		))),
	}?;
	let port = match network.params.get("port") {
		Some(Value::Integer(i)) => Ok(i),
		_ => Err(CalError::config_err(format!(
			"Expected integer parameter \"port\"."
		))),
	}?;
	let user = match network.params.get("username") {
		Some(Value::String(s)) => Ok(s),
		_ => Err(CalError::config_err(format!(
			"Expected string parameter \"username\"."
		))),
	}?;
	let pass = match network.params.get("password") {
		Some(Value::String(s)) => Ok(s),
		_ => Err(CalError::config_err(format!(
			"Expected string parameter \"password\"."
		))),
	}?;

	Ok(OpenConnectionArguments::new(host, *port as u16, user, pass))
}

pub struct AmqpAsb {
	pub conn: Connection,
	pub chan: Channel,
}

pub struct AmqpConsumer<T> {
	pub format: WireFormat,
	pub buffer: RingSender<T>,
}

#[async_trait]
impl<T: for<'de> Deserialize<'de> + Send> AsyncConsumer for AmqpConsumer<T> {
	async fn consume(&mut self, _: &Channel, _: Deliver, _: BasicProperties, data: Vec<u8>) {
		// Deserialize message
		if let Ok(msg) = crate::msg_serde::deserialize_msg(&self.format, &data) {
			// Add to ring buffer
			_ = self.buffer.send(msg);
		}
	}
}

/// Type to debug connection issues with AMQP.
#[cfg(debug_assertions)]
pub(crate) struct DebugConnectionCallback;
#[cfg(debug_assertions)]
#[async_trait]
impl ConnectionCallback for DebugConnectionCallback {
	async fn close(&mut self, connection: &Connection, close: Close) -> Result<(), Error> {
		eprintln!(
			"DEBUG: Connection({}) close(): {close:?}",
			connection.connection_name()
		);
		Ok(())
	}

	async fn blocked(&mut self, connection: &Connection, reason: String) {}

	async fn unblocked(&mut self, connection: &Connection) {}

	async fn secret_updated(&mut self, connection: &Connection) {}
}

/// Type to debug channel issues with AMQP.
#[cfg(debug_assertions)]
pub(crate) struct DebugChannelCallback;
#[cfg(debug_assertions)]
#[async_trait]
impl ChannelCallback for DebugChannelCallback {
	async fn close(&mut self, channel: &Channel, close: CloseChannel) -> Result<(), Error> {
		eprintln!(
			"DEBUG: Channel({}) close(): {close:?}",
			channel.channel_id()
		);
		Ok(())
	}

	async fn cancel(&mut self, channel: &Channel, cancel: Cancel) -> Result<(), Error> {
		eprintln!(
			"DEBUG: Channel({}) cancel(): {cancel:?}",
			channel.channel_id()
		);
		Ok(())
	}

	async fn flow(&mut self, channel: &Channel, active: bool) -> Result<bool, Error> {
		eprintln!("DEBUG: Channel({}) flow(): {active}", channel.channel_id());
		Ok(true)
	}

	async fn publish_ack(&mut self, channel: &Channel, ack: Ack) {
		eprintln!("DEBUG: Channel({}) ack(): {ack:?}", channel.channel_id());
	}

	async fn publish_nack(&mut self, channel: &Channel, nack: Nack) {
		eprintln!("DEBUG: Channel({}) nack(): {nack:?}", channel.channel_id());
	}

	async fn publish_return(
		&mut self,
		channel: &Channel,
		ret: Return,
		basic_properties: BasicProperties,
		content: Vec<u8>,
	) {
		eprintln!("DEBUG: Channel({}) return(): {ret:?}", channel.channel_id());
	}
}
