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
use std::{
	collections::HashMap,
	sync::{Arc, OnceLock, RwLock},
};
use tokio::runtime::Handle;
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

pub(crate) struct AmqpAsb {
	pub rt_handle: Handle,
	pub conn: Connection,
	pub chan: Channel,
	pub exchange: Option<String>,
}

pub struct AmqpConsumer<T> {
	pub format: WireFormat,
	pub buffer: RingSender<Arc<T>>,
}

#[async_trait]
impl<T: for<'de> Deserialize<'de> + Send + Sync> AsyncConsumer for AmqpConsumer<T> {
	async fn consume(&mut self, _: &Channel, _: Deliver, _: BasicProperties, data: Vec<u8>) {
		// Deserialize message
		if let Ok(msg) = crate::msg_serde::deserialize_msg(&self.format, &data) {
			// Add to ring buffer
			_ = self.buffer.send(Arc::new(msg));
		}
	}
}

pub(crate) struct ConnCb;
#[async_trait]
impl ConnectionCallback for ConnCb {
	async fn close(&mut self, connection: &Connection, close: Close) -> Result<(), Error> {
		// TODO: Have a way to relay error condition to [AsbConnection].
		eprintln!(
			"ERROR: Connection({}) closed by server: {close}",
			connection.connection_name()
		);
		Ok(())
	}

	async fn blocked(&mut self, connection: &Connection, reason: String) {}

	async fn unblocked(&mut self, connection: &Connection) {}

	async fn secret_updated(&mut self, connection: &Connection) {}
}

pub(crate) struct ChanCb;
#[async_trait]
impl ChannelCallback for ChanCb {
	async fn close(&mut self, chan: &Channel, close_channel: CloseChannel) -> Result<(), Error> {
		// TODO: Have a way to relay error condition to [AsbConnection].
		eprintln!(
			"ERROR: Channel({}) closed by server: {close_channel}",
			chan.channel_id()
		);
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
