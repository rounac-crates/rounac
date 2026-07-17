//! AMQPRS related utilities

use crate::{
	config::{NetworkConfig, NetworkKind, WireFormat},
	error::CalError,
};
use amqprs::{
	BasicProperties, Deliver,
	channel::Channel,
	connection::{Connection, OpenConnectionArguments},
	consumer::AsyncConsumer,
};
use async_trait::async_trait;
use ringbuf::traits::Producer;
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
	pub buffer: ringbuf::HeapProd<T>,
}

#[async_trait]
impl<T: for<'de> Deserialize<'de> + Send> AsyncConsumer for AmqpConsumer<T> {
	async fn consume(&mut self, _: &Channel, _: Deliver, _: BasicProperties, data: Vec<u8>) {
		// Deserialize message
		if let Ok(msg) = crate::msg_serde::deserialize_msg(&self.format, &data) {
			// Add to ring buffer
			// TODO: Make custom ring buffer that allows producer to overwrite SYNCHRONOUSLY.
			_ = self.buffer.try_push(msg);
		}
	}
}
