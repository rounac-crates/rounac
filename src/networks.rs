//! Module for the network related types.

pub mod amqp;

use crate::{
	config::{AsbConfig, NetworkKind},
	error::CalError,
};
use amqp::open_args_for_net;
use amqprs::{callbacks::DefaultConnectionCallback, connection::Connection};

pub enum AsbConnection {
	Amqp(amqp::AmqpAsb),
	Null,
}
impl AsbConnection {
	pub fn connect(net_name: &str, config: &AsbConfig) -> Result<Self, CalError> {
		let Some(network) = config.networks.get(net_name) else {
			return Err(CalError::config_err(format!(
				"Missing network config for {net_name}"
			)));
		};

		let rt = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()?;

		match network.kind {
			NetworkKind::Amqp => {
				let open_args = open_args_for_net(&network)?;
				Ok(AsbConnection::Amqp(rt.block_on(async {
					let conn = Connection::open(&open_args).await?;
					conn.register_callback(DefaultConnectionCallback).await?;
					let chan = conn.open_channel(None).await?;

					let a = amqp::AmqpAsb { conn, chan };

					Ok::<_, amqprs::error::Error>(a)
				})?))
			}
			NetworkKind::Null => Ok(AsbConnection::Null),
		}
	}
}
