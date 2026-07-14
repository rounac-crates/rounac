//! AMQPRS related utilities

use crate::config::{AsbConfig, NetworkKind};
use amqprs::connection::OpenConnectionArguments;
use toml::Value;

/// Get the necessary config params to create AMQP connection.
pub fn open_args_from_config(name: &str, config: AsbConfig) -> Option<OpenConnectionArguments> {
	let network = config.networks.get(name)?;

	// Verify this network is the correct type.
	if network.kind != NetworkKind::Amqp {
		return None;
	}

	// Get parameters
	let Value::String(host) = network.params.get("host")? else {
		return None;
	};
	let Value::Integer(port) = network.params.get("port")? else {
		return None;
	};
	let Value::String(user) = network.params.get("username")? else {
		return None;
	};
	let Value::String(pass) = network.params.get("password")? else {
		return None;
	};

	Some(OpenConnectionArguments::new(host, *port as u16, user, pass))
}
