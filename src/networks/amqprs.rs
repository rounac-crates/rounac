//! AMQPRS related utilities

use crate::{
	config::{AsbConfig, NetworkKind},
	error::CalError,
};
use amqprs::connection::OpenConnectionArguments;
use toml::Value;

/// Get the necessary config params to create AMQP connection for `net_name`.
pub fn open_args_for_net(
	net_name: &str,
	config: &AsbConfig,
) -> Result<OpenConnectionArguments, CalError> {
	let Some(network) = config.networks.get(net_name) else {
		return Err(CalError::config_err(format!(
			"No such network \"{net_name}\""
		)));
	};

	// Verify this network is the correct type.
	if network.kind != NetworkKind::Amqp {
		return Err(CalError::config_err(format!(
			"Expected network kind \"amqp\" for network \"{net_name}\""
		)));
	}

	// Get parameters
	let host = match network.params.get("host") {
		Some(Value::String(s)) => Ok(s),
		_ => Err(CalError::config_err(format!(
			"Expected string parameter \"host\" for network \"{net_name}\""
		))),
	}?;
	let port = match network.params.get("port") {
		Some(Value::Integer(i)) => Ok(i),
		_ => Err(CalError::config_err(format!(
			"Expected integer parameter \"port\" for network \"{net_name}\""
		))),
	}?;
	let user = match network.params.get("username") {
		Some(Value::String(s)) => Ok(s),
		_ => Err(CalError::config_err(format!(
			"Expected string parameter \"username\" for network \"{net_name}\""
		))),
	}?;
	let pass = match network.params.get("password") {
		Some(Value::String(s)) => Ok(s),
		_ => Err(CalError::config_err(format!(
			"Expected string parameter \"password\" for network \"{net_name}\""
		))),
	}?;

	Ok(OpenConnectionArguments::new(host, *port as u16, user, pass))
}
