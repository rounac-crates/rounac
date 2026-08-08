//! Parameter parsing.
//!

use crate::CalError;
use toml::{Table, Value};

pub(crate) struct ParamTool<'a>(pub &'a Table);
impl<'a> ParamTool<'a> {
	/// Get a required string parameter.
	pub fn get_str_req(&self, param: &str) -> Result<&'a str, CalError> {
		match self.0.get(param) {
			Some(Value::String(s)) => Ok(s),
			_ => Err(CalError::config_err(format_args!(
				"Expected string parameter \"{param}\"."
			))),
		}
	}

	/// Get an optional string parameter. Returns [Err] if parameter has wrong
	/// type.
	pub fn get_str(&self, param: &str) -> Result<Option<&'a str>, CalError> {
		match self.0.get(param) {
			Some(Value::String(s)) => Ok(Some(s)),
			Some(_) => Err(CalError::config_err(format_args!(
				"Expected string parameter \"{param}\"."
			))),
			None => Ok(None),
		}
	}

	/// Get a required integer parameter.
	pub fn get_int_req(&self, param: &str) -> Result<i64, CalError> {
		match self.0.get(param) {
			Some(Value::Integer(i)) => Ok(*i),
			_ => Err(CalError::config_err(format_args!(
				"Expected integer parameter \"{param}\"."
			))),
		}
	}

	/// Get an optional integer parameter. Returns [Err] if parameter has wrong
	/// type.
	pub fn get_int(&self, param: &str) -> Result<Option<i64>, CalError> {
		match self.0.get(param) {
			Some(Value::Integer(i)) => Ok(Some(*i)),
			Some(_) => Err(CalError::config_err(format_args!(
				"Expected integer parameter \"{param}\"."
			))),
			None => Ok(None),
		}
	}

	/// Get a required float parameter.
	pub fn get_float_req(&self, param: &str) -> Result<f64, CalError> {
		match self.0.get(param) {
			Some(Value::Float(f)) => Ok(*f),
			_ => Err(CalError::config_err(format_args!(
				"Expected float parameter \"{param}\"."
			))),
		}
	}

	/// Get an optional float parameter. Returns [Err] if parameter has wrong
	/// type.
	pub fn get_float(&self, param: &str) -> Result<Option<f64>, CalError> {
		match self.0.get(param) {
			Some(Value::Float(f)) => Ok(Some(*f)),
			Some(_) => Err(CalError::config_err(format_args!(
				"Expected float parameter \"{param}\"."
			))),
			None => Ok(None),
		}
	}

	/// Get a required boolean parameter.
	pub fn get_bool_req(&self, param: &str) -> Result<bool, CalError> {
		match self.0.get(param) {
			Some(Value::Boolean(b)) => Ok(*b),
			_ => Err(CalError::config_err(format_args!(
				"Expected boolean parameter \"{param}\"."
			))),
		}
	}

	/// Get an optional boolean parameter. Returns [Err] if parameter has wrong
	/// type.
	pub fn get_bool(&self, param: &str) -> Result<Option<bool>, CalError> {
		match self.0.get(param) {
			Some(Value::Boolean(b)) => Ok(Some(*b)),
			Some(_) => Err(CalError::config_err(format_args!(
				"Expected boolean parameter \"{param}\"."
			))),
			None => Ok(None),
		}
	}
}
