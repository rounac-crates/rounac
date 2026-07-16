//! Module for error type.

use std::{
	error::Error,
	fmt::{self, Display},
	io,
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CalErrorKind {
	Config,
	Io,
	Network,
	Topic,
	Other,
}
pub struct CalError {
	kind: CalErrorKind,
	data: Box<dyn Error>,
}
impl CalError {
	pub fn kind(&self) -> CalErrorKind {
		self.kind
	}

	/// Return a [CalError] with kind `Config`.
	pub fn config_err(msg: String) -> Self {
		CalError {
			kind: CalErrorKind::Config,
			data: msg.into(),
		}
	}

	/// Return a [CalError] with kind `Network`.
	pub fn net_err(msg: String) -> Self {
		CalError {
			kind: CalErrorKind::Network,
			data: msg.into(),
		}
	}

	/// Return a [CalError] with kind `Other`.
	pub fn other_err(msg: String) -> Self {
		CalError {
			kind: CalErrorKind::Other,
			data: msg.into(),
		}
	}

	/// Return a [CalError] with kind `Topic`.
	pub fn topic_err(msg: String) -> Self {
		CalError {
			kind: CalErrorKind::Topic,
			data: msg.into(),
		}
	}
}
impl Error for CalError {}
impl fmt::Debug for CalError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		Display::fmt(self, f)
	}
}
impl Display for CalError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		writeln!(f, "CalError({:?}): {}", self.kind, self.data)
	}
}

/// Macro to automate the [From] impls for various errors to [CalError].
macro_rules! calerror_conversions {
	{
		$($error:ty => $kind:expr)*
	} => {$(
		impl From<$error> for CalError {
			fn from(e: $error) -> Self {
				CalError {
					kind: $kind,
					data: e.into(),
				}
			}
		}
	)*}
}

calerror_conversions! {
	io::Error => CalErrorKind::Io
	amqprs::error::Error => CalErrorKind::Network
}
