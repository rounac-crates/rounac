//! Module for error type.

use std::{
	error::Error,
	fmt::{self, Display},
	io,
	sync::Arc,
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CalErrorKind {
	/// Error pertaining to the ASB configuration.
	Config,
	/// Error indicating the desired action is disallowed by the CAL spec.
	Illegal,
	/// Wrapped [std::io::Error].
	Io,
	/// Error pertaining to a network connection.
	Network,
	/// Error pertaining to (de)serialization of a message.
	Serde,
	/// An error not covered by another category.
	Other,
}

macro_rules! kind_helpers {
	{
		$($fn_name:ident -> $kind:expr)*
	} => {
		$(
		#[doc = concat!("Return a [CalError] with kind `", stringify!($kind), "`.")]
		pub(crate) fn $fn_name<D: Display>(msg: D) -> Self {
			use CalErrorKind::*;

			CalError {
				kind: $kind,
				data: Arc::from(Box::<dyn Error>::from(msg.to_string())),
			}
		}
		)*
	};
}

#[derive(Clone)]
pub struct CalError {
	kind: CalErrorKind,
	data: Arc<dyn Error>,
}
impl CalError {
	pub fn kind(&self) -> CalErrorKind {
		self.kind
	}

	pub(crate) fn new(kind: CalErrorKind, err: impl Error + 'static) -> Self {
		CalError {
			kind,
			data: Arc::from(err),
		}
	}

	kind_helpers! {
		config_err -> Config
		ill_err -> Illegal
		net_err -> Network
		other_err -> Other
		serde_err -> Serde
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
					data: Arc::from(Box::<dyn Error>::from(e)),
				}
			}
		}
	)*}
}

calerror_conversions! {
	io::Error => CalErrorKind::Io
	amqprs::error::Error => CalErrorKind::Network
	quick_xml::errors::Error => CalErrorKind::Serde
	quick_xml::errors::serialize::DeError => CalErrorKind::Serde
	quick_xml::errors::serialize::SeError => CalErrorKind::Serde
}
