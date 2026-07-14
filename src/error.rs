//! Module for error type.

use std::{
	error::Error,
	fmt::{self, Display},
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CalErrorKind {
	General,
	Config,
}
pub struct CalError {
	kind: CalErrorKind,
	data: Box<dyn Error>,
}
impl CalError {
	pub fn kind(&self) -> CalErrorKind {
		self.kind
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
