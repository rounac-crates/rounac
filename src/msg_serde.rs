//! Module for (de)serialization utilities.

use crate::{CalError, config::WireFormat};
use serde::{Deserialize, Serialize};

/// Serialize `msg` to bytes using the specified `format`.
pub fn serialize_msg<T: Serialize>(format: &WireFormat, msg: T) -> Result<Vec<u8>, CalError> {
	match format {
		WireFormat::Xml => {
			// Serialize message.
			let mut s = quick_xml::se::to_string(&msg)?;

			// Add `xmlns` to the root element.
			// TODO: Get this from message schema `targetNamespace`.
			const XMLNS: &str = " xmlns=\"https://www.vdl.afrl.af.mil/programs/oam\"";
			if let Some(i) = s.find('>') {
				s.insert_str(i, XMLNS);
			}

			Ok(s.into())
		}
	}
}

/// Deserialize `data` in the format `format` to a type `T`.
pub fn deserialize_msg<'de, T: Deserialize<'de>>(
	format: &WireFormat,
	data: &'de [u8],
) -> Result<T, CalError> {
	match format {
		WireFormat::Xml => {
			// First convert data to a [str].
			let Ok(data_str) = str::from_utf8(data) else {
				return Err(CalError::serde_err(
					"XML deserialization requires UTF-8 data".to_string(),
				));
			};

			quick_xml::de::from_str(data_str).map_err(|e| e.into())
		}
	}
}
