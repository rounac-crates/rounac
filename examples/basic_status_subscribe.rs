//! Basic status subscribe
//!
//! This example will demonstrate basic subscribing using a `ServiceStatus`
//! message from an AMQP (RabbitMQ) network.

use rounac::{Asb, Topic};
use rounac_uci::v2_5::elements::ServiceStatus;
use std::time::{Duration, Instant};

// Simple configuration that will utilize the `amqp` network type.
const CONFIG: &str = r#"
system_uuid = "00000000-0000-0000-0000-000000000000"

[services.basic_status_subscribe]
service_uuid = "00000000-0000-4000-8000-0123456789AB"
network = "rabbit"
wire_format = "xml"

[networks.rabbit]
kind = "amqp"
host = "localhost"
port = 5672
username = "guest"
password = "guest"
exchange = "rounac"
"#;

fn main() {
	// This must match the service name in the config to apply the configuration.
	const SVC_NAME: &str = "basic_status_subscribe";

	// Load the configuration and create the ASB + reader.
	let config = CONFIG.parse().unwrap();
	let asb = Asb::new(SVC_NAME, config).unwrap();
	let topic = Topic::<ServiceStatus>::new("status").unwrap();
	let reader = asb.new_reader(&topic).unwrap();

	// Get the UCI schema version.
	let schema_ver = rounac_uci::v2_5::SCHEMA_VERSION.to_owned();

	// Loop and send a few status messages.
	let listen_time = Duration::from_secs(10);
	let start = Instant::now();
	let mut now;
	let mut remaining = listen_time;

	println!(
		"Listening for status messages for {}s.",
		listen_time.as_secs()
	);
	while !remaining.is_zero() {
		match reader.read_timeout(remaining) {
			// Print some information and check schema if message received.
			Ok(Some(msg)) => {
				println!("Received status from {}!", msg.message_data.service_id.uuid);

				// Check schema version in header just to do something.
				if msg.message_header.schema_version != schema_ver {
					eprintln!("Status has mismatched schema version!!");
				}
			}
			// No message no error just keep going.
			Ok(None) => {}
			// If error stop trying to receive.
			Err(e) => {
				eprintln!("Reader error: {e}");
				break;
			}
		};

		now = start.elapsed();
		remaining = listen_time.saturating_sub(now);
	}
}
