//! Basic status publish
//!
//! This example will demonstrate basic publishing using a `ServiceStatus`
//! message over an AMQP (RabbitMQ) network.

use chrono::TimeDelta;
use rounac::{Asb, QosSettings, Topic};
use rounac_uci::v2_5::{
	choices::OwnerProducerChoiceType,
	elements::ServiceStatus,
	enums::{ClassificationEnum, MessageModeEnum, OwnerProducerEnum, ServiceStateEnum},
	types::{
		HeaderType, SecurityInformationType, ServiceIdType, ServiceStatusMdt, ServiceStatusMt,
		SystemIdType,
	},
};
use std::time::{Duration, Instant};

// Simple configuration that will utilize the `amqp` network type.
const CONFIG: &str = r#"
system_uuid = "00000000-0000-0000-0000-000000000000"

[services.basic_status_publish]
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

/// Returns empty security information for an unclassified USA producer.
fn security_info() -> SecurityInformationType {
	SecurityInformationType {
		classification: ClassificationEnum::U,
		owner_producer: vec![OwnerProducerChoiceType::GovernmentIdentifier(
			OwnerProducerEnum::Usa,
		)],
		joint: None,
		sci_controls: Vec::new(),
		sar_identifier: Vec::new(),
		atomic_energy_markings: Vec::new(),
		dissemination_controls: Vec::new(),
		display_only_to: Vec::new(),
		fgi_source_open: Vec::new(),
		fgi_source_protected: Vec::new(),
		releasable_to: Vec::new(),
		non_ic_markings: Vec::new(),
		classified_by: None,
		compilation_reason: None,
		derivatively_classified_by: None,
		classification_reason: None,
		non_us_controls: Vec::new(),
		derived_from: None,
		declass_date: None,
		declass_event: None,
		declass_exception: Vec::new(),
		has_approximate_markings: None,
		high_water_nato: Vec::new(),
		cui_basic: Vec::new(),
		cui_specified: Vec::new(),
		cui_decontrol_date: None,
		cui_decontrol_event: None,
		cui_controlled_by: None,
		cui_controlled_by_office: None,
		cui_poc: None,
		second_banner_line: Vec::new(),
		handle_via_channels: None,
	}
}

/// Returns a message header for the given parameters.
fn header(
	schema_version: String,
	system_id: SystemIdType,
	service_id: ServiceIdType,
) -> HeaderType {
	HeaderType {
		system_id,
		timestamp: chrono::Utc::now(),
		schema_version,
		mode: MessageModeEnum::NonexerciseSimulation,
		service_id: Some(service_id),
		mission_id: None,
	}
}

/// Returns a service status for `service_id` with 0 uptime and normal state.
fn service_status_mdt(service_id: ServiceIdType) -> ServiceStatusMdt {
	ServiceStatusMdt {
		service_id,
		time_up: chrono::TimeDelta::seconds(0).into(),
		service_state: ServiceStateEnum::Normal,
		service_state_reason: Vec::new(),
		predicted_service_state: Vec::new(),
		enabled_settings: Vec::new(),
		supported_settings: Vec::new(),
	}
}

fn main() {
	// This must match the service name in the config to apply the configuration.
	const SVC_NAME: &str = "basic_status_publish";

	// Load the configuration and create the ASB + writer.
	let config = CONFIG.parse().unwrap();
	let asb = Asb::new(SVC_NAME, config).unwrap();
	let topic = Topic::<ServiceStatus>::new("status", QosSettings::default()).unwrap();
	let writer = asb.new_writer(&topic).unwrap();

	// Get the UCI schema version.
	let schema_ver = rounac_uci::v2_5::SCHEMA_VERSION.to_owned();

	// Make system ID with the UUID from the config.
	let system_id = SystemIdType {
		uuid: asb.get_system_uuid(),
		// System name can be whatever.
		descriptive_label: Some("My System".to_owned()),
	};

	// Make service ID with the UUID from the config.
	let service_id = ServiceIdType {
		uuid: asb.get_service_uuid(),
		// Matching this example name for clarity, but this is not necessary.
		descriptive_label: Some(SVC_NAME.to_owned()),
		// Use crate version (if there is one) for simplicity.
		service_version: option_env!("CARGO_PKG_VERSION").map(|v| v.to_string().into()),
	};

	// Make the status message.
	let mut status = ServiceStatus(ServiceStatusMt {
		security_information: security_info(),
		message_header: header(schema_ver, system_id, service_id.clone()),
		message_data: service_status_mdt(service_id.clone()),
	});

	// Loop and send a few status messages.
	let start = Instant::now();
	let mut count = 0;
	const TIME: Duration = Duration::from_secs(5);
	println!("Sending as fast as possible for {}s.", TIME.as_secs());
	while start.elapsed() < TIME {
		if let Ok(_) = writer.write(&status) {
			count += 1;
		}

		// Update service uptime.
		status.message_data.time_up = TimeDelta::from_std(start.elapsed()).unwrap().into();
	}

	println!("Sent {count} messages in {}s.", TIME.as_secs());
}
