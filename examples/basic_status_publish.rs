//! Basic status publish
//!
//! This example will demonstrate basic publishing using a `ServiceStatus`
//! message over an AMQP (RabbitMQ) network.

use rounac::{Asb, QosSettings, Topic};
use rounac_uci::v2_5::{
	choices::OwnerProducerChoiceType,
	enums::{ClassificationEnum, OwnerProducerEnum},
	types::SecurityInformationType,
};

const CONFIG: &str = r#"
system_uuid = "00000000-0000-0000-0000-000000000000"

[services.basic_publish]
service_uuid = "00000000-0000-4000-8000-0123456789AB"
network = "rabbit"

[networks.rabbit]
kind = "amqp"
host = "localhost"
port = 5672
username = "guest"
password = "guest"
"#;

fn security_info(classification: ClassificationEnum) -> SecurityInformationType {
	SecurityInformationType {
		classification,
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

//fn make_status()

fn main() {
	let config = CONFIG.parse().unwrap();
	let asb = Asb::new("basic_publish", config).unwrap();
	let topic = Topic::new("status", QosSettings::default()).unwrap();
	let writer = asb.new_writer(&topic).unwrap();
}
