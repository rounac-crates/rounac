//! Basic OMS service.
//!
//! This example will be a barebones OMS service, which has two main
//! requirements:
//!
//! * Periodically sends a `ServiceStatus` message, at a configurable interval
//!   of `[1, 600]` seconds.
//! * Aperiodically (upon request) sends a `ServiceStatusDataRequestStatus`
//!   message in response to a `ServiceStatusDataRequest` message.

use chrono::TimeDelta;
use rounac::{Asb, AsbListener, AsbReader, AsbWriter, CalError, config::AsbConfig};
use rounac_uci::v2_5::{
	choices::OwnerProducerChoiceType,
	elements::{ServiceStatus, ServiceStatusDataRequest, ServiceStatusDataRequestStatus},
	enums::{
		ClassificationEnum, MessageModeEnum, OwnerProducerEnum, RequestProcessingStateEnum,
		ServiceStateEnum,
	},
	traits::IdType,
	types::{
		HeaderType, RequestIdType, SecurityInformationType, ServiceIdType,
		ServiceStatusDataRequestStatusMdt, ServiceStatusDataRequestStatusMt, ServiceStatusMdt,
		ServiceStatusMt, SystemIdType,
	},
};
use std::{
	sync::{
		Arc,
		atomic::{AtomicU8, Ordering},
	},
	time::{Duration, Instant},
};

// Simple configuration that will utilize the `amqp` network type.
const CONFIG: &str = r#"
[services.basic_oms_service]
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

/// Determines if any two [IdType] are equal, based on their UUIDs.
fn ids_eq<I1: IdType, I2: IdType>(id1: &I1, id2: &I2) -> bool {
	id1.get_uuid() == id2.get_uuid()
}

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

/// Convenience type for [ServiceStateEnum] that tracks state atomically.
#[derive(Debug)]
struct SyncServiceState(AtomicU8);
impl SyncServiceState {
	pub fn get_state(&self) -> ServiceStateEnum {
		match self.0.load(Ordering::Acquire) {
			x if x == ServiceStateEnum::Initializing as u8 => ServiceStateEnum::Initializing,
			x if x == ServiceStateEnum::Normal as u8 => ServiceStateEnum::Normal,
			x if x == ServiceStateEnum::Degraded as u8 => ServiceStateEnum::Degraded,
			x if x == ServiceStateEnum::Paused as u8 => ServiceStateEnum::Paused,
			x if x == ServiceStateEnum::Inoperable as u8 => ServiceStateEnum::Inoperable,
			// SAFETY: Users shall only use [set_state] to change the value.
			_ => unreachable!(),
		}
	}

	pub fn set_state(&self, state: ServiceStateEnum) {
		self.0.store(state as u8, Ordering::Release);
	}
}
impl Clone for SyncServiceState {
	/// Clones state using [Acquire][Ordering::Acquire] ordering.
	fn clone(&self) -> Self {
		SyncServiceState(AtomicU8::new(self.0.load(Ordering::Acquire)))
	}
}
impl From<ServiceStateEnum> for SyncServiceState {
	fn from(state: ServiceStateEnum) -> Self {
		SyncServiceState(AtomicU8::new(state as u8))
	}
}
impl Into<ServiceStateEnum> for SyncServiceState {
	fn into(self) -> ServiceStateEnum {
		self.get_state()
	}
}

/// A minimal OMS service.
///
/// This service adheres to the minimum reporting requirements for an OMS
/// service [[1]][1].
///
/// [1]: https://gitlab.com/open-arsenal/oms/standard/-/blob/main/docs_markdown_unofficial/14_1_OMSC-TMP-003_RevM_ServiceContractTemplate_DandD_v2_5.md?ref_type=heads#service-status-inputs-and-outputs
struct Service {
	system_id: SystemIdType,
	service_id: ServiceIdType,
	state: SyncServiceState,
	service_start: Instant,
	report_interval: Duration,
	asb: Asb,
	status_writer: AsbWriter<ServiceStatus>,
	status_req_reader: AsbReader<ServiceStatusDataRequest>,
	status_response_writer: AsbWriter<ServiceStatusDataRequestStatus>,
}
impl Service {
	pub fn new(name: &str, config: AsbConfig) -> Result<Arc<Self>, CalError> {
		// Get reporting interval from env, or default to 30 seconds if not set.
		let report_interval = match std::env::var("REPORT_INTERVAL") {
			Ok(v) => {
				let secs = v.parse().expect("integer seconds in the range [1, 600]");
				Duration::from_secs(secs)
			}
			Err(_) => Duration::from_secs(30),
		};

		// Connect to the ASB.
		let asb = Asb::new(name, config)?;

		// Start normal since no initialization needed outside this function.
		let state = ServiceStateEnum::Normal.into();

		// Make system and service ID
		let system_id = SystemIdType {
			uuid: asb.get_system_uuid(),
			descriptive_label: None,
		};
		let service_id = ServiceIdType {
			uuid: asb.get_service_uuid(),
			descriptive_label: None,
			service_version: None,
		};

		// Create readers and writers
		let status_writer = asb.new_writer("ServiceStatus")?;
		let status_req_reader = asb.new_reader("ServiceStatusDataRequest")?;
		let status_response_writer = asb.new_writer("ServiceStatusDataRequestStatus")?;

		// Create the service
		let service = Arc::new(Service {
			system_id,
			service_id,
			state,
			report_interval,
			service_start: Instant::now(),
			asb,
			status_writer,
			status_req_reader,
			status_response_writer,
		});

		// Immediately send out first status report.
		// NOTE: Commented here because [run_for] will do it right away.
		//service.send_status()?;

		// Register this service's listener for status requests
		service.status_req_reader.add_listener(service.clone());

		Ok(service)
	}

	// Simple fn so everything runs but not forever.
	pub fn run_for(self: Arc<Self>, dur: Duration) {
		let start = Instant::now();
		let mut run_time = Duration::ZERO;
		let mut sleep_time;

		while run_time < dur {
			// Send status, ignore error for simplicity.
			_ = self.send_status();

			// Sleep till next report or end of duration.
			sleep_time = dur.saturating_sub(run_time).min(self.report_interval);
			std::thread::sleep(sleep_time);
			run_time = start.elapsed();
		}
	}

	pub fn uptime(&self) -> Duration {
		self.service_start.elapsed()
	}

	fn current_status_data(&self) -> ServiceStatusMdt {
		ServiceStatusMdt {
			service_id: self.service_id.clone(),
			// SAFETY: Impossible to fail, barring monotomic clock error or bit flips.
			time_up: TimeDelta::from_std(self.uptime()).unwrap(),
			service_state: self.state.get_state(),
			service_state_reason: Vec::new(),
			predicted_service_state: Vec::new(),
			supported_settings: Vec::new(),
			enabled_settings: Vec::new(),
		}
	}

	/// Sends the periodic status message.
	fn send_status(&self) -> Result<(), CalError> {
		// Make the status message.
		let security_information = security_info();
		let message_header = header(
			rounac_uci::v2_5::SCHEMA_VERSION.to_string(),
			self.system_id.clone(),
			self.service_id.clone(),
		);
		let status = ServiceStatusMt {
			security_information,
			message_header,
			message_data: self.current_status_data(),
		}
		.into();

		// Send message
		self.status_writer.write(&status)
	}

	/// Determine whether this status request applies to us.
	///
	/// Per the service template, a request applies to this service if:
	///
	/// * The SystemID list contains ours OR is omitted.
	/// * AND the ServiceID list contains ours OR is omitted.
	///
	/// See the graphic [here][1].
	///
	/// [1]: https://gitlab.com/open-arsenal/oms/standard/-/raw/main/docs_markdown_unofficial/images/14_1_OMSC-TMP-003_RevM_ServiceContractTemplate_DandD_v2_5.docx/media/image5.png?ref_type=heads
	fn does_status_request_apply(&self, req: &ServiceStatusDataRequest) -> bool {
		let system_ids = &req.message_data.system_id;
		let service_ids = &req.message_data.service_id;

		// Check both conditions
		let system_ok =
			system_ids.iter().any(|id| ids_eq(id, &self.system_id)) || system_ids.is_empty();
		let service_ok =
			service_ids.iter().any(|id| ids_eq(id, &self.service_id)) || system_ids.is_empty();

		// Applies if both are true
		system_ok && service_ok
	}

	fn make_status_reply(&self, request_id: RequestIdType) -> ServiceStatusDataRequestStatus {
		// Make the reply.
		let security_information = security_info();
		let message_header = header(
			rounac_uci::v2_5::SCHEMA_VERSION.to_string(),
			self.system_id.clone(),
			self.service_id.clone(),
		);

		ServiceStatusDataRequestStatusMt {
			security_information,
			message_header,
			message_data: ServiceStatusDataRequestStatusMdt {
				request_id,
				request_processing_state: RequestProcessingStateEnum::Completed,
				request_processing_state_reason: None,
				service_status_data: Some(self.current_status_data()),
			},
		}
		.into()
	}
}
impl AsbListener<ServiceStatusDataRequest> for Service {
	fn on_msg(&self, msg: Arc<ServiceStatusDataRequest>) {
		// If the request applies, reply. Ignore otherwise.
		if self.does_status_request_apply(&msg) {
			let reply = self.make_status_reply(msg.message_data.request_id.clone());

			// Ignoring error for simplicity. Could attempt a retry on failure.
			_ = self.status_response_writer.write(&reply);
		}
	}
}

fn main() {
	// This must match the service name in the config to apply the configuration.
	const SVC_NAME: &str = "basic_oms_service";

	// Load the configuration and create the ASB.
	let config = CONFIG.parse().unwrap();
	println!("Starting service {SVC_NAME}..");
	let service = Service::new(SVC_NAME, config).unwrap();

	// Extra 2 seconds to try and make sure 2 reports get sent.
	const RUN_TIME: Duration = Duration::from_secs(62);
	println!("Running for {}s..", RUN_TIME.as_secs());
	service.run_for(RUN_TIME);
}
