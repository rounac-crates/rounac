//! Position Report Detailed benchmark publisher.

use chrono::Utc;
use rand::random;
use rounac::Asb;
use rounac_uci::v2_5::{
	choices::{OwnerProducerChoiceType, PointChoice4DType, PositionSourceIdChoiceType},
	elements::PositionReportDetailed,
	enums::{
		AltitudeReferenceEnum, ClassificationEnum, MessageModeEnum, NavigationSolutionStateEnum,
		OwnerProducerEnum,
	},
	types::{
		DetailedKinematicsErrorType, DetailedKinematicsType, HeaderType, OrientationType,
		Point4DType, PositionPositionCovarianceType, PositionReportDataType,
		PositionReportDetailedMdt, PositionReportDetailedMt, PositionVelocityCovarianceType,
		SecurityInformationType, ServiceIdType, SystemIdType, Velocity3DType,
		VelocityVelocityCovarianceType,
	},
};
use std::time::{Duration, Instant};

const CONFIG: &str = r#"
[services.prd_benchmark_publisher]
network = "artemis"
wire_format = "xml"

[networks.rabbit]
kind = "amqp"
host = "localhost"
port = 5672
username = "guest"
password = "guest"
exchange = "rounac"

[networks.artemis]
kind = "mqtt"
url = "mqtt://artemis:artemis@localhost:1883?client_id=prd_benchmark_publisher"
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

fn random_pp_cov() -> PositionPositionCovarianceType {
	PositionPositionCovarianceType {
		pn_pn: random(),
		pn_pe: random(),
		pe_pe: random(),
		..Default::default()
	}
}
fn random_pv_cov() -> PositionVelocityCovarianceType {
	PositionVelocityCovarianceType {
		pn_vn: random(),
		pn_ve: random(),
		pe_vn: random(),
		pe_ve: random(),
		..Default::default()
	}
}
fn random_vv_cov() -> VelocityVelocityCovarianceType {
	VelocityVelocityCovarianceType {
		vn_vn: random(),
		vn_ve: random(),
		ve_ve: random(),
		..Default::default()
	}
}

/// Returns PRD data.
fn prd_mdt(service_id: ServiceIdType) -> PositionReportDetailedMdt {
	let now = Utc::now();

	PositionReportDetailedMdt {
		position_report_data: vec![PositionReportDataType {
			position_source: PositionSourceIdChoiceType::ServiceId(service_id),
			component_id: None,
			navigation_solution_state: NavigationSolutionStateEnum::FreeInertial,
			figure_of_merit: Some(7),
			kinematics: DetailedKinematicsType {
				position: PointChoice4DType::AbsolutePoint(Point4DType {
					latitude: 1.234567,
					longitude: -10.987654,
					altitude: 100.0,
					altitude_reference: Some(AltitudeReferenceEnum::WgsHae),
					timestamp: now.clone(),
					depth_category: None,
					hae_adjustment: None,
				}),
				velocity: Velocity3DType {
					north_speed: 100.0,
					east_speed: -5.0,
					down_speed: 1.0,
					timestamp: Some(now.clone()),
				},
				air_data: None,
				acceleration: None,
				orientation: Some(OrientationType {
					yaw: 0.08726646,
					pitch: 0.03490659,
					roll: -0.01745329,
					timestamp: Some(now.clone()),
				}),
				wander_angle: None,
				magnetic_heading: Some(6.0),
				orientation_rate: None,
				orientation_acceleration: None,
			},
			kinematics_error: DetailedKinematicsErrorType {
				position_position_covariance: random_pp_cov(),
				position_velocity_covariance: random_pv_cov(),
				velocity_velocity_covariance: random_vv_cov(),
				orientation_orientation_covariance: None,
				position_orientation_covariance: None,
				velocity_orientation_covariance: None,
			},
			solution_corrections: None,
		}],
		simulation_target_number: None,
	}
}

fn main() {
	// This must match the service name in the config to apply the configuration.
	const SVC_NAME: &str = "prd_benchmark_publisher";

	// Load the configuration and create the ASB + writer.
	let config = CONFIG.parse().unwrap();
	let asb = Asb::new(SVC_NAME, config).unwrap();
	let writer = asb.new_writer::<PositionReportDetailed>("prd").unwrap();

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

	// Make the message.
	let mut pos = PositionReportDetailed(PositionReportDetailedMt {
		security_information: security_info(),
		message_header: header(schema_ver, system_id, service_id.clone()),
		message_data: prd_mdt(service_id.clone()),
	});

	// Loop and send a few status messages.
	let start = Instant::now();
	let mut total_time = Duration::ZERO;
	let mut count = 0;
	const TIME: Duration = Duration::from_mins(15);
	println!("Sending as fast as possible for {}s.", TIME.as_secs());
	let mut last_update = Duration::ZERO;
	let mut write_time = 0;
	let mut last_count = 0;
	while total_time < TIME {
		let now = Instant::now();
		if let Ok(_) = writer.write(&pos) {
			count += 1;
			write_time += now.elapsed().as_nanos();
		} else {
			eprintln!("ERROR");
		}

		if (total_time - last_update) >= Duration::from_secs(1) {
			let avg_write = write_time / (count - last_count);
			println!(
				"Total sent after {} is {count}. Avg write: {}ns",
				total_time.as_secs(),
				avg_write
			);
			last_update = total_time;
			write_time = 0;
			last_count = count;
		}

		// Update timestamps.
		let now = Utc::now();
		pos.message_header.timestamp = now.clone();
		for prd in pos.message_data.position_report_data.iter_mut() {
			if let PointChoice4DType::AbsolutePoint(ref mut pt) = prd.kinematics.position {
				pt.timestamp = now.clone()
			}

			prd.kinematics.velocity.timestamp = Some(now.clone());

			if let Some(ref mut orient) = prd.kinematics.orientation {
				orient.timestamp = Some(now.clone());
			}
		}

		// Update total time
		total_time = start.elapsed();
	}

	println!("Sent {count} messages in {}s.", TIME.as_secs());
}
