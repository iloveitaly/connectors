use std::fs::File;
use std::io::Read;

use proto_flow::flow::CaptureSpec;
use proto_flow::flow::CollectionSpec;
use proto_flow::flow::capture_spec;
use source_kafka::catalog;
use source_kafka::configuration;
use source_kafka::connector::Connector;
use source_kafka::connector::ConnectorConfig;
use source_kafka::state;
use support::assert_valid_json;
use support::mock_stdout;

use serde_json::json;

use crate::support::assert_empty;
use crate::support::parse_from_output;
use crate::support::parse_messages_from_output;

mod support;

#[test]
fn spec_test() {
    let mut stdout = mock_stdout();

    source_kafka::KafkaConnector::spec(&mut stdout).expect("spec command to succeed");

    insta::assert_yaml_snapshot!(parse_from_output(&stdout));
}

#[test]
fn check_test() {
    let mut stdout = mock_stdout();
    let config = local_config();

    source_kafka::KafkaConnector::validate(&mut stdout, config).expect("check command to succeed");

    insta::assert_yaml_snapshot!(parse_from_output(&stdout), {
        ".connectionStatus.message" => "{{ NUM_TOPICS_FOUND }}"
    });
}

#[test]
fn discover_test() {
    let mut stdout = mock_stdout();
    let config = local_config();

    source_kafka::KafkaConnector::discover(&mut stdout, config)
        .expect("discover command to succeed");

    // This is tricky to snapshot. It detects any other topics within the
    // connected Kafka, which will vary by dev machine.
    assert_valid_json(&stdout);
}

#[test]
fn read_simple_catalog_test() {
    let mut stdout = mock_stdout();
    let config = local_config();
    let catalog = local_capture("todo-list");

    source_kafka::KafkaConnector::read(&mut stdout, config, catalog, None, None)
        .expect("read command to succeed");

    let messages = parse_messages_from_output(&stdout);
    // Only look at the last 10 messages. Otherwise the snapshot file gets unwieldy.
    let last_ten = (messages.len() - 10)..;
    insta::assert_yaml_snapshot!(&messages[last_ten], {
        "[].record.emitted_at" => "{{ UNIX_TIMESTAMP }}"
    });
}

#[test]
fn read_resume_from_state_test() {
    let mut stdout = mock_stdout();
    let config = local_config();
    let catalog = local_capture("todo-list");

    let mut state = state::CheckpointSet::default();
    state.add(state::Checkpoint::new(
        "todo-list",
        0,
        state::Offset::UpThrough(37),
    ));
    state.add(state::Checkpoint::new(
        "todo-list",
        2,
        state::Offset::UpThrough(57),
    ));

    source_kafka::KafkaConnector::read(&mut stdout, config, catalog, None, Some(state))
        .expect("read command to succeed");

    insta::assert_yaml_snapshot!(parse_messages_from_output(&stdout), {
        "[].record.emitted_at" => "{{ UNIX_TIMESTAMP }}"
    });
}

fn local_config() -> configuration::Configuration {
    let mut file = File::open("tests/test-config.json").expect("to open test config file");
    let mut buf = String::new();
    file.read_to_string(&mut buf).expect("to read test config file");
    configuration::Configuration::parse(&buf).expect("to parse test config file")
}

fn local_capture(binding_name: &str) -> CaptureSpec {
    CaptureSpec {
        name: "capture".to_string(),
        bindings: vec![capture_spec::Binding {
            resource_config_json: serde_json::to_string(&catalog::Resource { stream: binding_name.to_string() }).unwrap(),
            resource_path: vec![binding_name.to_string()],
            collection: Some(CollectionSpec {
                ..Default::default()
            })
        }],
        ..Default::default()
    }
}
