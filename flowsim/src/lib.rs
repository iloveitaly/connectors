use std::collections::BTreeMap;

use connector_protocol::materialize as protocol;
use convert::Convert;

pub mod convert;
pub mod spec;

pub fn validate_request(
    materialization: &proto_flow::flow::MaterializationSpec,
) -> protocol::Request {
    // The materialization has a list of bindings already. We need to make those into a validate binding.
    let bindings: Vec<protocol::ValidateBinding> = materialization
        .bindings
        .clone()
        .iter()
        .map(|proto_binding| protocol::ValidateBinding {
            collection: proto_binding
                .clone()
                .collection
                .expect("binding must have a collection")
                .convert(),
            resource_config: Box::new(raw_value(&proto_binding.resource_spec_json)),
            field_config: BTreeMap::new(), // TODO: Not supported
        })
        .collect();

    protocol::Request::Validate {
        name: materialization.materialization.clone(),
        config: raw_value(&materialization.endpoint_spec_json),
        bindings,
    }
}

pub fn apply_request(
    materialization: &proto_flow::flow::MaterializationSpec,
    validated_bindings: &[protocol::ValidatedBinding],
) -> protocol::Request {
    protocol::Request::Apply {
        name: materialization.materialization.clone(),
        config: raw_value(&materialization.endpoint_spec_json),
        bindings: apply_bindings(materialization, validated_bindings),
        version: "flowsim_version".to_string(),
        dry_run: false,
    }
}

pub fn open_request(
    materialization: &proto_flow::flow::MaterializationSpec,
    validated_bindings: &[protocol::ValidatedBinding],
) -> protocol::Request {
    protocol::Request::Open {
        name: materialization.materialization.clone(),
        config: raw_value(&materialization.endpoint_spec_json),
        bindings: apply_bindings(materialization, validated_bindings),
        version: "flowsim_version".to_string(),
        key_begin: 0, // Placeholder
        key_end: 0,   // Placeholder
        driver_checkpoint: raw_value("{}"),
    }
}

pub fn apply_bindings(
    materialization: &proto_flow::flow::MaterializationSpec,
    validated_bindings: &[protocol::ValidatedBinding],
) -> Vec<protocol::ApplyBinding> {
    validated_bindings
        .clone()
        .iter()
        .zip(materialization.clone().bindings.iter())
        .map(
            |(validated_binding, materialization_binding)| protocol::ApplyBinding {
                collection: materialization_binding
                    .clone()
                    .collection
                    .expect("binding must have a collection")
                    .convert(),
                resource_config: Box::new(raw_value(&materialization_binding.resource_spec_json)),
                resource_path: validated_binding.resource_path.clone(),
                delta_updates: validated_binding.delta_updates,
                field_selection: field_selection(validated_binding, materialization_binding),
            },
        )
        .collect()
}

// Just pick all the fields the connector can handle, required or optional.
fn field_selection(
    validated_binding: &protocol::ValidatedBinding,
    materialization_binding: &proto_flow::flow::materialization_spec::Binding,
) -> protocol::FieldSelection {
    // Get collection keys
    let keys = materialization_binding
        .collection
        .clone()
        .unwrap()
        .key_ptrs
        .iter()
        .map(|key| key.strip_prefix("/").unwrap_or(key).to_string())
        .collect::<Vec<String>>();

    // Values is everything from constraints that is required, optional, or recommend except the document and keys
    let values = validated_binding
        .constraints
        .iter()
        .filter_map(|constraint| {
            let field = constraint.0;
            if field == "flow_document" || keys.contains(field) {
                return None;
            }

            let cons = constraint.1;
            match cons.r#type {
                protocol::ConstraintType::FieldRequired
                | protocol::ConstraintType::FieldOptional => Some(field.to_string()),
                _ => None,
            }
        })
        .collect();

    protocol::FieldSelection {
        keys,
        values,
        document: Some("flow_document".to_string()), // Always materializate the document, maybe should be "" ?
        field_config: BTreeMap::new(),               // TODO: Not supported
    }
}

pub fn raw_value(json: &str) -> connector_protocol::RawValue {
    connector_protocol::RawValue(
        serde_json::value::RawValue::from_string(json.to_string()).unwrap(),
    )
}
