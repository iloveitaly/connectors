pub trait Convert {
    type Target;
    #[must_use]
    fn convert(self: Self) -> Self::Target;
}

impl Convert for proto_flow::flow::CollectionSpec {
    type Target = connector_protocol::CollectionSpec;
    fn convert(self: Self) -> Self::Target {
        let Self {
            ack_json_template: _,
            collection,
            key_ptrs,
            partition_fields,
            partition_template: _,
            projections,
            read_schema_json,
            read_schema_uri: _,
            uuid_ptr: _,
            write_schema_json,
            write_schema_uri: _,
        } = self;

        let (schema, read_schema, write_schema) = if read_schema_json.is_empty() {
            (
                Some(serde_json::from_str(&write_schema_json).unwrap()),
                None,
                None,
            )
        } else {
            (
                None,
                Some(serde_json::from_str(&read_schema_json).unwrap()),
                Some(serde_json::from_str(&write_schema_json).unwrap()),
            )
        };

        Self::Target {
            name: collection,
            key: key_ptrs,
            partition_fields,
            projections: projections.into_iter().map(Convert::convert).collect(),
            schema,
            write_schema,
            read_schema,
        }
    }
}

impl Convert for proto_flow::flow::Projection {
    type Target = connector_protocol::Projection;
    fn convert(self: Self) -> Self::Target {
        let Self {
            ptr,
            field,
            explicit,
            is_partition_key,
            is_primary_key,
            inference,
        } = self;

        Self::Target {
            ptr,
            field,
            explicit,
            is_partition_key,
            is_primary_key,
            inference: inference.unwrap().convert(),
        }
    }
}

impl Convert for proto_flow::flow::Inference {
    type Target = connector_protocol::Inference;
    fn convert(self: Self) -> Self::Target {
        let Self {
            types,
            string,
            title,
            description,
            default_json,
            secret,
            exists,
        } = self;

        Self::Target {
            types,
            string: string.map(Convert::convert),
            title,
            description,
            default: if default_json.is_empty() {
                serde_json::from_str("null").unwrap()
            } else {
                serde_json::from_str(&default_json).unwrap()
            },
            secret,
            exists: proto_flow::flow::inference::Exists::from_i32(exists)
                .unwrap()
                .convert(),
        }
    }
}

impl Convert for proto_flow::flow::inference::String {
    type Target = connector_protocol::StringInference;
    fn convert(self: Self) -> Self::Target {
        let Self {
            content_encoding,
            content_type,
            format,
            is_base64,
            max_length,
        } = self;

        Self::Target {
            content_encoding,
            content_type,
            format,
            is_base64,
            max_length: max_length as usize,
        }
    }
}

impl Convert for proto_flow::flow::inference::Exists {
    type Target = connector_protocol::Exists;
    fn convert(self: Self) -> Self::Target {
        match self {
            proto_flow::flow::inference::Exists::Must => Self::Target::Must,
            proto_flow::flow::inference::Exists::May => Self::Target::May,
            proto_flow::flow::inference::Exists::Implicit => Self::Target::Implicit,
            proto_flow::flow::inference::Exists::Cannot => Self::Target::Cannot,
            proto_flow::flow::inference::Exists::Invalid => unreachable!("invalid exists variant"),
        }
    }
}
