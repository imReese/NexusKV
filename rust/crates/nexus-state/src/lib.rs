mod generated;

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Display, Formatter};

pub use generated::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DescriptorError {
    UnsupportedSchemaVersion(String),
    EmptyTensorSpecs,
    DuplicateRole(TensorRole),
}

impl Display for DescriptorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedSchemaVersion(version) => {
                write!(f, "unsupported schema version: {version}")
            }
            Self::EmptyTensorSpecs => write!(f, "tensor_specs must not be empty"),
            Self::DuplicateRole(role) => write!(f, "duplicate tensor role: {role:?}"),
        }
    }
}

impl Error for DescriptorError {}

pub fn validate_descriptor(descriptor: &AttentionStateDescriptor) -> Result<(), DescriptorError> {
    if descriptor.schema_version != SCHEMA_VERSION {
        return Err(DescriptorError::UnsupportedSchemaVersion(
            descriptor.schema_version.clone(),
        ));
    }
    if descriptor.tensor_specs.is_empty() {
        return Err(DescriptorError::EmptyTensorSpecs);
    }

    let mut roles = BTreeSet::new();
    for spec in &descriptor.tensor_specs {
        if !roles.insert(spec.role) {
            return Err(DescriptorError::DuplicateRole(spec.role));
        }
    }

    Ok(())
}

pub fn supports_partial_materialization(descriptor: &AttentionStateDescriptor) -> bool {
    descriptor
        .materialization
        .capabilities
        .contains(&MaterializationCapability::Partial)
        || matches!(
            descriptor.granularity,
            Granularity::Block | Granularity::Page | Granularity::Segment
        )
}
