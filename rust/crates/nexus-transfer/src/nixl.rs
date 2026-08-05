//! NVIDIA NIXL (NVIDIA Inference Transfer Library) Transport Abstractions.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NixlBackendTransport {
    NvLink,
    RdmaRoCe,
    NvmeOf,
    Tcp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NixlTransferDescriptor {
    pub transfer_id: u64,
    pub backend: NixlBackendTransport,
    pub src_device_id: usize,
    pub dst_device_id: usize,
    pub payload_bytes: usize,
    pub is_wire_speed: bool,
}

impl NixlTransferDescriptor {
    pub fn new(
        transfer_id: u64,
        backend: NixlBackendTransport,
        src_device_id: usize,
        dst_device_id: usize,
        payload_bytes: usize,
    ) -> Result<Self, &'static str> {
        if payload_bytes == 0 {
            return Err("NIXL payload bytes must be greater than zero");
        }

        Ok(Self {
            transfer_id,
            backend,
            src_device_id,
            dst_device_id,
            payload_bytes,
            is_wire_speed: matches!(backend, NixlBackendTransport::NvLink | NixlBackendTransport::RdmaRoCe),
        })
    }
}
