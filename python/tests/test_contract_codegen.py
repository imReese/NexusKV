import json
import subprocess
import sys
import unittest
from pathlib import Path

from nexuskv.contracts.generated import (
    AttentionStateDescriptor,
    BufferKind,
    DeviceClass,
    EngineFamily,
    Granularity,
    LayoutMetadata,
    MaterializationCapability,
    MaterializationProfile,
    QuantizationMetadata,
    SCHEMA_VERSION,
    StateSemanticType,
    TensorRole,
    TensorSpec,
    TierKind,
    TransferBackend,
    TransferCapability,
    TransferPath,
)


ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schema" / "nexuskv_contract_v1.json"


class ContractCodegenTest(unittest.TestCase):
    def test_generator_outputs_are_current(self) -> None:
        result = subprocess.run(
            [sys.executable, str(ROOT / "tools" / "generate_contracts.py"), "--check"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_generated_schema_version_matches_source(self) -> None:
        schema = json.loads(SCHEMA_PATH.read_text())
        self.assertEqual(SCHEMA_VERSION, schema["schema_version"])

    def test_generated_descriptor_captures_transfer_and_materialization_capabilities(self) -> None:
        descriptor = AttentionStateDescriptor(
            schema_version=SCHEMA_VERSION,
            descriptor_id="vllm-gqa-page",
            engine_family=EngineFamily.VLLM,
            semantic_type=StateSemanticType.GQA_KV,
            granularity=Granularity.PAGE,
            tensor_specs=[
                TensorSpec(
                    name="k_cache",
                    role=TensorRole.KEY,
                    dtype="float16",
                    shape=["layers", "pages", "kv_heads", "page_tokens", "head_dim"],
                ),
                TensorSpec(
                    name="v_cache",
                    role=TensorRole.VALUE,
                    dtype="float16",
                    shape=["layers", "pages", "kv_heads", "page_tokens", "head_dim"],
                ),
            ],
            quantization=QuantizationMetadata(scheme="none", bits=16, group_size=0),
            layout=LayoutMetadata(layout="paged", page_tokens=16, block_tokens=16, packed=True),
            compatibility_flags=["paged", "partial-materialization"],
            transfer_paths=[
                TransferPath(
                    backend=TransferBackend.STAGED_COPY,
                    capabilities=[TransferCapability.ASYNC, TransferCapability.DEVICE_TO_HOST],
                )
            ],
            materialization=MaterializationProfile(
                capabilities=[MaterializationCapability.PARTIAL, MaterializationCapability.PREFETCH],
                tier_kinds=[TierKind.DEVICE, TierKind.HOST_DRAM, TierKind.REMOTE_SHARED],
                device_classes=[DeviceClass.CUDA, DeviceClass.CPU],
                buffer_kinds=[BufferKind.DEVICE, BufferKind.HOST_PINNED],
            ),
            layout_metadata={"engine_layout": "paged_kv"},
        )

        self.assertEqual(descriptor.transfer_paths[0].backend, TransferBackend.STAGED_COPY)
        self.assertIn(MaterializationCapability.PARTIAL, descriptor.materialization.capabilities)


if __name__ == "__main__":
    unittest.main()
