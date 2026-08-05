from __future__ import annotations

import unittest
from nexuskv.benchmarks.attention_cache_size import (
    ATTENTION_PROFILES,
    AttentionCacheConfig,
    StateSemanticType,
)


class TestAttentionCacheSize(unittest.TestCase):
    def test_mha_bytes_per_token(self) -> None:
        llama2_70b = ATTENTION_PROFILES[0]
        # 2 * 80 * 64 * 128 * 2 = 2,621,440 bytes/token
        self.assertEqual(llama2_70b.bytes_per_token(2), 2621440)
        # 4k context footprint = 10485.76 MB = 10.24 GB
        fp_4k = llama2_70b.calculate_footprint(4096, 2)
        self.assertAlmostEqual(fp_4k, 10240.0, places=1)

    def test_gqa_bytes_per_token(self) -> None:
        llama3_70b = ATTENTION_PROFILES[1]
        # 2 * 80 * 8 * 128 * 2 = 327,680 bytes/token (8x smaller than MHA)
        self.assertEqual(llama3_70b.bytes_per_token(2), 327680)

    def test_mla_deepseek_v3_bytes_per_token(self) -> None:
        deepseek_v3 = ATTENTION_PROFILES[2]
        # 61 * (512 + 64) * 2 = 70,272 bytes/token
        self.assertEqual(deepseek_v3.bytes_per_token(2), 70272)
        # 32k context footprint = 2196.0 MB = 2.14 GB
        fp_32k = deepseek_v3.calculate_footprint(32768, 2)
        self.assertAlmostEqual(fp_32k, 2196.0, places=1)


if __name__ == "__main__":
    unittest.main()
