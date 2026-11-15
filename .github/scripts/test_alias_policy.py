#!/usr/bin/env python3
"""alias_policy 回归测试。

覆盖 T18 审查发现的别名回退缺口：
- 重跑旧版本（HEAD 是旧 SHA，看不到后代 tag）时，必须靠远端全量 tag 判定为 backfill
- 只重跑 publish（没有重跑 release）时同样成立，因为它不依赖任何历史 run 的输出
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from alias_policy import decide  # noqa: E402


class TestAliasPolicy(unittest.TestCase):
    def test_first_release_allowed(self):
        self.assertEqual(decide("v0.0.1", []), "ok")

    def test_newest_release_allowed(self):
        self.assertEqual(decide("v1.2.4", ["v1.2.3", "v1.2.4"]), "ok")

    def test_backfill_old_release_blocked(self):
        """A(v1.2.3) 失败 -> B(v1.2.4) 成功 -> 重跑 A：A 不能动 latest/1/1.2。"""
        self.assertEqual(
            decide("v1.2.3", ["v1.2.3", "v1.2.4"]), "backfill newest=v1.2.4"
        )

    def test_backfill_major_style(self):
        self.assertEqual(
            decide("v1.9.0", ["v1.9.0", "v2.0.0"]), "backfill newest=v2.0.0"
        )

    def test_backfill_ignores_unrelated_older(self):
        self.assertEqual(
            decide("v1.2.4", ["v1.0.0", "v1.2.3", "v1.2.4"]), "ok"
        )

    def test_non_stable_tags_ignored(self):
        self.assertEqual(decide("v1.2.3", ["vfoo", "v1.2", "v1.2.3-rc.1"]), "ok")

    def test_target_not_stable(self):
        self.assertEqual(decide("v1.2", ["v1.2.3"]), "error_not_stable")
        self.assertEqual(decide("v1.2.3-rc.1", ["v1.2.3"]), "error_not_stable")

    def test_equal_newest_is_ok(self):
        """同一个 tag 重跑（幂等恢复），应允许推别名。"""
        self.assertEqual(decide("v1.2.4", ["v1.2.4"]), "ok")


if __name__ == "__main__":
    unittest.main(verbosity=2)
