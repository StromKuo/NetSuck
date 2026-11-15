#!/usr/bin/env python3
"""release.yml 编排级回归测试。

覆盖 T19 审查发现的 BLOCKER：
- metadata 步骤必须在 IMAGE_NAME 设置之后（否则 images 为空，tags 退化成裸版本号，镜像推不到 GHCR）
- 两个 metadata 分支必须显式设置 flavor.latest（避免 metadata-action 隐式生成 latest）
- 补推分支只能推精确版本，不能出现 major/minor 通配 pattern
- CI 不应同时监听 main push 和被 workflow_call 调用（否则一次提交跑两遍）
"""

import sys
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
RELEASE = ROOT / ".github" / "workflows" / "release.yml"
CI = ROOT / ".github" / "workflows" / "ci.yml"


def load(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def publish_steps() -> list[dict]:
    return load(RELEASE)["jobs"]["publish"]["steps"]


def step_index(predicate) -> int | None:
    for i, s in enumerate(publish_steps()):
        if predicate(s.get("name", "")):
            return i
    return None


def find_step(substr: str) -> dict | None:
    for s in publish_steps():
        if substr in s.get("name", ""):
            return s
    return None


class TestPublishStepOrder(unittest.TestCase):
    """T19 BLOCKER：IMAGE_NAME 必须先于任何引用它的步骤。"""

    def test_image_name_set_before_metadata(self):
        set_idx = step_index(lambda n: "Set lower-case image name" in n)
        self.assertIsNotNone(set_idx, "缺少设置 IMAGE_NAME 的步骤")

        meta_idxs = [
            i for i, s in enumerate(publish_steps())
            if "metadata" in s.get("name", "").lower()
        ]
        self.assertTrue(meta_idxs, "缺少 metadata 步骤")
        for i in meta_idxs:
            self.assertLess(
                set_idx, i,
                f"metadata 步骤(#{i}) 必须晚于 IMAGE_NAME 设置(#{set_idx})"
            )

    def test_image_name_set_before_build(self):
        set_idx = step_index(lambda n: "Set lower-case image name" in n)
        build_idxs = [
            i for i, s in enumerate(publish_steps())
            if s.get("name", "").startswith("Build and push")
        ]
        self.assertTrue(build_idxs)
        for i in build_idxs:
            self.assertLess(set_idx, i)

    def test_image_name_lowercased(self):
        step = find_step("Set lower-case image name")
        self.assertIn("${GITHUB_REPOSITORY_OWNER,,}", step["run"],
                      "镜像路径必须转小写（GHCR 要求）")


class TestMetadataFlavor(unittest.TestCase):
    """T18 MAJOR-3：metadata-action 对稳定 semver 隐式生成 latest。"""

    def _flavor(self, substr: str) -> str:
        step = find_step(substr)
        self.assertIsNotNone(step, f"缺少步骤: {substr}")
        return (step["with"].get("flavor") or "").strip()

    def test_full_alias_branch_sets_latest_true(self):
        flavor = self._flavor("full aliases")
        self.assertIn("latest=true", flavor)

    def test_immutable_branch_sets_latest_false(self):
        flavor = self._flavor("immutable only")
        self.assertIn("latest=false", flavor)

    def test_immutable_branch_has_no_wildcard_patterns(self):
        """补推历史版本不得出现 major / major.minor 通配，否则会劫持别名。"""
        step = find_step("immutable only")
        tags = step["with"]["tags"]
        self.assertNotIn("{{major}}", tags)
        self.assertIn("{{version}}", tags)


class TestAliasDecisionWiring(unittest.TestCase):
    """T18 MAJOR-1/2：判定必须在 publish 内实时进行，不依赖 release 输出。"""

    def test_no_resuming_old_output(self):
        release = load(RELEASE)
        outputs = release["jobs"]["release"].get("outputs") or {}
        self.assertNotIn(
            "resuming_old", outputs,
            "别名判定不能依赖 release job 的输出（历史 run 会沿用旧值）"
        )

    def test_fetch_failure_is_fatal(self):
        step = find_step("Decide alias eligibility")
        self.assertIsNotNone(step)
        self.assertIn("if ! git fetch --tags", step["run"],
                      "tag 同步失败必须中止，不能继续判定")


class TestCITriggers(unittest.TestCase):
    """去重：main push 只由 release.yml 通过 workflow_call 触发一次。"""

    def test_ci_not_listening_on_main_push(self):
        d = load(CI)
        on = d.get(True, d.get("on"))
        self.assertNotIn("push", on, "ci.yml 不应监听 main push（会与 workflow_call 重复）")

    def test_ci_supports_workflow_call(self):
        d = load(CI)
        on = d.get(True, d.get("on"))
        self.assertIn("workflow_call", on)

    def test_release_calls_ci(self):
        d = load(RELEASE)
        verify = d["jobs"].get("verify")
        self.assertIsNotNone(verify, "release.yml 缺少 verify job")
        self.assertEqual(verify.get("uses"), "./.github/workflows/ci.yml")
        self.assertIn("verify", load(RELEASE)["jobs"]["release"]["needs"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
