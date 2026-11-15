#!/usr/bin/env python3
"""next_version.py 的回归测试。

覆盖 T16 审查发现的误判场景：
- 正文里出现 "feat:" 不该被当成 minor（SIGPIPE 之外的另一个误判来源）
- BREAKING-CHANGE（连字符）必须等同 BREAKING CHANGE（空格）
- 类型大小写不敏感（Feat / FIX）
- 大量 commit 下 breaking 不能被 SIGPIPE 吞掉
- 非三段稳定 tag 必须报错，不能静默算错
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from next_version import (  # noqa: E402
    Commit,
    bump_version,
    classify,
    latest_stable_tag,
    parse_commits,
)


def commits_from(*texts: str) -> list[Commit]:
    raw = "\x00".join(texts)
    return parse_commits(raw)


class TestClassify(unittest.TestCase):
    def test_feat_is_minor(self):
        self.assertEqual(classify(commits_from("feat: add x")), "minor")

    def test_feat_with_scope_is_minor(self):
        self.assertEqual(classify(commits_from("feat(api): add x")), "minor")

    def test_fix_is_patch(self):
        self.assertEqual(classify(commits_from("fix: repair y")), "patch")

    def test_perf_and_refactor_are_patch(self):
        self.assertEqual(classify(commits_from("perf: faster")), "patch")
        self.assertEqual(classify(commits_from("refactor: cleaner")), "patch")

    def test_bang_is_major(self):
        self.assertEqual(classify(commits_from("feat!: drop api")), "major")
        self.assertEqual(classify(commits_from("feat(api)!: drop api")), "major")
        self.assertEqual(classify(commits_from("fix!: breaking fix")), "major")

    def test_breaking_change_footer_space(self):
        self.assertEqual(
            classify(commits_from("feat: x\n\nBREAKING CHANGE: removed endpoint")), "major"
        )

    def test_breaking_change_footer_hyphen(self):
        """审查发现：BREAKING-CHANGE（连字符）曾被漏判。"""
        self.assertEqual(
            classify(commits_from("feat: x\n\nBREAKING-CHANGE: removed endpoint")), "major"
        )

    def test_type_is_case_insensitive(self):
        """审查发现：Feat: 曾被漏判（规范允许任意大小写）。"""
        self.assertEqual(classify(commits_from("Feat: capitalized")), "minor")
        self.assertEqual(classify(commits_from("FIX: capitalized")), "patch")

    def test_feat_inside_body_does_not_count(self):
        """审查发现：正文里的 feat: 示例曾被误判为 minor。"""
        self.assertEqual(
            classify(commits_from("docs: update guide\n\nExample: feat: something")), "none"
        )

    def test_chore_and_docs_are_none(self):
        self.assertEqual(classify(commits_from("docs: readme", "chore: deps")), "none")

    def test_major_wins_over_minor(self):
        self.assertEqual(
            classify(commits_from("feat: a", "fix!: b", "fix: c")), "major"
        )

    def test_minor_wins_over_patch(self):
        self.assertEqual(classify(commits_from("fix: a", "feat: b")), "minor")

    def test_many_commits_breaking_not_swallowed(self):
        """审查发现：大量 commit 时 grep -q 触发 SIGPIPE，breaking 被静默吞掉。"""
        many = [f"docs: filler {i}" for i in range(5000)]
        many.insert(0, "feat!: breaking at the top")
        self.assertEqual(classify(commits_from(*many)), "major")


class TestBumpVersion(unittest.TestCase):
    def test_patch(self):
        self.assertEqual(bump_version("v1.2.3", "patch"), "v1.2.4")

    def test_minor(self):
        self.assertEqual(bump_version("v1.2.3", "minor"), "v1.3.0")

    def test_major(self):
        self.assertEqual(bump_version("v1.2.3", "major"), "v2.0.0")

    def test_from_zero(self):
        self.assertEqual(bump_version("v0.0.0", "minor"), "v0.1.0")

    def test_two_segment_tag_rejected(self):
        """审查发现：v1.2 曾被静默算成 v1.2.1。"""
        with self.assertRaises(SystemExit):
            bump_version("v1.2", "patch")

    def test_prerelease_tag_rejected(self):
        """审查发现：v1.2.3-rc.1 曾导致 unbound variable。"""
        with self.assertRaises(SystemExit):
            bump_version("v1.2.3-rc.1", "patch")

    def test_non_semver_tag_rejected(self):
        with self.assertRaises(SystemExit):
            bump_version("vfoo", "patch")


class TestParseCommits(unittest.TestCase):
    def test_subject_and_body_split(self):
        commits = commits_from("fix: subject\n\nbody line\nBREAKING CHANGE: x")
        self.assertEqual(len(commits), 1)
        self.assertEqual(commits[0].subject, "fix: subject")
        self.assertIn("BREAKING CHANGE", commits[0].body)

    def test_empty_chunks_ignored(self):
        self.assertEqual(parse_commits("\x00\x00"), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
