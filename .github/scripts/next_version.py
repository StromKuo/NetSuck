#!/usr/bin/env python3
"""根据 Conventional Commits 推算下一个语义化版本。

用法:
    next_version.py --latest v1.2.3 --commits commits.txt
    next_version.py --latest v1.2.3 --range v1.2.3..HEAD

输出（stdout，两行）:
    bump=<major|minor|patch|none>
    version=<vX.Y.Z 或空>

设计要点（对应 T16 审查发现）:
- 不用 shell 管道 + grep -q，避免 SIGPIPE 把匹配静默变成 false；
- 按 commit 分离 subject 与 body/footer，正文里出现的 "feat:" 示例不会误判；
- 支持规范允许的 BREAKING-CHANGE（连字符）与 BREAKING CHANGE（空格）；
- 类型大小写不敏感（规范允许任意大小写）；
- 只接受三段稳定版本 tag（vX.Y.Z），预发布/两段 tag 直接报错而非静默算错。
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass

# Conventional Commits: type(scope)!: description
# type 不限定为小写（规范允许任意大小写），scope 可选，! 表示 breaking
SUBJECT_RE = re.compile(r"^(?P<type>[A-Za-z0-9_-]+)(?:\((?P<scope>[^()]*)\))?(?P<breaking>!)?:\s")

# footer 中的 breaking 标记：BREAKING CHANGE 与 BREAKING-CHANGE 等价
BREAKING_FOOTER_RE = re.compile(r"^BREAKING[ -]CHANGE:", re.MULTILINE)

STABLE_TAG_RE = re.compile(r"^v(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)$")

# 触发 patch 的类型
PATCH_TYPES = {"fix", "perf", "refactor"}
MINOR_TYPES = {"feat"}


@dataclass
class Commit:
    subject: str
    body: str

    @property
    def ctype(self) -> str | None:
        m = SUBJECT_RE.match(self.subject)
        if not m:
            return None
        return m.group("type").lower()

    @property
    def is_breaking(self) -> bool:
        m = SUBJECT_RE.match(self.subject)
        if m and m.group("breaking"):
            return True
        return bool(BREAKING_FOOTER_RE.search(self.body))


def parse_commits(raw: str) -> list[Commit]:
    """按 NUL 分隔解析，首行为 subject，其余为 body/footer。"""
    commits: list[Commit] = []
    for chunk in raw.split("\x00"):
        chunk = chunk.strip("\n")
        if not chunk:
            continue
        lines = chunk.split("\n", 1)
        subject = lines[0].strip()
        body = lines[1] if len(lines) > 1 else ""
        if subject:
            commits.append(Commit(subject=subject, body=body))
    return commits


def classify(commits: list[Commit]) -> str:
    """返回 major / minor / patch / none。

    优先级：breaking > feat > fix/perf/refactor。
    """
    has_minor = False
    has_patch = False
    for c in commits:
        if c.is_breaking:
            return "major"
        ctype = c.ctype
        if ctype in MINOR_TYPES:
            has_minor = True
        elif ctype in PATCH_TYPES:
            has_patch = True
    if has_minor:
        return "minor"
    if has_patch:
        return "patch"
    return "none"


def bump_version(latest: str, bump: str) -> str:
    m = STABLE_TAG_RE.match(latest)
    if not m:
        raise SystemExit(
            f"error: latest tag {latest!r} is not a stable semver tag (expected vX.Y.Z)"
        )
    major, minor, patch = int(m.group("major")), int(m.group("minor")), int(m.group("patch"))
    if bump == "major":
        major, minor, patch = major + 1, 0, 0
    elif bump == "minor":
        minor, patch = minor + 1, 0
    elif bump == "patch":
        patch += 1
    else:
        raise ValueError(f"unknown bump type: {bump}")
    return f"v{major}.{minor}.{patch}"


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True
    ).stdout


def latest_stable_tag() -> str | None:
    """取祖先链上最大的稳定三段版本 tag；无则 None。"""
    tags = git("tag", "--list", "v*", "--merged", "HEAD").split()
    stable = [t for t in tags if STABLE_TAG_RE.match(t)]
    if not stable:
        return None

    def key(tag: str) -> tuple[int, int, int]:
        m = STABLE_TAG_RE.match(tag)
        assert m is not None
        return (int(m.group("major")), int(m.group("minor")), int(m.group("patch")))

    return max(stable, key=key)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--latest", help="上一个稳定 tag，如 v1.2.3；省略则自动探测")
    ap.add_argument("--commits", help="从文件读取 commit 原文（NUL 分隔），用于测试")
    ap.add_argument("--range", help="git 范围，如 v1.2.3..HEAD")
    args = ap.parse_args()

    detected = latest_stable_tag()
    # 首次发版（仓库还没有任何稳定 tag）以 v0.0.0 为基线，
    # 但 git log 仍读取完整历史以便分类出 patch/minor/major。
    latest = args.latest or detected or "v0.0.0"
    base_for_range = detected or ""

    if args.commits:
        raw = open(args.commits, encoding="utf-8").read()
    else:
        log_range = args.range or (f"{base_for_range}..HEAD" if base_for_range else "HEAD")
        # %x00 分隔 commit，subject 与 body 之间用换行保持可解析
        raw = git("log", log_range, "--pretty=format:%s%n%b%x00")

    commits = parse_commits(raw)
    bump = classify(commits)

    print(f"bump={bump}")
    if bump == "none":
        print("version=")
        return 0

    print(f"version={bump_version(latest, bump)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
