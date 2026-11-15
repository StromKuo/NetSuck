#!/usr/bin/env python3
"""判定某个版本是否允许更新 latest / major / major.minor 这些可变别名。

规则：只有该版本是**当前远端最新稳定版本**时才允许推别名。
补推历史版本（如 v1.2.3 在 v1.2.4 之后重跑）只能推不可变精确版本，
否则会把 latest/1/1.2 指回旧镜像，等于线上回滚。

用法:
    alias_policy.py <target-tag> <all-tag-1> <all-tag-2> ...
输出:
    ok                    允许推别名
    backfill newest=vX.Y.Z  只推不可变版本，并报告当前最新
"""

from __future__ import annotations

import sys

from next_version import STABLE_TAG_RE


def parse(tag: str) -> tuple[int, int, int] | None:
    m = STABLE_TAG_RE.match(tag)
    if not m:
        return None
    return (int(m.group("major")), int(m.group("minor")), int(m.group("patch")))


def decide(target: str, all_tags: list[str]) -> str:
    cur = parse(target)
    if cur is None:
        return "error_not_stable"

    versions = [v for v in (parse(t) for t in all_tags) if v is not None]
    if not versions:
        # 远端还没有任何稳定 tag，本版本即最新
        return "ok"

    newest = max(versions)
    if cur >= newest:
        return "ok"
    return "backfill newest=v%d.%d.%d" % newest


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: alias_policy.py <target-tag> [tags...]", file=sys.stderr)
        return 2
    print(decide(argv[1], argv[2:]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
