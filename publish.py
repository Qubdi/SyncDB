#!/usr/bin/env python3
"""Bump version, build, and publish Qubdi-SyncDB to PyPI.

Usage
-----
  python publish.py patch          # 0.1.0 -> 0.1.1
  python publish.py minor          # 0.1.0 -> 0.2.0
  python publish.py major          # 0.1.0 -> 1.0.0
  python publish.py 1.2.3          # set exact version
  python publish.py patch --dry-run  # build but do not upload
  python publish.py patch --no-tag   # skip git tag
  python publish.py patch --yes --push  # non-interactive (automation)

The quality gate (component tests + ruff + mypy) ALWAYS runs before a release.
There is deliberately no flag to skip it; the only escape hatch is the
environment variable SYNCDB_PUBLISH_SKIP_GATE=1, which prints a loud warning
so it cannot be used silently.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

PYPROJECT = Path(__file__).parent / "pyproject.toml"
VERSION_RE = re.compile(r'^version\s*=\s*"([^"]+)"', re.MULTILINE)


# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

def read_version() -> str:
    text = PYPROJECT.read_text(encoding="utf-8")
    match = VERSION_RE.search(text)
    if not match:
        sys.exit("Could not find version = \"...\" in pyproject.toml")
    return match.group(1)


def bump_version(current: str, part: str) -> str:
    try:
        major, minor, patch = map(int, current.split("."))
    except ValueError:
        sys.exit(f"Current version '{current}' is not in MAJOR.MINOR.PATCH format")
    if part == "major":
        return f"{major + 1}.0.0"
    if part == "minor":
        return f"{major}.{minor + 1}.0"
    if part == "patch":
        return f"{major}.{minor}.{patch + 1}"
    # Explicit version string — validate format
    if not re.fullmatch(r"\d+\.\d+\.\d+", part):
        sys.exit(f"'{part}' is not a valid version (use MAJOR.MINOR.PATCH or patch/minor/major)")
    return part


def write_version(new_version: str) -> None:
    text = PYPROJECT.read_text(encoding="utf-8")
    updated = VERSION_RE.sub(f'version = "{new_version}"', text, count=1)
    PYPROJECT.write_text(updated, encoding="utf-8")


# ---------------------------------------------------------------------------
# Shell helpers
# ---------------------------------------------------------------------------

def run(cmd: list[str], *, check: bool = True) -> int:
    print(f"\n$ {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if check and result.returncode != 0:
        sys.exit(f"Command failed with exit code {result.returncode}")
    return result.returncode


def require_tool(name: str) -> None:
    if shutil.which(name) is None:
        sys.exit(f"'{name}' not found. Install it with: pip install {name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("version", help="patch | minor | major | x.y.z")
    parser.add_argument("--dry-run", action="store_true", help="Build but do not upload to PyPI")
    parser.add_argument("--no-tag", action="store_true", help="Skip creating a git tag")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompts (automation)")
    parser.add_argument("--push", action="store_true", help="Push the release tag without prompting")
    args = parser.parse_args()

    require_tool("twine")

    current = read_version()
    new = bump_version(current, args.version)

    print(f"\nVersion: {current}  ->  {new}")
    if not args.dry_run and not args.yes:
        confirm = input("Continue? [y/N] ").strip().lower()
        if confirm != "y":
            sys.exit("Aborted.")

    # 1. Quality gate: tests + lint + types.  Always runs — a release that
    # skips its own gate is how regressions ship.  The env-var escape hatch
    # exists for genuine emergencies only and announces itself loudly.
    if os.environ.get("SYNCDB_PUBLISH_SKIP_GATE") == "1":
        print(
            "\n" + "!" * 70
            + "\n!!  SYNCDB_PUBLISH_SKIP_GATE=1 — RELEASING WITHOUT RUNNING THE"
            + "\n!!  TEST/LINT/TYPE GATE.  Do not do this for a normal release."
            + "\n" + "!" * 70
        )
    else:
        print("\n--- Quality gate: component tests ---")
        run([sys.executable, "-m", "pytest", "Tests/Library/Components", "-q"])
        print("\n--- Quality gate: ruff ---")
        run([sys.executable, "-m", "ruff", "check", "Library/"])
        print("\n--- Quality gate: mypy ---")
        run([sys.executable, "-m", "mypy", "Library/"])

    # 2. Bump version in pyproject.toml
    write_version(new)
    print(f"\nUpdated pyproject.toml: version = \"{new}\"")

    # 3. Clean previous dist/
    dist_dir = Path("dist")
    if dist_dir.exists():
        shutil.rmtree(dist_dir)
        print("Removed old dist/")

    # 4. Build
    print("\n--- Building ---")
    run([sys.executable, "-m", "build"])

    # 5. Verify package.  Paths are stringified explicitly: Path objects in the
    # argv list crash the command logging on Windows ("dist/*" globbing is also
    # shell-dependent, so expand it here for all platforms).
    artifacts = sorted(str(p) for p in dist_dir.glob("*"))
    if not artifacts:
        sys.exit("Build produced no artifacts in dist/")
    print("\n--- Checking package ---")
    run(["twine", "check", *artifacts])

    if args.dry_run:
        print("\nDry run complete. Artifacts in dist/. Version NOT uploaded.")
        return

    # 6. Upload
    print("\n--- Uploading to PyPI ---")
    run(["twine", "upload", *artifacts])

    # 7. Git tag
    if not args.no_tag:
        tag = f"v{new}"
        run(["git", "add", "pyproject.toml"], check=False)
        run(["git", "commit", "-m", f"release {tag}"], check=False)
        run(["git", "tag", tag], check=False)
        print(f"\nTagged: {tag}")
        push = args.push
        if not push and not args.yes:
            push = input("Push tag to remote? [y/N] ").strip().lower() == "y"
        if push:
            run(["git", "push", "--follow-tags"], check=False)

    print(f"\nPublished Qubdi-SyncDB {new} to PyPI.")


if __name__ == "__main__":
    main()
