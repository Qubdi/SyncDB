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
  python publish.py patch --no-test  # skip test run
"""

from __future__ import annotations

import argparse
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
    parser.add_argument("--no-test", action="store_true", help="Skip running the test suite")
    args = parser.parse_args()

    require_tool("twine")

    current = read_version()
    new = bump_version(current, args.version)

    print(f"\nVersion: {current}  ->  {new}")
    if not args.dry_run:
        confirm = input("Continue? [y/N] ").strip().lower()
        if confirm != "y":
            sys.exit("Aborted.")

    # 1. Run tests
    if not args.no_test:
        print("\n--- Running unit tests ---")
        run([sys.executable, "-m", "pytest",
             "Tests/Library/Components/config",
             "Tests/Library/Components/connectors",
             "Tests/Library/Components/files",
             "Tests/Library/Components/progress",
             "Tests/Library/Components/sql",
             "Tests/Library/Components/sync",
             "Tests/Library/Components/type_mapping",
             "-q"])

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

    # 5. Verify package
    print("\n--- Checking package ---")
    run(["twine", "check", "dist/*"] if sys.platform != "win32" else ["twine", "check", *dist_dir.glob("*")])

    if args.dry_run:
        print(f"\nDry run complete. Artifacts in dist/. Version NOT uploaded.")
        return

    # 6. Upload
    print("\n--- Uploading to PyPI ---")
    run(["twine", "upload", *dist_dir.glob("*")])

    # 7. Git tag
    if not args.no_tag:
        tag = f"v{new}"
        run(["git", "add", "pyproject.toml"], check=False)
        run(["git", "commit", "-m", f"release {tag}"], check=False)
        run(["git", "tag", tag], check=False)
        print(f"\nTagged: {tag}")
        push = input("Push tag to remote? [y/N] ").strip().lower()
        if push == "y":
            run(["git", "push", "--follow-tags"], check=False)

    print(f"\nPublished Qubdi-SyncDB {new} to PyPI.")


if __name__ == "__main__":
    main()
