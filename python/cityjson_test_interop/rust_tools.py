from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Final

REPO_ROOT: Final = Path(__file__).resolve().parents[2]
RUST_TOOLS_MANIFEST: Final = REPO_ROOT / "rust-tools" / "Cargo.toml"


def run_rust_tool(args: list[str]) -> str:
    result = subprocess.run(
        [
            "cargo",
            "run",
            "--quiet",
            "--manifest-path",
            str(RUST_TOOLS_MANIFEST),
            "--",
            *args,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def read_arrow_json(path: Path) -> dict[str, object]:
    return json.loads(run_rust_tool(["read-arrow-json", str(path)]))


def read_dataset_json(path: Path) -> dict[str, object]:
    return json.loads(run_rust_tool(["read-dataset-json", str(path)]))


def write_arrow_fixture(path: Path) -> None:
    run_rust_tool(["write-arrow-fixture", str(path)])


def write_dataset_fixture(path: Path) -> None:
    run_rust_tool(["write-dataset-fixture", str(path)])
