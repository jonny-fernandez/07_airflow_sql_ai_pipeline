"""Verify CP3 runner imports when launched as a standalone file from another cwd."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_cp3_script_imports_warehouse_helpers_from_any_cwd(tmp_path):
    repo = Path(__file__).resolve().parents[1]
    script = repo / "scripts" / "project07_cp3_evidence.py"
    env = os.environ.copy()
    # Explicitly remove any PYTHONPATH fallback; exercise the script bootstrap.
    env.pop("PYTHONPATH", None)
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert "--offline-only" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr
