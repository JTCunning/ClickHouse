"""Run the PromQL support matrix regression and freshness checks."""

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).parents[2]
UNIT_TEST = REPO_ROOT / "ci/jobs/scripts/docs/test_promql_support_matrix.py"
GENERATOR = REPO_ROOT / "ci/jobs/scripts/docs/generate_promql_support_matrix.py"


def run(command):
    result = subprocess.run(
        command,
        capture_output=True,
        check=False,
        cwd=REPO_ROOT,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_promql_support_matrix_generator():
    run([sys.executable, str(UNIT_TEST)])


def test_promql_support_matrix_is_current():
    run([sys.executable, str(GENERATOR), "--check"])
