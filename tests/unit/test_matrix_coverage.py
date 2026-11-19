"""Keep the compatibility matrix and the differential tests in step (matrix §3.3)."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MATRIX = ROOT / "docs" / "compatibility-matrix.md"
TESTS = [ROOT / "tests" / "compat" / "test_matrix_rows.py", ROOT / "tests" / "compat" / "test_functions.py"]

_ROW = re.compile(r"^\| ([A-Z]+-\d+)(?: †)? \|(.*)$", re.M)
# test_FP_04_05_09_x covers FP-04, FP-05, FP-09; test_CAST_01_to_05_x covers CAST-01 … CAST-05.
_TEST = re.compile(r"^def test_([A-Z]+)((?:_\d+)+)(?:_to_(\d+))?_", re.M)


def matrix_rows() -> dict[str, str]:
    return {m.group(1): m.group(2) for m in _ROW.finditer(MATRIX.read_text())}


def tested_ids() -> set[str]:
    ids: set[str] = set()
    for m in _TEST.finditer("\n".join(t.read_text() for t in TESTS)):
        family, nums, upto = m.group(1), [int(n) for n in m.group(2).strip("_").split("_")], m.group(3)
        if upto:
            nums = list(range(nums[-1], int(upto) + 1)) + nums[:-1]
        ids.update(f"{family}-{n:02d}" for n in nums)
    return ids


def test_every_tested_id_is_a_matrix_row():
    unknown = tested_ids() - set(matrix_rows())
    assert not unknown, f"tests name rows that are not in the matrix: {sorted(unknown)}"


def test_rows_marked_diff_have_tests():
    rows = matrix_rows()
    claimed = {rid for rid, text in rows.items() if "/ diff" in text}
    missing = claimed - tested_ids()
    assert not missing, f"rows claim 'diff' evidence but have no test: {sorted(missing)}"
