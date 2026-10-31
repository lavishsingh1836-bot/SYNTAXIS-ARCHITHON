import pandas as pd
from app import compare_stage, fingerprint

def stage(df):
    return {
        "fingerprint": fingerprint(df), "rows": len(df), "columns": len(df.columns),
        "schema": [{"name": str(c), "dtype": str(df[c].dtype)} for c in df.columns],
        "details": {}, "data": df.where(pd.notna(df), None).to_dict(orient="records")
    }

def test_fingerprint_is_deterministic():
    df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
    assert fingerprint(df) == fingerprint(df.copy())

def test_identical_stage():
    df = pd.DataFrame({"x": [1, 2]})
    assert compare_stage(stage(df), stage(df.copy()), 1e-8, 1e-5)["status"] == "Identical"

def test_row_level_non_numeric_change_is_reported():
    a = stage(pd.DataFrame({"id": [1, 2], "label": ["a", "b"]}))
    b = stage(pd.DataFrame({"id": [1, 2], "label": ["a", "changed"]}))
    result = compare_stage(a, b, 1e-8, 1e-5)
    assert result["status"] == "Different"
    assert result["row_diffs"][0]["column"] == "label"
    assert result["row_diff_count"] == 1

def test_numeric_variation_within_tolerance_is_not_divergence():
    a = stage(pd.DataFrame({"value": [100.0, 200.0]}))
    b = stage(pd.DataFrame({"value": [100.0000001, 200.0]}))
    result = compare_stage(a, b, 1e-5, 1e-5)
    assert result["status"] == "Within tolerance"
    assert result["row_diff_count"] == 0

def test_numeric_change_outside_tolerance_detected_at_stage():
    a = stage(pd.DataFrame({"value": [10.0, 20.0]}))
    b = stage(pd.DataFrame({"value": [10.0, 20.5]}))
    result = compare_stage(a, b, 1e-8, 1e-5)
    assert result["status"] == "Different"
    assert result["row_diffs"][0]["row"] == 2

def test_added_and_removed_rows_are_reported():
    a = stage(pd.DataFrame({"x": [1, 2]}))
    b = stage(pd.DataFrame({"x": [1, 2, 3]}))
    result = compare_stage(a, b, 0, 0)
    assert result["added_rows"] == 1
    assert result["status"] == "Different"

def test_primary_key_matches_reordered_rows():
    left = stage(pd.DataFrame({"record_id": ["A", "B"], "value": [10, 20]}))
    right = stage(pd.DataFrame({"record_id": ["B", "A"], "value": [20, 10]}))
    result = compare_stage(left, right, 0, 0, primary_key="record_id")
    assert result["status"] == "Identical"
    assert result["row_diff_count"] == 0


def test_primary_key_detects_update_and_insert():
    left = stage(pd.DataFrame({"record_id": ["A", "B"], "value": [10, 20]}))
    right = stage(pd.DataFrame({"record_id": ["A", "C"], "value": [11, 30]}))
    result = compare_stage(left, right, 0, 0, primary_key="record_id")
    assert result["status"] == "Different"
    assert {d["kind"] for d in result["row_diffs"]} == {"numeric", "removed row", "added row"}


def test_primary_key_must_be_unique():
    left = stage(pd.DataFrame({"record_id": ["A", "A"], "value": [10, 20]}))
    right = stage(pd.DataFrame({"record_id": ["A", "B"], "value": [10, 20]}))
    import pytest
    with pytest.raises(ValueError, match="not unique"):
        compare_stage(left, right, 0, 0, primary_key="record_id")


def test_primary_key_accepts_pipeline_normalized_column_name():
    left = stage(pd.DataFrame({"Record ID": ["A", "B"], "value": [1, 2]}))
    right = stage(pd.DataFrame({"record_id": ["B", "A"], "value": [2, 1]}))
    result = compare_stage(left, right, 0, 0, primary_key="Record ID")
    assert result["row_diff_count"] == 0
    assert "Column names or order differ." in result["changes"]
