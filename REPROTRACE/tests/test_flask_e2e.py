"""HTTP-level tests. Run with `pytest`; Flask is installed from requirements.txt."""
import io
import json
import pandas as pd
import pytest

import app as reprotrace


def test_health_endpoint():
    response = reprotrace.app.test_client().get("/health")
    assert response.status_code == 200
    assert response.get_json()["status"] == "ok"


def test_upload_compare_and_download_reports(tmp_path, monkeypatch):
    monkeypatch.setattr(reprotrace, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(reprotrace, "RUN_DIR", tmp_path / "runs")
    monkeypatch.setattr(reprotrace, "REPORT_DIR", tmp_path / "reports")
    for directory in (reprotrace.UPLOAD_DIR, reprotrace.RUN_DIR, reprotrace.REPORT_DIR):
        directory.mkdir(parents=True)
    client = reprotrace.app.test_client()
    for filename, csv_text in [
        ("base.csv", "record_id,value\nA,1\nB,2\n"),
        ("candidate.csv", "record_id,value\nB,2\nA,1\n"),
    ]:
        response = client.post("/upload", data={"files": (io.BytesIO(csv_text.encode()), filename)},
                               content_type="multipart/form-data", follow_redirects=True)
        assert response.status_code == 200
    runs = reprotrace.get_runs()
    assert len(runs) == 2
    response = client.post("/compare", data={"run_ids": [runs[0]["run_id"], runs[1]["run_id"]],
                                              "primary_key": "record_id", "atol": "0", "rtol": "0"})
    assert response.status_code == 200
    assert b"Download complete JSON report" in response.data
    reports = list(reprotrace.REPORT_DIR.glob("*.json"))
    assert len(reports) == 1
    report_id = reports[0].stem
    assert client.get(f"/comparison-report/{report_id}").status_code == 200
    assert client.get(f"/comparison-report/{report_id}/csv").status_code == 200


def test_run_detail_and_manifest_download(tmp_path, monkeypatch):
    monkeypatch.setattr(reprotrace, "RUN_DIR", tmp_path)
    run_id = "test123"
    manifest = {"run_id": run_id, "file_name": "sample.csv", "created_at": "2026-01-01T00:00:00Z",
                "file_size_bytes": 1, "git": {"available": False}, "stages": []}
    (tmp_path / f"{run_id}.json").write_text(json.dumps(manifest))
    client = reprotrace.app.test_client()
    assert client.get(f"/run/{run_id}").status_code == 200
    assert client.get(f"/report/{run_id}").status_code == 200
