from __future__ import annotations
import hashlib, json, os, uuid, subprocess, tempfile, csv, io
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, session

PROJECT_DIR = Path(__file__).resolve().parent
BASE = Path(os.environ.get("REPROTRACE_DATA_DIR", PROJECT_DIR))
UPLOAD_DIR = BASE / "uploads"
RUN_DIR = BASE / "runs"
REPORT_DIR = BASE / "reports"
for directory in (UPLOAD_DIR, RUN_DIR, REPORT_DIR):
    directory.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "replace-this-in-production")
ALLOWED = {".csv", ".xlsx", ".xls", ".json", ".parquet"}
STAGES = [
    ("1. Ingestion", "Load the uploaded file and capture its initial structure."),
    ("2. Validation", "Check dimensions, duplicate rows, missing values, and column types."),
    ("3. Cleaning", "Normalize column names and remove exact duplicate records."),
    ("4. Transformation", "Convert numeric columns and create a reproducible transformed dataset."),
    ("5. Analysis", "Calculate descriptive statistics and final quality metrics."),
]

def fingerprint(df):
    payload = pd.util.hash_pandas_object(df, index=True).values.tobytes()
    return hashlib.sha256(payload).hexdigest()

def load_frame(path):
    suffix = path.suffix.lower()
    if suffix == ".csv": return pd.read_csv(path)
    if suffix in {".xlsx", ".xls"}: return pd.read_excel(path)
    if suffix == ".json": return pd.read_json(path)
    if suffix == ".parquet": return pd.read_parquet(path)
    raise ValueError("Unsupported file format")

def json_safe(value):
    if isinstance(value, (np.integer,)): return int(value)
    if isinstance(value, (np.floating,)): return None if not np.isfinite(value) else float(value)
    if isinstance(value, (np.bool_,)): return bool(value)
    if pd.isna(value): return None
    return str(value)

def git_provenance():
    """Capture repository revision and working-tree state without requiring Git."""
    try:
        commit = subprocess.check_output(["git", "-C", str(PROJECT_DIR), "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
        dirty = bool(subprocess.check_output(["git", "-C", str(PROJECT_DIR), "status", "--porcelain"], text=True).strip())
        diff = subprocess.check_output(["git", "-C", str(PROJECT_DIR), "diff", "HEAD"], stderr=subprocess.DEVNULL)
        return {"available": True, "commit": commit, "working_tree_dirty": dirty,
                "working_tree_diff_sha256": hashlib.sha256(diff).hexdigest()}
    except (subprocess.CalledProcessError, FileNotFoundError):
        return {"available": False, "reason": "Project is not a Git repository or Git is unavailable."}


def load_pipeline_config():
    config_path = PROJECT_DIR / "pipeline_config.json"
    defaults = {"normalize_column_names": True, "drop_exact_duplicates": True, "coerce_numeric_columns": True}
    if config_path.exists():
        supplied = json.loads(config_path.read_text())
        defaults.update({k: bool(v) for k, v in supplied.items() if k in defaults})
    return defaults


def run_pipeline(path, run_id):
    config = load_pipeline_config()
    raw = load_frame(path)
    artifacts, metrics = [], []
    current = raw.copy()

    def record(name, frame, details):
        artifacts.append({
            "stage": name, "rows": int(frame.shape[0]), "columns": int(frame.shape[1]),
            "schema": [{"name": str(c), "dtype": str(frame[c].dtype)} for c in frame.columns],
            "fingerprint": fingerprint(frame), "details": details,
            "preview": frame.head(8).replace({np.nan: None}).to_dict(orient="records"),
            # Full stage snapshot enables record/cell-level comparisons.
            "data": frame.replace({np.nan: None}).to_dict(orient="records")
        })

    record(STAGES[0][0], current, {"source_file": path.name, "loaded": True})
    validation = {
        "duplicate_rows": int(current.duplicated().sum()),
        "missing_cells": int(current.isna().sum().sum()),
        "missing_by_column": {str(k): int(v) for k, v in current.isna().sum().items()},
        "numeric_columns": [str(c) for c in current.select_dtypes(include=np.number).columns],
        "non_numeric_columns": [str(c) for c in current.select_dtypes(exclude=np.number).columns],
    }
    record(STAGES[1][0], current, validation)
    if config["normalize_column_names"]:
        current.columns = [str(c).strip().lower().replace(" ", "_") for c in current.columns]
    before = len(current)
    if config["drop_exact_duplicates"]:
        current = current.drop_duplicates().reset_index(drop=True)
    record(STAGES[2][0], current, {"removed_duplicate_rows": int(before-len(current)),
                                   "normalized_column_names": config["normalize_column_names"],
                                   "drop_exact_duplicates": config["drop_exact_duplicates"]})
    numeric = current.select_dtypes(include=np.number).columns.tolist()
    if config["coerce_numeric_columns"]:
        for col in numeric:
            current[col] = pd.to_numeric(current[col], errors="coerce")
    record(STAGES[3][0], current, {"numeric_columns_transformed": [str(c) for c in numeric],
                                   "transformation": "numeric coercion; no scaling" if config["coerce_numeric_columns"] else "disabled by pipeline config"})
    summary = {}
    for col in current.select_dtypes(include=np.number).columns:
        s = current[col]
        summary[str(col)] = {"count": int(s.count()), "mean": json_safe(s.mean()),
                             "std": json_safe(s.std()), "min": json_safe(s.min()),
                             "max": json_safe(s.max())}
    analysis = {"numeric_summary": summary, "row_count": int(len(current)),
                "column_count": int(len(current.columns)),
                "missing_cells": int(current.isna().sum().sum()),
                "duplicate_rows": int(current.duplicated().sum())}
    record(STAGES[4][0], current, analysis)
    manifest = {
        "run_id": run_id, "created_at": datetime.now(timezone.utc).isoformat(),
        "file_name": path.name, "file_size_bytes": path.stat().st_size,
        "pipeline": "REPROTRACE five-stage demo pipeline",
        "tolerances": {"atol": 1e-8, "rtol": 1e-5}, "git": git_provenance(),
        "python_version": __import__("platform").python_version(), "pandas_version": pd.__version__,
        "numpy_version": np.__version__, "pipeline_config": config, "stages": artifacts
    }
    (RUN_DIR / f"{run_id}.json").write_text(json.dumps(manifest, indent=2, default=json_safe))
    return manifest

def get_runs():
    runs=[]
    for p in sorted(RUN_DIR.glob("*.json"), key=lambda x:x.stat().st_mtime, reverse=True):
        try: runs.append(json.loads(p.read_text()))
        except Exception: continue
    return runs

def _is_missing(value):
    return value is None or (isinstance(value, float) and np.isnan(value))

def _cell_equal(left, right, atol, rtol):
    if _is_missing(left) and _is_missing(right):
        return True, "same"
    if _is_missing(left) or _is_missing(right):
        return False, "missing-value change"
    # Numeric values are tolerance-aware at every stage, not only in summaries.
    if isinstance(left, (int, float, np.integer, np.floating)) and not isinstance(left, bool) and \
       isinstance(right, (int, float, np.integer, np.floating)) and not isinstance(right, bool):
        equal = bool(np.isclose(float(left), float(right), atol=atol, rtol=rtol, equal_nan=True))
        return equal, "numeric"
    equal = str(left) == str(right)
    return equal, "value"

def compare_stage(a, b, atol, rtol, primary_key=None):
    changes=[]
    if a["columns"] != b["columns"]: changes.append(f"Column count: {a['columns']} → {b['columns']}")
    left_cols=[x["name"] for x in a.get("schema", [])]
    right_cols=[x["name"] for x in b.get("schema", [])]
    if left_cols != right_cols: changes.append("Column names or order differ.")
    left_types={x["name"]:x["dtype"] for x in a.get("schema", [])}
    right_types={x["name"]:x["dtype"] for x in b.get("schema", [])}
    type_changes=[c for c in set(left_types)&set(right_types) if left_types[c] != right_types[c]]
    if type_changes: changes.append("Data type changes: " + ", ".join(sorted(type_changes)))
    left_rows=a.get("data", a.get("preview", [])); right_rows=b.get("data", b.get("preview", []))
    common_cols=[c for c in left_cols if c in right_cols]
    row_diffs=[]; changed_cells=0; numeric_outside=0; within_tolerance=0
    if primary_key:
        def resolve_key(requested, columns):
            if requested in columns: return requested
            normalized=requested.strip().lower().replace(" ", "_")
            matches=[column for column in columns if str(column).strip().lower().replace(" ", "_")==normalized]
            return matches[0] if len(matches)==1 else None
        left_key=resolve_key(primary_key,left_cols); right_key=resolve_key(primary_key,right_cols)
        if not left_key or not right_key:
            raise ValueError(f"Primary key '{primary_key}' must exist in both stage schemas (including normalized form).")
        def keyed(rows, side, key_column):
            result={}
            for row in rows:
                key=row.get(key_column)
                if key is None or str(key)=="": raise ValueError(f"Primary key '{primary_key}' contains missing values in {side} data.")
                token=str(key)
                if token in result: raise ValueError(f"Primary key '{primary_key}' is not unique in {side} data.")
                result[token]=row
            return result
        lmap=keyed(left_rows,"baseline",left_key); rmap=keyed(right_rows,"candidate",right_key)
        left_keys=set(lmap); right_keys=set(rmap)
        removed_keys=sorted(left_keys-right_keys); added_keys=sorted(right_keys-left_keys)
        for key in removed_keys: row_diffs.append({"row":key,"column":"(row)","baseline":"present","candidate":"<missing>","kind":"removed row"})
        for key in added_keys: row_diffs.append({"row":key,"column":"(row)","baseline":"<missing>","candidate":"present","kind":"added row"})
        pairs=[(key,lmap[key],rmap[key]) for key in sorted(left_keys & right_keys)]
        added_rows=len(added_keys); removed_rows=len(removed_keys)
    else:
        max_rows=max(len(left_rows),len(right_rows)); pairs=[]
        added_rows=max(0,len(right_rows)-len(left_rows)); removed_rows=max(0,len(left_rows)-len(right_rows))
        for idx in range(max_rows):
            lrow=left_rows[idx] if idx<len(left_rows) else None; rrow=right_rows[idx] if idx<len(right_rows) else None
            if lrow is None or rrow is None:
                row_diffs.append({"row":idx+1,"column":"(row)","baseline":"<missing>" if lrow is None else "present","candidate":"present" if rrow is not None else "<missing>","kind":"added/removed row"})
            else: pairs.append((idx+1,lrow,rrow))
    for row_id,lrow,rrow in pairs:
        for col in common_cols:
            lv=lrow.get(col); rv=rrow.get(col); equal,kind=_cell_equal(lv,rv,atol,rtol)
            if not equal:
                changed_cells += 1
                if kind=="numeric": numeric_outside += 1
                row_diffs.append({"row":row_id,"column":col,"baseline":lv,"candidate":rv,"kind":kind})
            elif kind=="numeric" and lv is not None and rv is not None and float(lv)!=float(rv): within_tolerance += 1
    missing_left=sorted(set(left_cols)-set(right_cols)); missing_right=sorted(set(right_cols)-set(left_cols))
    if missing_left: changes.append("Columns removed: "+", ".join(missing_left))
    if missing_right: changes.append("Columns added: "+", ".join(missing_right))
    if added_rows: changes.append(f"Added rows: {added_rows}")
    if removed_rows: changes.append(f"Removed rows: {removed_rows}")
    if changed_cells: changes.append(f"Changed cells outside tolerance: {changed_cells}")
    numeric_note=(f"Compared numeric cells across this stage using atol={atol:g}, rtol={rtol:g}. {numeric_outside} numeric cell(s) outside tolerance; {within_tolerance} numeric cell(s) varied but remained within tolerance.")
    da,db=a.get("details",{}),b.get("details",{})
    metadata_note="Stage metadata differs; inspect each run manifest for context." if da!=db else "Stage metadata matches."
    if changes or changed_cells or added_rows or removed_rows: status="Different"
    else: status="Identical" if (primary_key or a.get("fingerprint")==b.get("fingerprint")) else "Within tolerance"
    total=len(row_diffs)
    return {"status":status,"changes":changes,"numeric_note":numeric_note,"metadata_note":metadata_note,
            "row_diff_count":total,"row_diffs":row_diffs,"row_diff_truncated":total>100,
            "added_rows":added_rows,"removed_rows":removed_rows,"changed_cells":changed_cells,
            "primary_key":primary_key}


def build_comparison(selected, atol, rtol, primary_key=None):
    baseline=selected[0]; comparisons=[]
    for other in selected[1:]:
        stages=[]; earliest=None
        for left,right in zip(baseline["stages"],other["stages"]):
            result=compare_stage(left,right,atol,rtol,primary_key)
            stages.append({"name":left["stage"],"left":left,"right":right,**result})
            if earliest is None and result["status"]=="Different": earliest=left["stage"]
        comparisons.append({"baseline":baseline,"other":other,"stages":stages,
                            "earliest":earliest or "No meaningful divergence detected"})
    return comparisons


def run_git_controlled(run_id, git_ref):
    """Execute the selected revision in a detached worktree against immutable input bytes."""
    source_manifest=RUN_DIR/f"{run_id}.json"
    if not source_manifest.exists(): raise ValueError("Source run not found.")
    manifest=json.loads(source_manifest.read_text())
    source_file=UPLOAD_DIR/f"{run_id}_{Path(manifest['file_name']).name}"
    if not source_file.exists(): raise ValueError("Original uploaded input is unavailable; controlled rerun cannot proceed.")
    try:
        commit=subprocess.check_output(["git","-C",str(PROJECT_DIR),"rev-parse","--verify",f"{git_ref}^{{commit}}"],text=True,stderr=subprocess.PIPE).strip()
    except (subprocess.CalledProcessError, FileNotFoundError): raise ValueError("Git ref is invalid or this project is not a Git repository.")
    rerun_id=uuid.uuid4().hex[:12]
    with tempfile.TemporaryDirectory(prefix="reprotrace_git_") as temp:
        worktree=Path(temp)/"code"; data=Path(temp)/"data"
        subprocess.run(["git","-C",str(PROJECT_DIR),"worktree","add","--detach",str(worktree),commit],check=True,capture_output=True,text=True)
        try:
            env=os.environ.copy(); env["REPROTRACE_DATA_DIR"]=str(data)
            env["PYTHONPATH"]=str(worktree)+os.pathsep+env.get("PYTHONPATH","")
            code="from pathlib import Path; import app; app.run_pipeline(Path(__import__('sys').argv[1]), __import__('sys').argv[2])"
            subprocess.run([__import__("sys").executable,"-c",code,str(source_file.resolve()),rerun_id],cwd=worktree,env=env,check=True,capture_output=True,text=True)
            produced=data/"runs"/f"{rerun_id}.json"
            rerun=json.loads(produced.read_text()); rerun["file_name"]=manifest["file_name"]; rerun["rerun"]={"source_run_id":run_id,"git_commit":commit,"requested_ref":git_ref}
            (RUN_DIR/f"{rerun_id}.json").write_text(json.dumps(rerun,indent=2,default=json_safe))
            return rerun
        finally:
            subprocess.run(["git","-C",str(PROJECT_DIR),"worktree","remove","--force",str(worktree)],capture_output=True,text=True)

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html", runs=get_runs(), stages=STAGES)

@app.route("/upload", methods=["POST"])
def upload():
    uploaded=request.files.getlist("files")
    if not uploaded or not any(f.filename for f in uploaded):
        flash("Choose at least one file to upload.", "error"); return redirect(url_for("index"))
    successes=0
    for f in uploaded:
        name=Path(f.filename).name
        suffix=Path(name).suffix.lower()
        if suffix not in ALLOWED:
            flash(f"Skipped {name}: unsupported format.", "error"); continue
        run_id=uuid.uuid4().hex[:12]
        dest=UPLOAD_DIR / f"{run_id}_{name}"
        f.save(dest)
        try:
            run_pipeline(dest, run_id); successes+=1
        except Exception as e:
            dest.unlink(missing_ok=True)
            flash(f"Could not process {name}: {e}", "error")
    if successes: flash(f"Successfully processed {successes} file(s) through all five stages.", "success")
    return redirect(url_for("index"))

@app.route("/compare", methods=["POST"])
def compare():
    ids=request.form.getlist("run_ids")
    try:
        atol=max(0.0,float(request.form.get("atol","1e-8"))); rtol=max(0.0,float(request.form.get("rtol","1e-5")))
    except ValueError:
        flash("Tolerance values must be numbers.","error"); return redirect(url_for("index"))
    if len(ids)<2: flash("Select at least two runs to compare.","error"); return redirect(url_for("index"))
    all_runs={r["run_id"]:r for r in get_runs()}; selected=[all_runs[i] for i in ids if i in all_runs]
    if len(selected)<2: flash("One or more selected runs could not be found.","error"); return redirect(url_for("index"))
    primary_key=request.form.get("primary_key","").strip() or None
    try: comparisons=build_comparison(selected,atol,rtol,primary_key)
    except ValueError as exc: flash(str(exc),"error"); return redirect(url_for("index"))
    report_id=uuid.uuid4().hex[:12]
    report_data={"report_id":report_id,"created_at":datetime.now(timezone.utc).isoformat(),"baseline_run_id":selected[0]["run_id"],
                 "selected_run_ids":ids,"tolerances":{"atol":atol,"rtol":rtol},"primary_key":primary_key,
                 "comparisons":comparisons,"summary":{"pairs":len(comparisons),"stage_comparisons":sum(len(c["stages"]) for c in comparisons),
                 "earliest_divergences":{c["other"]["run_id"]:c["earliest"] for c in comparisons}}}
    (REPORT_DIR/f"{report_id}.json").write_text(json.dumps(report_data,indent=2,default=json_safe))
    return render_template("comparison.html",comparisons=comparisons,selected=selected,atol=atol,rtol=rtol,
                           primary_key=primary_key,report_id=report_id)


@app.route("/rerun/<run_id>", methods=["POST"])
def rerun(run_id):
    git_ref=request.form.get("git_ref","HEAD").strip() or "HEAD"
    try:
        result=run_git_controlled(run_id,git_ref)
        flash(f"Controlled rerun created: {result['run_id']} from commit {result['git']['commit'][:12]}.","success")
    except (ValueError, subprocess.CalledProcessError) as exc:
        flash(f"Controlled rerun failed: {exc}","error")
    return redirect(url_for("run_detail",run_id=run_id))


@app.route("/comparison-report/<report_id>")
def comparison_report(report_id):
    p=REPORT_DIR/f"{report_id}.json"
    if not p.exists(): return "Comparison report not found",404
    return send_file(p,as_attachment=True,download_name=f"reprotrace_comparison_{report_id}.json")


@app.route("/comparison-report/<report_id>/csv")
def comparison_report_csv(report_id):
    p=REPORT_DIR/f"{report_id}.json"
    if not p.exists(): return "Comparison report not found",404
    report=json.loads(p.read_text()); output=io.StringIO(); writer=csv.writer(output)
    writer.writerow(["baseline_run","candidate_run","stage","status","earliest_divergence","row_key_or_number","column","baseline_value","candidate_value","change_kind"])
    for comp in report["comparisons"]:
        for stage in comp["stages"]:
            diffs=stage["row_diffs"] or [{"row":"","column":"","baseline":"","candidate":"","kind":""}]
            for diff in diffs:
                writer.writerow([comp["baseline"]["run_id"],comp["other"]["run_id"],stage["name"],stage["status"],comp["earliest"],diff["row"],diff["column"],diff["baseline"],diff["candidate"],diff["kind"]])
    return app.response_class(output.getvalue(),mimetype="text/csv",headers={"Content-Disposition":f"attachment; filename=reprotrace_comparison_{report_id}.csv"})

@app.route("/run/<run_id>")
def run_detail(run_id):
    p=RUN_DIR/f"{run_id}.json"
    if not p.exists(): flash("Run not found.","error"); return redirect(url_for("index"))
    return render_template("run.html", run=json.loads(p.read_text()), stages=STAGES)

@app.route("/report/<run_id>")
def report(run_id):
    p=RUN_DIR/f"{run_id}.json"
    if not p.exists(): return "Run not found",404
    return send_file(p, as_attachment=True, download_name=f"reprotrace_{run_id}_manifest.json")

@app.route("/health")
def health(): return {"status":"ok","service":"REPROTRACE Flask"}

if __name__ == "__main__":
    app.run(debug=True)
