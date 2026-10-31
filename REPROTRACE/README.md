# REPROTRACE — Flask pipeline observability and reproducibility

A Flask web application (not Streamlit) for multi-file ingestion, five-stage pipeline instrumentation, row-level comparisons, Git-controlled reruns, and downloadable investigation reports.

## Capabilities

- Upload multiple CSV, XLSX, XLS, JSON, or Parquet files in one submission.
- Run every input through **Ingestion → Validation → Cleaning → Transformation → Analysis**.
- Persist each stage's schema, dimensions, SHA-256 artifact fingerprint, metadata, preview, and full row snapshot.
- Compare two or more executions pairwise against the first selected baseline.
- Compare records by an optional **primary-key column**. Keys must exist in both compared stages, be non-null, and be unique. If no key is provided, comparison falls back to row-position alignment.
- Apply `atol` and `rtol` numeric tolerance to numeric cells at every stage. Values inside tolerance are not treated as meaningful divergence.
- Identify the earliest meaningful divergent stage and inspect row/cell-level changes.
- Download a complete JSON comparison report (all row differences, not only the UI's first 100) and a flattened CSV difference report.
- Inspect each run's manifest, environment versions, Git commit, working-tree state, and pipeline configuration.
- Rerun an immutable uploaded input using a selected Git commit, tag, or branch in a detached temporary Git worktree. The rerun uses the `pipeline_config.json` from that revision and stores a new run manifest.

## Setup

```bash
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

Initialize and commit the project before using Git-controlled reruns:

```bash
git init
git add app.py templates static tests requirements.txt pipeline_config.json README.md .gitignore
git commit -m "REPROTRACE baseline"
```

Run the application:

```bash
python app.py
```

Open http://127.0.0.1:5000

## Controlled rerun workflow

1. Initialize Git and commit a known-good version as shown above.
2. Upload one or more datasets. Original inputs are retained in `uploads/` under generated names.
3. Open a run's **Inspect** page.
4. Enter a commit hash, tag, or branch in **Git commit / tag / branch** and click **Rerun using this Git revision**.
5. REPROTRACE creates a detached temporary worktree, executes that revision against the original uploaded input, writes a new manifest, then removes the temporary worktree.

The requested revision must be available in the local Git repository. Git must be installed and accessible on the server. This mechanism executes trusted project code; do not expose it to untrusted users or repositories. For stronger reproducibility, commit all code and configuration changes and keep dependency lockfiles in version control. The prototype records the active Python/pandas/NumPy versions but does not build a separate dependency environment for each historical commit. Dirty working-tree changes are fingerprinted in provenance but are not silently injected into a historical commit rerun.

## Pipeline configuration

Edit `pipeline_config.json` and commit the change to version pipeline behavior:

```json
{
  "normalize_column_names": true,
  "drop_exact_duplicates": true,
  "coerce_numeric_columns": true
}
```

A rerun against a Git revision reads the configuration file from that exact revision. The manifest stores the effective configuration and Python/pandas/NumPy versions.

## Tests

```bash
pytest -q
```

`tests/test_pipeline.py` covers fingerprints, tolerance-aware numeric comparisons, primary-key alignment, duplicate-key validation, and row insertions/removals. `tests/test_flask_e2e.py` uses Flask's test client to exercise health, upload, comparison, report download, run detail, and manifest download routes. A Git-controlled rerun should additionally be tested in an environment with Git installed and a committed working tree.

## Operational notes

- Full stage snapshots and comparison reports can consume substantial disk space. Add retention and upload-size policies before production use.
- The Git rerun feature executes code from the selected revision. Restrict access, disable debug mode, and add authentication/authorization before deployment.
- Use a stable, unique domain key for reliable matching when record order may change. If omitted, position-based matching is used.
- JSON reports retain all differences. The browser table intentionally renders only the first 100 rows per stage for responsiveness.
- The app is a reproducibility research prototype, not a secure multi-tenant execution sandbox.
