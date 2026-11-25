# Run Global News Monitor locally

## Set up your database

### Requirements

- Python 3.12+
- PostgreSQL with PostGIS available on the server. The migration role must be able
  to enable the extension in the database, or an administrator can enable it first.
  On Windows, the PostgreSQL installer and Stack Builder provide both components.

Create a dedicated database, such as `global_news_monitor`, using your local
PostgreSQL tools. For integration tests, create a second database such as
`global_news_monitor_test`.

### Install dependencies

```bash
python -m venv .venv
```

Activate it with `source .venv/bin/activate` on macOS/Linux or
`.venv\Scripts\Activate.ps1` in PowerShell, then install dependencies.

```bash
python -m pip install -r requirements.txt
```

### Configure environment

Create a `.env` file at the repository root:

```env
DATABASE_URL=postgresql://username:password@localhost:5432/global_news_monitor
```

Optional:

```env
TEST_DATABASE_URL=postgresql://username:password@localhost:5432/global_news_monitor_test
INGEST_BATCH_SIZE=500
INGEST_READINESS_MAX_AGE_MINUTES=60
```

### Prepare the database

Run migrations:

```bash
python -m src.main migrate
```

## Usage

### Ingest the latest export

```bash
python -m src.main ingest
```

Typical first run behavior:

- discovers the latest export
- creates an ingestion run
- claims a checkpoint
- parses and inserts events
- marks the checkpoint as `completed`

Typical second run behavior for the same export:

- sees the checkpoint is already `completed`
- skips reprocessing
- does not insert duplicate rows

Example:

```text
checkpoint_status=completed outcome=already_completed
```

### Inspect recent normalized events

```bash
python -m src.main latest --limit 20
```

### Inspect recent ingestion runs

```bash
python -m src.main runs --limit 10
```

### Inspect aggregate stats

```bash
python -m src.main stats --hours 24
```

### Run the API locally

```bash
uvicorn src.api.main:app --reload
```

### Run continuous ingestion

```bash
python scripts/run_scheduler.py
```

Run the scheduler as a separate process from the API. It starts an ingest
immediately and targets a 15-minute interval between starts. It processes one
export at a time and continues polling after a failed or timed-out run. A
single scheduled pass is available with `python -m src.main sync`. By default
it checks the previous 24 hours and attempts up to eight missing or retryable
exports, reserving room for the newest eligible file. A PostgreSQL advisory
lock serializes the entire pass with other ingests. Failed checkpoints cool
down before retry, and slots beyond the lookback stay as visible coverage gaps.
The original `ingest` command still processes only the latest export.

## Run the dashboard locally

The React and TypeScript dashboard is in `frontend/`. It uses Vite, Leaflet
with bundled Natural Earth boundaries, and Recharts. Node.js 22.12 or later
and npm are required alongside the Python and PostGIS setup above.

After configuring the root `.env` and activating the Python virtual
environment, run these commands from the repository root:

```bash
python -m src.main migrate
python -m src.main ingest
python -m uvicorn src.api.main:app --host 127.0.0.1 --port 8000
```

In a second terminal, start Vite:

```bash
cd frontend
npm ci
npm run dev
```

Open the local URL printed by Vite, usually `http://localhost:5173`. To keep
data arriving while you use the dashboard, run
`python scripts/run_scheduler.py` in a third terminal from the repository root.

The browser uses `/api` by default. Vite forwards those requests to
`http://127.0.0.1:8000` during development. To override these settings, copy
`frontend/.env.example` to `frontend/.env.local` and edit it:

| Variable | Default | Purpose |
| --- | --- | --- |
| `VITE_API_BASE_URL` | `/api` | Public browser API base, included in the build |
| `API_PROXY_TARGET` | `http://127.0.0.1:8000` | Local Vite proxy destination |

For a static production build, configure the hosting platform to forward
`/api/*` to FastAPI. If using a separate origin directly, configure FastAPI
CORS for the specific frontend origin. Database credentials belong in the
backend environment.

```bash
npm test
npm run build
npm run preview
```

Run those commands from `frontend/`. The build includes a TypeScript check
and writes the static site to `frontend/dist/`.

## Testing

Run the full test suite:

```bash
python -m pip install pytest
python -m pytest -q -rs
```

Set `TEST_DATABASE_URL` to a disposable PostGIS-enabled database to include
PostgreSQL integration tests. The suite applies the complete Alembic chain to
that database and skips database tests when the test URL is absent. The
migration recovery test also needs `TEST_MIGRATION_DATABASE_URL` pointing to
a separate, disposable loopback database named `gnm_migration_test`. It
rolls the schema back and tests a failed upgrade before retrying.

The test suite covers:

- parsing and transformation helpers
- categorization logic
- repository behavior
- API responses
- retry classification
- advisory lock behavior
- readiness checks
- catch-up ordering, cooldown, partial-export retry, retention, and coverage

For deployment and historical test results, see the [deployment guide](DEPLOYMENT.md) and [v1.1 validation](V1.1_VALIDATION.md).
