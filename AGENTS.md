# Working on Global News Monitor

GNM is an independent CMDA student project for readers exploring where and how news-reported activity changes. Connect charts and map selections to the records and original links behind them. GDELT automatically codes news coverage. Event counts, distinct URLs, and independently verified events are different things. Reporting volume measures observed coverage, and sampled classification results describe the sample.

## Where to work

| Area | Entry points |
| --- | --- |
| Export collection and replay | `src/connectors/gdelt/`, `src/pipeline/`, `scripts/run_scheduler.py` |
| Normalization, categories, and queries | `src/domain/events/`, `src/ingestion/`, `sql/` |
| API and command line | `src/api/main.py`, `src/api/dashboard.py`, `python -m src.main --help` |
| Browser dashboard | `frontend/src/`, React, TypeScript, and Vite |
| Schema and deployment | `migrations/versions/`, `compose.yaml`, `deploy/` |

On Azure, the scheduler runs the Python collector on the Linux VM. It writes to a separate managed PostgreSQL server with PostGIS. FastAPI reads from that server under a restricted role. React runs in the visitor's browser. Caddy serves the built frontend and proxies API requests over HTTPS. See the [local setup](docs/LOCAL_DEVELOPMENT.md), [technical walkthrough](docs/TECHNICAL_WALKTHROUGH.md), [API contract](docs/API_REFERENCE.md), and [deployment runbook](docs/DEPLOYMENT.md). The README links the validation reports for each release.

## Development and data safety

- Read the repository and release history, inspect the branch, working tree, and any preview services, and preserve unrelated work. Use a dedicated development branch and make focused commits. Run a local preview on loopback with its own database and storage. Preserve preview data, backups, volumes, and stashes when stopping services.
- Use three distinct environments. A **disposable test database** can be rebuilt by integration tests. An **isolated local preview** contains persistent development data or an approved protected restore. **Production** is the live Azure database and VM. Before any write or test, confirm the host, database name, and role. Use separate disposable databases for `TEST_DATABASE_URL` and `TEST_MIGRATION_DATABASE_URL`. Never point tests, local migration commands, or preview ingestion at production.
- After local setup, run `python -m pytest -q` with the disposable PostgreSQL URLs to include integration and migration tests. From `frontend/`, use `npm ci`, `npm test`, and `npm run build` (includes TypeScript). Use `python -m src.main migrate` on a confirmed local database. Check desktop and phone layouts on a running preview with real retained data where available.
- Retain original payloads and identities. Keep replay idempotent and rollups consistent with normalized records. Use authoritative source exports and exact identity checks for backfills, with a bounded dry run before writes. Keep unknowns unknown. Match time windows, distinguish complete zeros from missing exports, and guard small baselines and changes in category definitions. Check changes against direct SQL, realistic data, and focused fixtures. Label illustrations, local benchmarks, and Azure measurements clearly.

## Release and operating boundaries

Explicit owner authorization is required for production access, deployment, merging to `main`, credential changes, or additional costs. Authorization for one release does not carry forward. Keep the existing low-cost footprint, spending controls, retention, HTTPS, hostname-verified PostgreSQL TLS, firewall restrictions, and separate database roles. The API reads. Ingestion writes application data without administration rights. Use parameterized SQL, validated inputs, and bounded queries. Keep credentials and protected artifacts out of Git, logs, commands, and screenshots.

For an approved release, record the baseline and exact image, source, and dependency identifiers. Scan the actual candidate images. Restore a protected backup in isolation and rehearse the migration, role checks, and recovery before touching live data. Preserve the previous images and configuration. Verify deployment, database integrity, and consecutive scheduled exports before declaring the release healthy. Use the [deployment runbook](docs/DEPLOYMENT.md) for the actual procedure.

## Writing and interface

Use plain labels, show the window and missing data, and link to the method where extra detail helps. Keep the README approachable and carry the chronological development story forward in the [Development Journey](docs/DEVELOPMENT_JOURNEY.md). Write in the author’s voice, introduce unfamiliar tools where they solved a real problem, and ground claims in the repository and the author’s experience. Acknowledge ChatGPT and Codex briefly and accurately. Keep formulas, API contracts, and operational procedures in their reference documents. Preserve the evidence and conclusions in earlier validation reports.
