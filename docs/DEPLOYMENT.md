# Deploy Global News Monitor

Production is hosted in Azure North Central US under Azure for Students.
See the [Azure runbook](#azure-production-deployment) for the deployed services,
rollout commands, and security checks. Hosting estimates are in USD and were
checked November 23, 2026. Taxes, domain renewals, and usage overages may add cost.

The v1.2 candidate is available for local review. Its new queries use the
existing schema and runtime grants. [Local validation](V1.2_VALIDATION.md)

## Hosting options evaluated

| Option | Estimated recurring cost | Tradeoff |
| --- | --- | --- |
| Oracle Cloud Always Free VM with Compose | $0 if an eligible VM, storage, and hostname are available | One instance runs every service. A1 capacity is region-dependent and Oracle may reclaim idle instances. Use the student domain benefit for a hostname if eligible. |
| Azure for Students VM and Flexible Server | Estimated $0 to $3.65 per 730-hour month within the listed free allowances | Deployed. The Azure bill will confirm whether the Standard static IPv4 uses the free IP meter. The eligible student offer has a time limit. |
| Vercel Hobby + Render Free API + Neon Free + GitHub Actions | $0 within limits, demo only | Render sleeps after 15 idle minutes and can take about one minute to wake. Neon Free provides 1 GB per project and 100 CU-hours per month. Public-repository GitHub Actions schedules can be delayed or dropped and disable after 60 days without repository activity. The database fits only a few days of this workload. |
| Hetzner CX23 in Europe with Compose | $6.49 compute + $0.60 IPv4, totaling $7.09 if capacity is available | 2 vCPU, 4 GB RAM, 40 GB SSD. The product page showed it as unavailable during review. Hosting in Europe also adds network latency for visitors in the U.S. |
| DigitalOcean Basic VM with Compose | $12 for 1 vCPU, 2 GiB RAM, 50 GiB SSD and 2,000 GiB transfer. Optional weekly VM backup adds $2.40, totaling $14.40. | One always-running host with OS, database and backup maintenance. The $6 1 GiB plan has less memory for PostgreSQL, ETL, and concurrent chart requests. |
| Vercel Hobby + managed Render | About $15.50 at light use, itemized below | Less server maintenance. The small managed database needs a workload check, and cron and bandwidth can raise cost. |

Azure was chosen for the student allowances, a dedicated PostgreSQL server,
and the Azure-managed hostname. The estimates for alternate providers are
available for future migration planning. Single-instance deployments can be
interrupted by maintenance or provider outages.

- [Oracle Always Free compute and storage](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm)
- [GitHub Student Developer Pack and domain offers](https://education.github.com/pack)
  and [Azure for Students terms](https://azure.microsoft.com/en-us/pricing/offers/ms-azr-0170p)
- [DigitalOcean compute price](https://www.digitalocean.com/pricing/droplets)
  and [backup price](https://docs.digitalocean.com/products/backups/details/pricing/).
  DigitalOcean is absent from the Pack listing checked for this estimate.
- [Hetzner CX23 specs and availability](https://www.hetzner.com/cloud/cost-optimized/),
  [compute price](https://docs.hetzner.com/general/infrastructure-and-availability/price-adjustment/),
  and [IPv4 price](https://docs.hetzner.com/cloud/servers/primary-ips/overview/)
- [Render free limits](https://render.com/docs/free),
  [Neon Free limits](https://neon.com/blog/neon-free-plan-1-gb-per-project),
  [Neon PostGIS](https://neon.com/docs/extensions/postgis),
  and [GitHub schedule behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule)

### Render and Vercel details

| Component | Monthly estimate |
| --- | ---: |
| Vercel Hobby static site for a personal portfolio | $0 |
| Render Starter API, 512 MB RAM | $7.00 |
| Render Postgres 0.1 CPU / 256 MB, with PostGIS | $6.00 |
| 5 GB provisioned database storage at $0.30 per GB | Up to $1.50 |
| Render Starter cron at 15-minute cadence | At least $1.00 |
| **Estimated total at the cron minimum** | **Up to $15.50** |

Cron compute is billed by active time with a monthly minimum. Render
allows one active run per cron job and stops runs after 12 hours. This
project uses a 12-minute command timeout. Free Render Postgres offers 1 GB,
expires after 30 days and has no backups. Paid Postgres persists across API
deploys. Render Free web services share a workspace limit of 750 running
hours per month. Vercel Hobby is for personal non-commercial use and
includes HTTPS, 100 GB of fast data transfer, and a 120-second external
proxy limit. The dashboard itself
allows 20 seconds for an API response. Start with measured requests and watch
usage in the provider dashboards.

- [Render pricing](https://render.com/pricing),
  [cron limits and billing](https://render.com/docs/cronjobs),
  and [PostGIS support](https://render.com/docs/postgresql-extensions)
- [Vercel Hobby](https://vercel.com/docs/plans/hobby),
  [limits](https://vercel.com/docs/limits),
  and [external rewrites](https://vercel.com/docs/routing/rewrites)

## Sizing and initial history

Local PostgreSQL 17 verification used 33 real GDELT exports. Eight sampled
exports came from two earlier days and 25 from a six-hour recent interval
including both ends. The database stored 37,454 distinct raw and normalized
events, adding 52.25 MB after migrations. Raw and normalized relations
accounted for 52.19 MB total including their indexes. At the recent sample's
rate of about 1,280 records per export and 96 exports per day, the observed
increment of about 1.4 kB per event projects to 170 MB per day or about
2.9 GB for 17 days. This is an estimate from a small sample. Provision room
for higher-volume intervals, indexes, temporary queries, retained audit
records, and vacuum overhead. Monitor total and available host disk space,
including WAL and logs.

The default 24-hour lookback can seed up to 97 source slots gradually.
Start with it, let the collector build the baseline over time, and use
the chart's coverage readout when sharing the portfolio. A one-time
`GDELT_LOOKBACK_HOURS=168` catch-up fills at most seven days within the
same per-run budget if storage allows. Observe storage and job duration
before widening it, and return to the regular lookback afterward. The
templates retain 17 days of detail to cover the default spike baseline.

| Setting | Default | Allowed range or behavior |
| --- | --- | --- |
| `GDELT_LOOKBACK_HOURS` | 24 | 0–168. Zero runs latest-only scheduling. |
| `GDELT_MAX_EXPORTS_PER_RUN` | 8 | 2–24. One slot is reserved for the newest eligible export during backlog. |
| `GDELT_RETENTION_DAYS` | 0 in local CLI, 17 in production templates | Zero disables pruning. An enabled value must exceed the lookback and be at most 90 days. |

Catch-up never runs beyond the configured archive window. Once an outage
ages out, earlier gaps stay visible. Retention prunes up to 10,000 raw records
and their normalized rows plus up to 10,000 rejects per successful scheduler
pass. Expired checkpoints preserve the distinction between retained and
expired coverage. Event uniqueness is enforced while records are retained.
Run and quality summaries remain available. Postgres
usually reuses freed space while provisioned disk size stays fixed.

## Option A — One VM

1. After choosing a provider, provision a Linux VM with Docker Engine and
   Compose v2. Configure a public DNS name pointing to its IP. Open inbound
   TCP 80 and 443 for Caddy. Limit SSH to your administration network and
   leave PostgreSQL and FastAPI ports private.
2. Clone the approved Git branch onto the VM. Create the deployment
   environment with restricted permissions. Set `SITE_ADDRESS` to the DNS
   name and paste the generated hexadecimal secret as `POSTGRES_PASSWORD`.

```bash
umask 077
cp deploy/.env.example deploy/.env
openssl rand -hex 24
editor deploy/.env
docker compose --env-file deploy/.env config --quiet
docker compose --env-file deploy/.env up -d --build
docker compose --env-file deploy/.env ps
docker compose --env-file deploy/.env logs --tail 80 api scheduler
```

Compose installs PostGIS support in the PostgreSQL image. The API applies
Alembic migrations before binding its port. The scheduler waits for API
liveness and starts immediately. Caddy requests a public HTTPS certificate
and forwards `/api/*` on the same origin. PostgreSQL and certificates use
named volumes; keep those volumes across redeploys. For a local Compose
smoke test, set `SITE_ADDRESS=:80` and `BIND_ADDRESS=127.0.0.1`.

For a portable logical backup, keep the resulting file on private storage,
preferably off-host. Test restores to a separate database.

```bash
umask 077
docker compose --env-file deploy/.env exec -T postgres pg_dump -U gnm -Fc gnm > gnm.dump
```

## Option B — Managed Vercel and Render

1. Confirm the monthly estimate and grant GitHub access to the intended
   repository. Select the reviewed branch and `deploy/render.yaml` as the
   Blueprint path. Review all paid instances before applying it. The API
   applies migrations with the internal database connection. The database
   is configured to block external connections.
2. Confirm `/health` works on the assigned Render HTTPS hostname, then run the
   ingestion service once from its dashboard. The cron runs at minutes
   5, 20, 35, and 50 UTC so an export has time to arrive. Check job logs
   and wait for the next scheduled invocation.
3. In Vercel, use `frontend` as the project root, `npm ci` for installation,
   `npm run build` for the build, and `dist` as output. Use `/api` as
   `VITE_API_BASE_URL`. Add a `frontend/vercel.json` after the Render origin
   is assigned, substituting the real public HTTPS origin:

```json
{
  "rewrites": [
    {
      "source": "/api/:path*",
      "destination": "https://your-render-service.onrender.com/:path*"
    }
  ],
  "headers": [
    {
      "source": "/api/:path*",
      "headers": [{ "key": "Cache-Control", "value": "no-store" }]
    }
  ]
}
```

Use same-origin routing so the browser needs no CORS configuration. A
direct browser connection to another origin requires adding only the
approved frontend origins to FastAPI's CORS configuration. Database URLs
remain in Compose or Render service environment settings.

## Azure production deployment

The North Central US deployment runs Caddy, FastAPI, and the 15-minute
scheduler on an Ubuntu 24.04 `Standard_B2ats_v2` VM. Azure manages the
dedicated PostgreSQL 17 Flexible Server on Burstable `Standard_B1ms`.
The VM has 2 vCPUs, 1 GiB of RAM, and a 64 GiB Premium SSD LRS OS disk
at tier P6. The database has 32 GiB of storage, seven days of local backups,
and storage auto-grow, high availability, and geo-redundant backups disabled.

The deployment is in `rg-global-news-monitor-prod`. [Public dashboard](https://global-news-monitor.northcentralus.cloudapp.azure.com/)

| Resource | Name |
| --- | --- |
| Ubuntu VM | `vm-gnm-prod` |
| P6 OS disk | `osdisk-gnm-prod` |
| PostgreSQL Flexible Server and database | `gnm-postgres-ncus-202611`, `gnm` |
| VNet and app subnet | `vnet-gnm-prod`, `snet-gnm-app` |
| NIC and network security group | `nic-gnm-prod`, `nsg-gnm-prod` |
| Standard static public IPv4 | `pip-gnm-prod` |

The student subscription lists 750 VM hours and 750 B1ms database hours
per month, the P6 disk allowance, 32 GB each of database and backup storage,
1,500 public IP hours, and 15 GB of outbound data. These allowances expire
November 23, 2027 for this deployment. Azure reports the VM disk as P6,
which matches [Microsoft's free VM disk configuration](https://marketplace.microsoft.com/en-us/product/microsoft.freeaccountvirtualmachine?tab=overview).
Estimate $0 if the Standard static IPv4 is covered and usage stays within
the allowances. For the IPv4, budget $3.65 per 730 hours at retail until
the posted meter is visible. The free-services API returned an empty list
during launch, and the usage-details API rejected the student offer. Review
**Education → Overview** and
**Subscriptions → Azure for Students → View all free services** after
usage posts. Microsoft describes a one- to two-day delay for
[free-service metering](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/check-free-service-usage).
Keep the spending limit on and the $5 budget alerts enabled.
Track VM outbound bytes and the database's storage and backup usage as
history accumulates. A budget alert reports spending and leaves services
running under the subscription's spending limit.

During launch, Azure Monitor reported a database storage peak of 3.9 GiB,
about 17 MiB of backups, and about 33 MiB of VM outbound traffic in the
observed hour. This snapshot covers initial catch-up and build traffic.

Provision a resource group, VNet, subnet, network security group, VM NIC,
and one Standard static IPv4 with an Azure DNS label. Use an x64 Gen2 Ubuntu
image, Trusted Launch with Secure Boot and vTPM, and an SSH public key.
Install Docker Engine with its Compose plugin from the signed Docker apt
repository. The host uses a 2 GiB swap file on its existing P6 disk. Docker
stores its images locally, and Caddy stores its certificate state in a
persistent Docker volume. The deployment uses no registry or separate
build VM.

- Restrict SSH to the operator's IP, require key-only login, and disable
  direct root SSH. Test Caddy via an SSH tunnel with its host ports bound
  to loopback. The API has no
  published host port. After the audit, bind Caddy to the public interface
  and open TCP 80 and 443 for ACME validation. Verify the issued HTTPS
  certificate and HTTP redirect from an external client before sharing the URL.
- Keep Flexible Server's firewall closed except for the VM's exact static
  IPv4. Leave the broad Azure-services rule disabled. Verify a TLS connection
  from the VM and rejection from an unlisted IP. Use a direct connection
  to port 5432 so the ingestion session retains its advisory lock.
- Configure `azure.extensions` to allow PostGIS. Apply migrations as the
  database owner before starting the public stack. Run
  [`deploy/azure-roles.sql`](../deploy/azure-roles.sql) in the `gnm` database
  and set the two passwords using interactive `psql \password` prompts.
  Reapply grants for any new tables or sequences added by future migrations.
- Store runtime URLs as `GNM_READ_DATABASE_URL` and
  `GNM_WRITE_DATABASE_URL` in a mode-0600 file on the VM. Use a URL-encoded
  password, the Azure database hostname, and
  `sslmode=verify-full&sslrootcert=/etc/ssl/certs/ca-certificates.crt&connect_timeout=10`.
  Keep the database owner's credential outside the running services.
- Use a recent Docker Compose release that supports `!reset`. For the
  private smoke test set `BIND_ADDRESS=127.0.0.1` and `SITE_ADDRESS=:80`.
  Set `SITE_ADDRESS` to the Azure-managed DNS hostname and remove the
  loopback bind when opening the web ports. Pass `config --quiet` to avoid
  printing connection strings.

```bash
docker compose -p app --env-file deploy/.env -f compose.yaml -f deploy/compose.azure.yaml config --quiet
docker compose -p app --env-file deploy/.env -f compose.yaml -f deploy/compose.azure.yaml up -d --no-build --wait
```

Build `linux/amd64` images off the host where possible. For this release
the Go link step exceeded the VM's practical memory budget. The Caddy
binary was compiled off-host with the versions in
[`Dockerfile.web`](../deploy/Dockerfile.web), transferred by SSH, and packaged
on the VM. To reproduce that build, use `xcaddy` from its official release and
copy the resulting `proxy-build` directory to the host before packaging.

```bash
mkdir -p ../proxy-build/usr/bin
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 xcaddy build v2.11.9 \
  --output ../proxy-build/usr/bin/caddy \
  --replace golang.org/x/net=golang.org/x/net@v0.62.0

# With the binary and source on the VM, run from the repo root.
docker buildx build --load --platform linux/amd64 \
  --build-context proxy-build=../proxy-build -t app-web -f deploy/Dockerfile.web .
docker compose -p app --env-file deploy/.env --parallel 1 -f compose.yaml \
  -f deploy/compose.azure.yaml build --pull api
```

The overlay disables local PostgreSQL and runs the API with a read-only
login, read-only transactions, and a 12-second statement timeout. The
ingestion login can write to the tables and sequences it uses. Docker
rotates local service logs and restarts services after host or process
restarts. Before opening Caddy to the public, verify the API routes and a
real ingestion cycle. Scan both images at the deployed digests and check
NSG rules, database firewall rules, permissions, and TLS.

### Production verification

The managed database reached `0007_idempotency_hardening` with PostgreSQL 17
and PostGIS 3.6.4. Both runtime roles used hostname-verified TLS 1.3. The API
role was denied a write, and a client outside the VM's database firewall rule
could not connect. The network security group exposes only TCP 80 and 443 to
the internet. Production responses served a trusted Let's Encrypt certificate,
redirected HTTP to HTTPS, and included HSTS and a dashboard content security
policy. The public FastAPI routes were reached through Caddy.

Trivy scanned the exact images used for launch. It reported no detected
secrets, critical advisories, or high-severity advisories with an available
fix. Thirteen distinct high-severity Debian advisories and one Caddy
dependency advisory remained without fixes. The Caddy configuration does
not accept user-supplied JSON Web Keys. Rebuild and rescan as upstream
patches become available.

On November 24, 2026 UTC, the production scheduler started at 02:30 and
ran again at 02:45 and 03:00 without a restart or manual ingest. Each run
completed eight exports, including the most recent export and older
unprocessed slots. The production database had 32 completed checkpoints
and 24,162 distinct raw records with matching normalized rows at 03:00.

The browser verification covered the map, combined country and category
filtering, cursor pagination, and the responsive event feed. The open
dashboard advanced from the 02:45 source export to 03:00, keeping United
States and Politics selected while the count increased from 609 to 799.
A public API check verified a fixed UTC window, coverage, and populated
spike and tension routes. Initial coverage was partial and built up via
scheduled catch-up. Single-instance availability and sustained load at
the full retention window have yet to be measured.

## Public verification before publishing the URL

1. Open the HTTPS frontend from a separate network or device. Check
   `/api/health`, `/api/health/ready`, and `/api/freshness` through the
   frontend origin. Verify a migration and real ingestion in host logs.
2. Observe at least two automatic ingestion cycles and confirm the browser
   updates after the checkpoint advances. Confirm the public page stays
   responsive while the local development processes are off.
3. Apply a country and category filter, zoom the map, page through events,
   and check coverage and event timestamps. Compare indicators with the
   available collection history.
4. Check logs, database size, and representative request latency. Test a
   missed interval and recovery in a controlled preview. A replay should
   complete the missing checkpoint without growing duplicate event counts.
5. Add the verified URL to the repository README and About section when
   these checks pass.

The local benchmark measured five in-process TestClient requests per route
after PostgreSQL `ANALYZE`, with a 24-hour filter and partial collection.
Median elapsed times were 164.1 ms for analytics, 65.6 ms for events, 78.7 ms
for spikes, 73.3 ms for tension, and 63.7 ms for freshness. These values
include local connection and serialization time. Managed instance latency,
load limits, and 17- or 30-day query performance remain unverified.

## v1.1 migration and rollback

Use the existing Ubuntu VM, B1ms PostgreSQL 17 server, 32 GiB database,
and 17-day raw-event cleanup settings. Keep the $5 budget, spending limit,
static IP, and network controls in place. At $0.005/hour the IPv4 meter is
approximately $3.60 for a 30-day month or $3.72 for a 31-day month if other
usage stays within its allowances. Allow for one-time migration WAL and
temporary work files. Record Azure Storage Used and WAL usage before the
migration. Leave several GiB of headroom above the expected table size.
Production rehearsal and image approval come before rollout.

The current v1.1 cutover used the existing Compose project named `app`.
Its scanned source bundle is in the VM's
`/home/gnmops/gnm-deploy/candidates/410b11b/src`. The previous Compose
configuration and protected runtime environment remain in the original
`app` directory. Use the candidate Compose files and original runtime
environment for live v1.1 operations. See the
[validation report](V1.1_VALIDATION.md) for the exact image IDs, recovery
rehearsal, and measured release results.

### Prepare and apply

Confirm the existing administrator login before starting a maintenance
window. The runtime role files are for the API and scheduler. Keep any
administrator credential in approved protected storage and out of terminal
output, Git, and application configuration. A reset requires separate
approval and invalidates the previous password for the administrator.
Check that the API and scheduler still authenticate under their own roles
after any approved reset. [Microsoft administrator password reset procedure](https://learn.microsoft.com/en-us/azure/postgresql/security/security-reset-admin-password)

1. Build v1.1 backend and Caddy/frontend images off the VM where practical.
   Scan the exact candidate image digests. On the VM record the current
   container/image IDs and tag the v1.0 images locally for rollback before
   loading new images. Validate the production Compose overlay using
   `config --quiet`. Save the v1.0 Compose files for rollback.
2. Confirm a usable Flexible Server backup and record the UTC recovery
   point before migration. Use a PostgreSQL 17 client for any logical dump
   of the PostgreSQL 17 server. The VM's stock client is PostgreSQL 16.
   Keep a protected pre-migration dump and verify a restore to an isolated
   database before changing production. A managed restore would use a
   separate server, so obtain approval before provisioning one.
3. Stop the scheduler using the same `-p app`, environment file, and overlay
   used at deployment. Allow any manual ingest to finish before migration.
   Keep the v1.0 API serving during this step. The migration takes write
   locks on the raw/normalized tables and export checkpoints. Schedule an
   ingestion maintenance window and monitor database storage and locks.
4. Run Alembic once as the database owner using the candidate backend image.
   Use the existing verified-TLS owner URL through the environment in a
   trusted shell. Use a short lock timeout so a competing writer fails the
   migration promptly. The service roles keep their existing statement timeouts.
5. Verify the revision, quality report, raw counts, and sampled rollup parity.
   Verify the grants for existing Azure roles or run `azure-roles.sql` only
   when creating new runtime roles on a fresh installation. Optionally run
   `ANALYZE raw_events, normalized_events, event_hourly_counts` as the owner
   after migration. Start the upgraded API, web, and scheduler using the
   production Compose configuration and its existing runtime credentials.
6. Check HTTPS from an off-VM browser, the health routes, analytics and
   historical coverage, country profile, and map. Watch two scheduler cycles.
   Confirm a new archive slot, compare coverage and response times, and
   look for database errors and unexpected restarts.

Record the source slot and the retained event-count range before the
change. Afterward verify each completed export's coverage, compare hourly
rollups against raw rows for a fixed retained window, and check new
checkpoints across two consecutive automatic cycles. Record the same
HTTP windows before and after deployment. Compare the v1.0 analytics
request to the compatibility response first, then measure the v1.1
summary, viewport, history, and profile requests separately.

Example migration commands, with the backend candidate already saved under
its release tag and the owner URL exported privately as `DATABASE_URL`:

```bash
compose=(docker compose -p app --env-file deploy/.env -f compose.yaml -f deploy/compose.azure.yaml)
"${compose[@]}" config --quiet
"${compose[@]}" stop scheduler
docker run --rm --env DATABASE_URL --env 'PGOPTIONS=-c lock_timeout=5s' \
  gnm-backend:v1.1 python -m src.main migrate
unset DATABASE_URL
# Verify the revision, correction report, and rollup parity before restarting.
# Tag the verified v1.1 images under the image names selected by the overlay first.
"${compose[@]}" up -d --no-build --wait api scheduler web
```

Run the following as the owner to verify the report and count parity on an
hour when there has been steady coverage. A stale result sampled during
ingestion can be avoided with a read-only repeatable-read transaction.

```sql
TABLE alembic_version;
TABLE gdelt_code_correction_report;

BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
WITH bounds AS (SELECT date_trunc('hour', now(), 'UTC') - interval '1 hour' AS start)
SELECT
    (SELECT count(*) FROM normalized_events, bounds
     WHERE event_time_utc >= start AND event_time_utc < start + interval '1 hour') AS raw_count,
    (SELECT coalesce(sum(live_count), 0) FROM event_hourly_counts, bounds
     WHERE hour_utc = start) AS aggregate_count;
COMMIT;
```

### Rollback

For an application regression, stop the v1.1 scheduler, retag the saved
v1.0 images under the Compose image names, and start the v1.0 stack with the
saved configuration. The v1.1 schema is additive and the corrected rows
remain compatible with v1.0. The rollups detect v1.0 retention when a
checkpoint is already expired. During an extended rollback the v1.0
normalizer can create rows that need reconciliation again. Rehearse that
reconciliation on a restored copy before returning to v1.1. A temporary
rollback also pauses the automatic 90-day archive purge, which can be run
by the database owner if needed.

A schema downgrade is optional for application rollback. Export any needed
historical aggregate and coverage tables before downgrading because their
expired raw detail cannot be reconstructed. With writers stopped, run
`python -m alembic downgrade 0007_idempotency_hardening` under the v1.1 owner
image and configuration, then start v1.0. The downgrade drops new schema
objects and preserves corrected country and event values. Restore the
pre-migration backup only if the data corrections themselves must be
reversed. Review any point-in-time restore cost and replay the exports
ingested since the backup before switching traffic.

### Internal operations and external availability

Run `bash deploy/check-host.sh <public-hostname>` from the repo on the VM.
The HTTPS probes use loopback with the public hostname and certificate.
The script prints website and API timing, ingestion status, RAM, disk,
container exit and restart information, and host interface counters.
Use `docker compose logs --since 1h api scheduler` with the established
production flags to investigate. The API adds a `Server-Timing` header and a
latency/status log entry for every analytics, map, history, event, and profile
request. The existing rotated Docker logs bound local storage use.

| Signal | Where to inspect and what to do |
| --- | --- |
| Website or API fails its internal probe | Inspect Caddy, API, and PostgreSQL logs. A readiness failure with live API points to the database or ingest freshness. |
| Checkpoints lag or fail | `/api/health/monitor` tracks 96 slots ending 30 minutes behind the current clock. Inspect the listed UTC slots in scheduler logs and checkpoint status. Check upstream archive availability before replaying. |
| Scheduler heartbeat is stale after 25 minutes | Check Docker exit, restart, or OOM status and scheduler logs. A completed cycle means the polling process completed, and export coverage indicates what actually arrived. |
| Analytics or map latency rises | Compare latency log distributions for the same filter and range. Capture `EXPLAIN (ANALYZE, BUFFERS)` on a copy before changing indexes or database settings. |
| Storage growth or low remaining space | Review Flexible Server Storage Used, Storage Percent, WAL storage, and backup usage. Investigate before 70% and urgently at 85%. Confirm cleanup, autovacuum, and checkpoint progress. |
| VM memory, CPU credits, or container restarts | Track available memory locally and track Percentage CPU and CPU Credits Remaining in Azure Metrics. Investigate sustained available memory below 150 MiB, credit exhaustion, an OOM, or increasing restarts. |
| Transfer and billing | Track Network Out Total in Azure Metrics and reconcile actual billable bandwidth in Cost Management. Use the lower 15 GB student-account allowance for planning until the meter confirms coverage. Review usage near 10 GB/month and check Cost Analysis against the existing $5 budget. |

Azure's standard platform metrics are available with the existing account.
The runbook uses the existing metrics and local Docker logs, so it adds no
paid log ingestion or alert resources. [Azure Monitor pricing](https://azure.microsoft.com/en-us/pricing/details/monitor/) · [VM metrics](https://learn.microsoft.com/en-us/azure/azure-monitor/reference/supported-metrics/microsoft-compute-virtualmachines-metrics) · [PostgreSQL metrics](https://learn.microsoft.com/en-us/azure/azure-monitor/reference/supported-metrics/microsoft-dbforpostgresql-flexibleservers-metrics) · [Azure bandwidth pricing](https://azure.microsoft.com/en-us/pricing/details/bandwidth/)

An outside monitor tests public DNS, routing, certificate trust, Caddy,
and the API from outside Azure. UptimeRobot's free tier supports five-minute
HTTP checks and requires an account. If approved, configure separate HTTPS
monitors for `/` and `/api/health`; use `/api/health/ready` as an optional
data-readiness check with a separate alert. The internal monitor returns HTTP
200 for operational degradations and lists issues in its response. No external
monitor was enrolled for this release. [UptimeRobot free-tier documentation](https://help.uptimerobot.com/en/articles/11604710-who-should-use-uptimerobot-s-free-plan)
