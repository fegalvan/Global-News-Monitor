# Deploy Global News Monitor

Deployment is pending a hosting choice and account authorization. The
templates in this repository are prepared for review and have not provisioned
any resources. Costs below are estimates in USD checked November 23, 2026,
before tax, domain renewals, or usage overages.

## Hosting options

| Option | Estimated recurring cost | Tradeoff |
| --- | --- | --- |
| Oracle Cloud Always Free VM with Compose | $0 if an eligible VM, storage, and hostname are available | One instance runs every service. A1 capacity is region-dependent and Oracle may reclaim idle instances. Use the student domain benefit for a hostname if eligible. |
| Azure for Students VM with Compose | $0 out of pocket while eligible credit covers the VM, disk, and IP | The Pack lists $100 in Azure credit. Azure documents a 12-month term and student renewal eligibility. Size and regional charges determine how long the credit lasts. |
| Vercel Hobby + Render Free API + Neon Free + GitHub Actions | $0 within limits, demo only | Render sleeps after 15 idle minutes and can take about one minute to wake. Neon Free provides 1 GB per project and 100 CU-hours per month. Public-repository GitHub Actions schedules can be delayed or dropped and disable after 60 days without repository activity. The database fits only a few days of this workload. |
| Hetzner CX23 in Europe with Compose | $6.49 compute + $0.60 IPv4, totaling $7.09 if capacity is available | 2 vCPU, 4 GB RAM, 40 GB SSD. The product page showed it as unavailable during review. Hosting in Europe also adds network latency for visitors in the U.S. |
| DigitalOcean Basic VM with Compose | $12 for 1 vCPU, 2 GiB RAM, 50 GiB SSD and 2,000 GiB transfer. Optional weekly VM backup adds $2.40, totaling $14.40. | One always-running host with OS, database and backup maintenance. The $6 1 GiB plan has less memory for PostgreSQL, ETL, and concurrent chart requests. |
| Vercel Hobby + managed Render | About $15.50 at light use, itemized below | Less server maintenance. The small managed database needs a workload check, and cron and bandwidth can raise cost. |

Start with an eligible Always Free VM if capacity and reclaim risk are
acceptable. Azure student credit offers a time-limited alternative. Check
the $7.09 Hetzner plan if European hosting is acceptable. The $12 DigitalOcean
VM is the fallback with a published plan in this budget. Both use a single-instance
deployment, so maintenance and provider outages can interrupt service.
Confirm the hostname renewal price and optional backup before ordering.

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

## Azure for Students — split VM and database

The Azure plan uses a North Central US `Standard_B2ats_v2` Ubuntu VM and
PostgreSQL 17 Flexible Server on Burstable `Standard_B1ms`. Confirm account
eligibility for VM hours, the 64 GiB P6 OS disk, and 32 GB database and
backup allowances before creating resources. One Standard static IPv4
accounts for the estimated $3.65 monthly charge when those allowances apply.
Use a single database instance with 32 GiB of storage, seven-day backup
retention, and local backups. Leave high availability, geo-redundant
backups, and storage auto-grow disabled for this configuration. Check
storage and backup usage after initial ingestion and during steady state.

- Restrict SSH to the operator's IP and require key-only login. Test Caddy
  via an SSH tunnel with its host ports bound to loopback. The API has no
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
docker compose --env-file deploy/.env -f compose.yaml -f deploy/compose.azure.yaml config --quiet
docker compose --env-file deploy/.env -f compose.yaml -f deploy/compose.azure.yaml up -d --no-build
```

Build `linux/amd64` images on a machine with more memory and load them onto
the small VM before starting the services. The Azure overlay
disables the local PostgreSQL service by default and runs the API with a
read-only login, read-only transactions, and a 12-second statement timeout.
The ingestion login has write access only to the ingestion tables it uses.
Verify every API route and a real ingestion cycle before opening Caddy to
the public. Capture image vulnerability scan results at the deployed digests,
and confirm NSG rules, DB firewall, read/write permissions, and TLS on the
actual infrastructure.

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
