# Global News Monitor

Global News Monitor helps you see where events are being reported around the world, how reporting changes over time, and which articles you can follow to learn more. I built it as an independent project while studying CMDA at Virginia Tech. It was my first time building and deploying a live dashboard.

**[Open the live dashboard](https://global-news-monitor.northcentralus.cloudapp.azure.com/)** · **[Read the development story](docs/DEVELOPMENT_JOURNEY.md)**

The public site runs v1.1. The v1.2 dashboard is available for local review on the `v1.2-improvements` branch. [v1.1 release](https://github.com/fegalvan/Global-News-Monitor/releases/tag/v1.1.0) · [Run locally](docs/LOCAL_DEVELOPMENT.md)

## What you can explore

- Use the **map** or country selector to see where event records are concentrated. A country profile shows its most reported locations and opens a country-specific view of the coverage.
- Use **Trends** to compare equal periods. In v1.2, an available comparison also shows how each category contributed to the difference and lets you open coverage for the period you want to inspect.
- Open **News coverage** in v1.2 to explore original GDELT links by country, category, and time. Switch between varied sources and most recent links, and open each record’s coded details. **Event records** shows the underlying entries individually.

The GDELT Project identifies events in news coverage using automated coding. Its records reflect which sources it collects and how those sources report. The same article can produce several records, and differences in record counts say more about reporting patterns than the severity of what happened. [GDELT Events codebook](https://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf)

## How it works

The Python pipeline processes the latest GDELT files every 15 minutes. PostgreSQL keeps the original source fields, the cleaned records the dashboard uses, and hourly summaries for longer comparisons. FastAPI runs the filtered queries that feed the React dashboard. Caddy serves the site and its API on the same HTTPS address.

```mermaid
flowchart LR
    G["GDELT event exports"] --> P["Python collector"]
    P --> D["Managed PostgreSQL + PostGIS"]
    D --> A["FastAPI"]
    A --> W["Caddy"]
    W --> R["React in the browser"]
    subgraph VM["Azure VM · Docker Compose"]
      P
      A
      W
    end
```

*Simplified deployment view. The collector and API share the database, while the browser talks to Caddy. PostgreSQL retains 17 days of event detail and 90 days of hourly history in production.*

## Project notes

| If you want to see… | Start here |
| --- | --- |
| How the project grew from a Python script to a live dashboard | [Development journey](docs/DEVELOPMENT_JOURNEY.md) |
| How the data, map, and comparisons work | [Technical walkthrough](docs/TECHNICAL_WALKTHROUGH.md) |
| How to run and test the project yourself | [Local setup](docs/LOCAL_DEVELOPMENT.md) |
| Request parameters and response shapes | [API reference](docs/API_REFERENCE.md) |
| Azure setup, roles, monitoring, backups, and recovery | [Deployment guide](docs/DEPLOYMENT.md) |
| What was verified at each release | [v1.1 validation](docs/V1.1_VALIDATION.md) · [v1.2 local validation](docs/V1.2_VALIDATION.md) · [v1.2 quality follow-up](docs/V1.2_QUALITY_VALIDATION.md) |
