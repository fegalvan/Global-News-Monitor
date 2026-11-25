# Working on Global News Monitor

Keep the dashboard centered on the reporting GDELT observed. Use plain labels, show the relevant time window and where data is missing, and connect analytical changes to the records and links that can be inspected. Treat record counts, article links, reporting volume, and real-world conditions as separate concepts. Explain a method where a reader can find it without interrupting the main view.

When changing analytics, match the time windows, account for missing exports, and preserve the distinction between a complete zero and missing data. Check calculations against direct queries and cover edge cases with tests. Use retained real data for local rehearsals and label synthetic fixtures and local benchmarks accordingly.

Keep the README short. Carry the development story forward in [the journey](docs/DEVELOPMENT_JOURNEY.md), using the project's commits and release records for names, dates, and measurements. In the journey, write from the author’s perspective and introduce tools where they solved a problem the project actually faced. Describe what the author needed to understand without assuming undocumented experiences or overstating their contributions. Acknowledge ChatGPT and Codex briefly and accurately. Keep deeper explanations and clearly labeled illustrative examples in the walkthrough. Keep setup, API contracts, operations, and methods in the corresponding reference docs. Preserve the original meaning and evidence in prior validation reports.

Work against an isolated local database when making release changes. Preserve the existing resource footprint, credentials, and restricted database roles. Production access, deployment, a merge into `main`, and changes that incur recurring costs require the owner's explicit authorization.
