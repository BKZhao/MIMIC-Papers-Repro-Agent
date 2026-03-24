# Security Notes

## Migration rule

When reusing logic from existing agent projects, keep architecture patterns but do not copy embedded credentials.

## Required practices

- Store all DB/API secrets in environment variables.
- Keep sample values only in `.env.example`.
- Never print raw secret values in logs or reports.
- Use masked connection strings in diagnostics.

## Current connector stance

`src/repro_agent/db/connectors.py` only reads `MIMIC_PG_*` env variables and prints a masked DSN.
No credentials are committed in framework code.

