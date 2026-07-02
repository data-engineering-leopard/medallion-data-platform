# Architecture

## Medallion Architecture

This project follows the Medallion Architecture pattern — a data design pattern used extensively in Databricks and Delta Lake environments.

### Why Medallion?

- **Separation of concerns** — each layer has a single responsibility
- **Reprocessability** — raw data is always preserved in Bronze
- **Data quality** — issues are caught and quarantined at Silver
- **Scalability** — adding new sources or tables requires minimal code changes

---

## Bronze Layer

Raw data ingested exactly as received. No transformations, no cleaning.

- One task file per source system
- Explicit schema enforcement via YAML
- Lenient schema validation — logs drift but does not fail
- Separate parquet output per data event

---

## Silver Layer

Cleaned and validated data with SCD2 history tracking.

- Config driven — adding a new table requires only a new YAML file
- Cleaning rules defined in YAML (drop nulls, uppercase, lowercase)
- SCD2 for slowly changing dimensions
- Quarantine for records missing required date columns

### SCD2 — Slowly Changing Dimension Type 2

Tracks the full history of changes to a record:

| customer_id | email | effective_from | effective_to | is_current |
|---|---|---|---|---|
| 1 | alice@old.com | 2024-01-01 | 2024-03-15 | false |
| 1 | alice@new.com | 2024-03-15 | 9999-12-31 | true |

### Quarantine

Records missing both `created_date` and `updated_date` are written to a separate quarantine path rather than being silently dropped.

---

## Gold Layer

Business ready dimensional model tables.

- **dim_customers** — SCD2 customer dimension with surrogate key
- **fact_orders** — Orders joined to customers at time of order
- **dim_leads** — SCD2 leads dimension with surrogate key

---

## Deployment

This project deploys to Databricks via Asset Bundles:

```bash
databricks bundle deploy --target dev
```

Each layer runs as an independent Databricks job, allowing independent scheduling and monitoring.
