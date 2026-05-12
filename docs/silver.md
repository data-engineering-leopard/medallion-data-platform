# Silver Layer

The Silver layer cleans and validates Bronze data, applying SCD2 history tracking for slowly changing dimensions.

## Principles

!!! info "Silver Rules"
    - Config driven — zero code changes to add a new table
    - Clean data — remove nulls, normalise casing
    - Track history — SCD2 for dimensions that change over time
    - Quarantine — never silently drop bad records

## Config Driven Design

Adding a new Silver table requires only a new YAML file in `config/schemas/silver/`. No code changes needed.

## Silver YAML Format

```yaml
table: customers
input_path: data/bronze/customers
output_path: data/silver/customers
quarantine_path: quarantine/silver/customers
scd2: true
scd2_key: id
effective_from_column: updated_date
effective_from_fallback_column: created_date
scd2_track_columns:
  - email
  - status
  - country
drop_null_columns:
  - name
  - country
uppercase_columns:
  - name
lowercase_columns:
  - status
```

## Cleaning Rules

| Rule | Description |
|---|---|
| `drop_null_columns` | Remove rows where these columns are null |
| `uppercase_columns` | Uppercase values in these columns |
| `lowercase_columns` | Lowercase values in these columns |

## SCD2 Configuration

| Field | Description |
|---|---|
| `scd2` | Enable SCD2 history tracking |
| `scd2_key` | Unique identifier for each record |
| `effective_from_column` | Column to use as effective_from date |
| `effective_from_fallback_column` | Fallback if primary date column is null |
| `scd2_track_columns` | Columns that trigger a new version when changed |

## Quarantine

Records missing both date columns are written to `quarantine_path` instead of Silver. This ensures:

- Nothing is silently dropped
- Bad records can be investigated and reprocessed
- Quarantine is separate from production data paths

!!! warning "Quarantine Location"
    Quarantine data lives in `quarantine/silver/` — completely separate
    from `data/` to avoid mixing with production data.