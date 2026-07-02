# Bronze Layer

The Bronze layer ingests raw data from source systems exactly as received.

## Principles

!!! info "Bronze Rules"
    - Load everything as-is — no cleaning, no filtering
    - Use explicit schemas from YAML — never infer
    - Validate leniently — log drift but do not fail
    - Write separate parquet output per data event

## Source Systems

### Online TCG

Handles customers and orders from the Online TCG shop.

**Task file:** `src/my_project/tasks/bronze/bronze_online_tcg.py`

**Schema configs:**
- `config/schemas/bronze/online_tcg_customers.yaml`
- `config/schemas/bronze/online_tcg_orders.yaml`

### Salesforce

Handles leads from Salesforce CRM.

**Task file:** `src/my_project/tasks/bronze/bronze_salesforce.py`

**Schema config:**
- `config/schemas/bronze/salesforce_leads.yaml`

## Adding a New Source System

1. Create a new task file: `src/my_project/tasks/bronze/bronze_{source}.py`
2. Create schema YAMLs in `config/schemas/bronze/`
3. Add the task to `databricks.yml`
4. Add the source to `config/pipeline_config.yaml`

No existing code needs to change.

## Schema YAML Format

```yaml
table: customers
fields:
  - name: id
    type: integer
    nullable: true
  - name: name
    type: string
    nullable: true
```

### Supported Field Types

| Type | PySpark Type |
|---|---|
| string | StringType |
| integer | IntegerType |
| float | FloatType |
| double | DoubleType |
| boolean | BooleanType |
| long | LongType |
| timestamp | TimestampType |
| date | DateType |
