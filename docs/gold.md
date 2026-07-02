# Gold Layer

The Gold layer produces business ready dimensional model tables following Kimball dimensional modelling principles.

## Tables

### dim_customers

Customer dimension with full SCD2 history and surrogate key.

| Column | Type | Description |
|---|---|---|
| customer_key | long | Surrogate key — unique per version |
| customer_id | integer | Natural key from source |
| name | string | Customer name |
| email | string | Customer email |
| status | string | Customer status |
| country | string | Customer country |
| effective_from | timestamp | When this version became active |
| effective_to | timestamp | When this version was superseded |
| is_current | boolean | True if this is the current version |

### fact_orders

Orders fact table joined to dim_customers **at the time of the order**.

| Column | Type | Description |
|---|---|---|
| order_id | integer | Order identifier |
| customer_key | long | FK to dim_customers at time of order |
| customer_id | integer | Natural key for reference |
| product | string | Product ordered |
| amount | float | Order amount |
| status | string | Order status |
| order_date | string | Date of order |

!!! info "Historical Accuracy"
    Orders are joined to the customer version that was active when
    the order was placed — not the current version. This ensures
    historical reports are accurate even after customer data changes.

### dim_leads

Salesforce leads dimension with full SCD2 history and surrogate key.

| Column | Type | Description |
|---|---|---|
| lead_key | long | Surrogate key — unique per version |
| lead_id | integer | Natural key from Salesforce |
| first_name | string | Lead first name |
| last_name | string | Lead last name |
| email | string | Lead email |
| company | string | Lead company |
| status | string | Lead status |
| lead_source | string | How the lead was acquired |
| country | string | Lead country |
| effective_from | timestamp | When this version became active |
| effective_to | timestamp | When this version was superseded |
| is_current | boolean | True if this is the current version |

## Adding a New Gold Table

1. Create a new task file: `src/my_project/tasks/gold/{table_name}.py`
2. Create a schema YAML: `config/schemas/gold/{table_name}.yaml`
3. Add the task to `databricks.yml`
4. Add the task to `src/my_project/pipeline.py`
