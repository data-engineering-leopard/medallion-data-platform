# API Reference

Auto-generated API documentation from source code docstrings.

## Tasks

### Bronze

::: my_project.tasks.bronze.bronze_online_tcg
    options:
      members:
        - load_customers
        - load_orders
        - get_customers_schema
        - get_orders_schema
        - run_bronze

::: my_project.tasks.bronze.bronze_salesforce
    options:
      members:
        - load_leads
        - get_leads_schema
        - run_bronze_salesforce

### Silver

::: my_project.tasks.silver.silver_task
    options:
      members:
        - load_silver_config
        - apply_cleaning_rules
        - run_silver_table
        - run_silver

### Gold

::: my_project.tasks.gold.dim_customers
    options:
      members:
        - build_dim_customers
        - run_dim_customers

::: my_project.tasks.gold.fact_orders
    options:
      members:
        - build_fact_orders
        - run_fact_orders

::: my_project.tasks.gold.dim_leads
    options:
      members:
        - build_dim_leads
        - run_dim_leads

## Utilities

::: my_project.utils.scd2
    options:
      members:
        - separate_quarantine_records
        - resolve_effective_from
        - merge_scd2
        - apply_scd2

::: my_project.utils.schema_loader
    options:
      members:
        - load_schema_from_yaml

::: my_project.utils.schema_validator
    options:
      members:
        - validate_schema

::: my_project.utils.surrogate_key
    options:
      members:
        - add_surrogate_key

::: my_project.utils.config_models
    options:
      members:
        - BronzeFieldConfig
        - BronzeSchemaConfig
        - SilverTableConfig
        - GoldTableConfig
        - PipelineConfig

::: my_project.utils.pipeline_config_loader
    options:
      members:
        - load_pipeline_config
        - get_path
