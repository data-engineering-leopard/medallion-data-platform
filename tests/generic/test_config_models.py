import pytest
from pydantic import ValidationError
from my_project.utils.config_models import (
    BronzeFieldConfig,
    BronzeSchemaConfig,
    SilverTableConfig,
    GoldTableConfig,
    PipelinePathsConfig,
    PipelineSourcesConfig,
    PipelineSilverConfig,
    PipelineConfig
)


# ===========================
# BRONZE SCHEMA CONFIG TESTS
# ===========================

class TestBronzeFieldConfig:

    def test_valid_field_config(self):
        """Should create valid field config"""
        field = BronzeFieldConfig(
            name="id",
            type="integer",
            nullable=True
        )
        assert field.name == "id"
        assert field.type == "integer"
        assert field.nullable is True

    def test_nullable_defaults_to_true(self):
        """Nullable should default to True if not specified"""
        field = BronzeFieldConfig(name="id", type="integer")
        assert field.nullable is True

    def test_invalid_type_raises_error(self):
        """Unknown field type should raise ValidationError"""
        with pytest.raises(ValidationError):
            BronzeFieldConfig(name="id", type="unknown_type")

    def test_empty_name_raises_error(self):
        """Empty field name should raise ValidationError"""
        with pytest.raises(ValidationError):
            BronzeFieldConfig(name="", type="string")


class TestBronzeSchemaConfig:

    def test_valid_schema_config(self):
        """Should create valid schema config"""
        config = BronzeSchemaConfig(
            table="customers",
            fields=[
                BronzeFieldConfig(name="id", type="integer"),
                BronzeFieldConfig(name="name", type="string")
            ]
        )
        assert config.table == "customers"
        assert len(config.fields) == 2

    def test_empty_fields_raises_error(self):
        """Schema with no fields should raise ValidationError"""
        with pytest.raises(ValidationError):
            BronzeSchemaConfig(table="customers", fields=[])

    def test_missing_table_raises_error(self):
        """Missing table name should raise ValidationError"""
        with pytest.raises(ValidationError):
            BronzeSchemaConfig(
                fields=[BronzeFieldConfig(name="id", type="integer")]
            )


# ===========================
# SILVER TABLE CONFIG TESTS
# ===========================

class TestSilverTableConfig:

    def test_valid_non_scd2_config(self):
        """Should create valid non-SCD2 silver config"""
        config = SilverTableConfig(
            table="orders",
            input_path="data/bronze/orders",
            output_path="data/silver/orders",
            scd2=False
        )
        assert config.table == "orders"
        assert config.scd2 is False

    def test_valid_scd2_config(self):
        """Should create valid SCD2 silver config"""
        config = SilverTableConfig(
            table="customers",
            input_path="data/bronze/customers",
            output_path="data/silver/customers",
            scd2=True,
            scd2_key="id",
            scd2_track_columns=["email", "status"]
        )
        assert config.scd2 is True
        assert config.scd2_key == "id"

    def test_scd2_without_key_raises_error(self):
        """SCD2 config without scd2_key should raise ValidationError"""
        with pytest.raises(ValidationError):
            SilverTableConfig(
                table="customers",
                input_path="data/bronze/customers",
                output_path="data/silver/customers",
                scd2=True,
                scd2_track_columns=["email"]
            )

    def test_scd2_without_track_columns_raises_error(self):
        """SCD2 config without track columns should raise ValidationError"""
        with pytest.raises(ValidationError):
            SilverTableConfig(
                table="customers",
                input_path="data/bronze/customers",
                output_path="data/silver/customers",
                scd2=True,
                scd2_key="id"
            )

    def test_drop_null_columns_defaults_to_empty(self):
        """drop_null_columns should default to empty list"""
        config = SilverTableConfig(
            table="orders",
            input_path="data/bronze/orders",
            output_path="data/silver/orders",
            scd2=False
        )
        assert config.drop_null_columns == []

    def test_missing_input_path_raises_error(self):
        """Missing input_path should raise ValidationError"""
        with pytest.raises(ValidationError):
            SilverTableConfig(
                table="orders",
                output_path="data/silver/orders",
                scd2=False
            )


# ===========================
# GOLD TABLE CONFIG TESTS
# ===========================

class TestGoldTableConfig:

    def test_valid_gold_config(self):
        """Should create valid gold config"""
        config = GoldTableConfig(
            table="dim_customers",
            input_path="data/silver/customers",
            output_path="data/gold/dim_customers",
            fields=[
                BronzeFieldConfig(name="customer_key", type="long"),
                BronzeFieldConfig(name="customer_id", type="integer")
            ]
        )
        assert config.table == "dim_customers"
        assert len(config.fields) == 2

    def test_missing_output_path_raises_error(self):
        """Missing output_path should raise ValidationError"""
        with pytest.raises(ValidationError):
            GoldTableConfig(
                table="dim_customers",
                input_path="data/silver/customers",
                fields=[
                    BronzeFieldConfig(name="id", type="integer")
                ]
            )


# ===========================
# PIPELINE CONFIG TESTS
# ===========================

class TestPipelineConfig:

    def test_valid_pipeline_config(self):
        """Should create valid pipeline config"""
        config = PipelineConfig(
            paths=PipelinePathsConfig(
                raw="data/raw",
                bronze="data/bronze",
                silver="data/silver",
                gold="data/gold",
                quarantine="quarantine/silver"
            ),
            sources=PipelineSourcesConfig(
                online_tcg={"customers": "customers.csv",
                            "orders": "orders.csv"},
                salesforce={"leads": "sf_leads.csv"}
            ),
            silver=PipelineSilverConfig(
                config_path="config/schemas/silver"
            )
        )
        assert config.paths.raw == "data/raw"
        assert config.paths.bronze == "data/bronze"

    def test_missing_paths_raises_error(self):
        """Missing paths section should raise ValidationError"""
        with pytest.raises(ValidationError):
            PipelineConfig(
                sources=PipelineSourcesConfig(
                    online_tcg={"customers": "customers.csv"},
                    salesforce={"leads": "sf_leads.csv"}
                ),
                silver=PipelineSilverConfig(
                    config_path="config/schemas/silver"
                )
            )

    def test_missing_raw_path_raises_error(self):
        """Missing raw path should raise ValidationError"""
        with pytest.raises(ValidationError):
            PipelinePathsConfig(
                bronze="data/bronze",
                silver="data/silver",
                gold="data/gold",
                quarantine="quarantine/silver"
            )