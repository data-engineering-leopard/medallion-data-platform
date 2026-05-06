import pytest
from pyspark.sql.types import (
    StructType, StringType, IntegerType
)
from my_project.utils.schema_loader import load_schema_from_yaml


class TestSchemaLoader:

    def test_loads_schema_from_valid_yaml(self):
        """Should return a StructType from a valid YAML file"""
        schema = load_schema_from_yaml("config/schemas/bronze/online_tcg_customers.yaml")
        assert isinstance(schema, StructType)

    def test_loaded_schema_has_correct_fields(self):
        """Schema fields should match what is defined in the YAML"""
        schema = load_schema_from_yaml("config/schemas/bronze/online_tcg_customers.yaml")
        field_names = [field.name for field in schema.fields]
        assert "id" in field_names
        assert "name" in field_names
        assert "email" in field_names
        assert "status" in field_names
        assert "country" in field_names

    def test_loaded_schema_has_correct_types(self):
        """Field types should match what is defined in the YAML"""
        schema = load_schema_from_yaml("config/schemas/bronze/online_tcg_customers.yaml")
        field_map = {field.name: field.dataType for field in schema.fields}
        assert isinstance(field_map["id"], IntegerType)
        assert isinstance(field_map["name"], StringType)

    def test_loaded_schema_respects_nullable(self):
        """Nullable flags should match what is defined in the YAML"""
        schema = load_schema_from_yaml("config/schemas/bronze/online_tcg_customers.yaml")
        field_map = {field.name: field.nullable for field in schema.fields}
        assert field_map["id"] is True

    def test_raises_error_for_missing_yaml(self):
        """Should raise a clear error if the YAML file does not exist"""
        with pytest.raises(FileNotFoundError):
            load_schema_from_yaml("config/schemas/nonexistent.yaml")

    def test_raises_error_for_unknown_type(self, tmp_path):
        """Should raise a clear error if an unknown type is in the YAML"""
        bad_yaml = tmp_path / "bad_schema.yaml"
        bad_yaml.write_text(
            "table: test\nfields:\n  - name: id\n    type: unknown_type\n    nullable: true\n"
        )
        with pytest.raises(ValueError):
            load_schema_from_yaml(str(bad_yaml))