from typing import Optional
from pydantic import BaseModel, field_validator, model_validator
from my_project.utils.logger import get_logger

logger = get_logger(__name__)

VALID_FIELD_TYPES = [
    "string", "integer", "float", "double",
    "boolean", "long", "timestamp", "date"
]


# ===========================
# BRONZE MODELS
# ===========================

class BronzeFieldConfig(BaseModel):
    """Defines a single field in a Bronze schema YAML"""
    name: str
    type: str
    nullable: bool = True

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v.lower() not in VALID_FIELD_TYPES:
            raise ValueError(
                f"Unknown field type '{v}'. "
                f"Must be one of: {VALID_FIELD_TYPES}"
            )
        return v.lower()

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Field name cannot be empty")
        return v


class BronzeSchemaConfig(BaseModel):
    """Defines a Bronze schema YAML file"""
    table: str
    fields: list[BronzeFieldConfig]

    @field_validator("fields")
    @classmethod
    def validate_fields(cls, v: list) -> list:
        if not v:
            raise ValueError("Schema must have at least one field")
        return v


# ===========================
# SILVER MODELS
# ===========================

class SilverTableConfig(BaseModel):
    """
    Defines a Silver table YAML config.
    Validates SCD2 settings are consistent —
    if scd2=True then scd2_key and scd2_track_columns are required.
    """
    table: str
    input_path: str
    output_path: str
    scd2: bool
    scd2_key: Optional[str] = None
    scd2_track_columns: Optional[list[str]] = None
    effective_from_column: Optional[str] = None
    effective_from_fallback_column: Optional[str] = None
    effective_from_ts: Optional[str] = None
    quarantine_path: Optional[str] = None
    drop_null_columns: list[str] = []
    uppercase_columns: list[str] = []
    lowercase_columns: list[str] = []

    @model_validator(mode="after")
    def validate_scd2_fields(self) -> "SilverTableConfig":
        if self.scd2:
            if not self.scd2_key:
                raise ValueError(
                    f"scd2_key is required when scd2=True "
                    f"for table '{self.table}'"
                )
            if not self.scd2_track_columns:
                raise ValueError(
                    f"scd2_track_columns is required when scd2=True "
                    f"for table '{self.table}'"
                )
        return self


# ===========================
# GOLD MODELS
# ===========================

class GoldTableConfig(BaseModel):
    """Defines a Gold table YAML config"""
    table: str
    input_path: str
    output_path: str
    fields: list[BronzeFieldConfig]
    dim_customers_path: Optional[str] = None
    fact_orders_path: Optional[str] = None


# ===========================
# PIPELINE CONFIG MODELS
# ===========================

class PipelinePathsConfig(BaseModel):
    """Defines the paths section of pipeline_config.yaml"""
    raw: str
    bronze: str
    silver: str
    gold: str
    quarantine: str


class PipelineSourcesConfig(BaseModel):
    """Defines the sources section of pipeline_config.yaml"""
    online_tcg: dict[str, str]
    salesforce: dict[str, str]


class PipelineSilverConfig(BaseModel):
    """Defines the silver section of pipeline_config.yaml"""
    config_path: str


class PipelineConfig(BaseModel):
    """
    Top level pipeline config model.
    Validates the entire pipeline_config.yaml file.
    """
    paths: PipelinePathsConfig
    sources: PipelineSourcesConfig
    silver: PipelineSilverConfig