import yaml
from my_project.utils.logger import get_logger
from my_project.utils.config_models import PipelineConfig

logger = get_logger(__name__)


def load_pipeline_config(config_path: str) -> dict:
    """
    Loads and validates the pipeline config from a YAML file.
    Uses Pydantic to validate the config structure.

    Raises FileNotFoundError if the config file does not exist.
    Raises ValidationError if the config is invalid.
    """
    try:
        with open(config_path, "r") as f:
            raw_config = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Pipeline config not found at: {config_path}")

    # Validate with Pydantic — raises ValidationError if invalid
    config = PipelineConfig(**raw_config)
    logger.info(f"Loaded and validated pipeline config from: {config_path}")

    return config.model_dump()


def get_path(config: dict, *keys: str) -> str:
    """
    Helper to safely retrieve a nested path from config.

    Example:
        get_path(config, "paths", "bronze")
        → "data/bronze"
    """
    value = config
    for key in keys:
        value = value[key]
    return value
