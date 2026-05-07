import logging
import yaml

logger = logging.getLogger(__name__)


def load_pipeline_config(config_path: str) -> dict:
    """
    Loads the pipeline config from a YAML file.

    Returns a dict with paths, sources and silver config.
    Raises FileNotFoundError if the config file does not exist.
    """
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            logger.info(f"Loaded pipeline config from: {config_path}")
            return config
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Pipeline config not found at: {config_path}"
        )


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