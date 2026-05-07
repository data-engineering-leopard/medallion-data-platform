import logging


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_LEVEL = logging.INFO


def setup_logging(level: int = LOG_LEVEL) -> None:
    """
    Configures logging for the entire application.

    Call this once at the entry point of each task.
    Uses __name__ based loggers throughout the codebase
    so log messages show the module they came from.

    In Databricks, logging is handled by the platform
    so this only applies when running locally.
    """
    logging.basicConfig(
        level=level,
        format=LOG_FORMAT
    )


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger for the given module name.

    Usage:
        from my_project.utils.logger import get_logger
        logger = get_logger(__name__)
    """
    return logging.getLogger(name)