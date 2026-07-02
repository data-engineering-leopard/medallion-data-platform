import pytest
from my_project.utils.pipeline_config_loader import (
    load_pipeline_config,
    get_path
)


class TestLoadPipelineConfig:

    def test_loads_config_from_valid_yaml(self):
        """Should load pipeline config from YAML"""
        config = load_pipeline_config("config/pipeline_config.yaml")
        assert isinstance(config, dict)

    def test_config_has_paths_section(self):
        """Config should have a paths section"""
        config = load_pipeline_config("config/pipeline_config.yaml")
        assert "paths" in config

    def test_config_has_sources_section(self):
        """Config should have a sources section"""
        config = load_pipeline_config("config/pipeline_config.yaml")
        assert "sources" in config

    def test_config_has_silver_section(self):
        """Config should have a silver section"""
        config = load_pipeline_config("config/pipeline_config.yaml")
        assert "silver" in config

    def test_config_paths_has_required_keys(self):
        """Paths section should have all required keys"""
        config = load_pipeline_config("config/pipeline_config.yaml")
        paths = config["paths"]
        assert "raw" in paths
        assert "bronze" in paths
        assert "silver" in paths
        assert "gold" in paths
        assert "quarantine" in paths

    def test_config_sources_has_online_tcg(self):
        """Sources section should have online_tcg"""
        config = load_pipeline_config("config/pipeline_config.yaml")
        assert "online_tcg" in config["sources"]

    def test_config_sources_has_salesforce(self):
        """Sources section should have salesforce"""
        config = load_pipeline_config("config/pipeline_config.yaml")
        assert "salesforce" in config["sources"]

    def test_raises_error_for_missing_config(self):
        """Should raise FileNotFoundError if config file does not exist"""
        with pytest.raises(FileNotFoundError):
            load_pipeline_config("config/nonexistent.yaml")


class TestGetPath:

    def test_gets_nested_path(self):
        """Should retrieve nested value from config dict"""
        config = load_pipeline_config("config/pipeline_config.yaml")
        result = get_path(config, "paths", "bronze")
        assert result == "data/bronze"

    def test_gets_source_file(self):
        """Should retrieve source file name from config"""
        config = load_pipeline_config("config/pipeline_config.yaml")
        result = get_path(config, "sources", "online_tcg", "customers")
        assert result == "customers.csv"

    def test_gets_silver_config_path(self):
        """Should retrieve silver config path"""
        config = load_pipeline_config("config/pipeline_config.yaml")
        result = get_path(config, "silver", "config_path")
        assert result == "assets/silver"