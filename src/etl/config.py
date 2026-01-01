"""
Configuration management for ETL pipeline.
"""

import os
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = PROJECT_ROOT / "config"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"


class Config:
    """Base configuration class."""

    # Data paths
    DATA_ROOT = DATA_DIR
    SOURCE_DATA = DATA_DIR / "sources"
    ANALYTICS_DATA = DATA_DIR / "analytics"
    EXPORTS_DATA = DATA_DIR / "exports"
    TEMP_DATA = DATA_DIR / "temp"

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def load_yaml(cls, filename: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        config_path = CONFIG_DIR / filename
        if not config_path.exists():
            logger.warning(f"Config file not found: {config_path}")
            return {}

        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}


# Initialize logger
logger.add(
    DATA_DIR / "logs" / "etl_{time}.log",
    rotation="500 MB",
    retention="10 days",
    format="{time} | {level: <8} | {name}:{function}:{line} - {message}",
)

logger.info(f"ETL initialized. Project root: {PROJECT_ROOT}")
