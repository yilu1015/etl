"""
Utility functions for ETL pipeline.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger


def load_json(filepath: Path) -> Dict[str, Any]:
    """Load JSON file."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        return {}
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {filepath}")
        return {}


def save_json(data: Dict[str, Any], filepath: Path, pretty: bool = True) -> None:
    """Save data to JSON file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2 if pretty else None)
    logger.info(f"Saved JSON to {filepath}")


def load_csv(filepath: Path, **kwargs) -> Optional[pd.DataFrame]:
    """Load CSV file as DataFrame."""
    try:
        return pd.read_csv(filepath, **kwargs)
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        return None
    except Exception as e:
        logger.error(f"Error reading CSV {filepath}: {e}")
        return None


def save_csv(df: pd.DataFrame, filepath: Path, **kwargs) -> None:
    """Save DataFrame to CSV file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(filepath, index=False, encoding="utf-8", **kwargs)
    logger.info(f"Saved CSV to {filepath}")


def ensure_dir(dirpath: Path) -> None:
    """Ensure directory exists."""
    dirpath.mkdir(parents=True, exist_ok=True)
