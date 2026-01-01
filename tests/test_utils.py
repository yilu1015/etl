"""Tests for utility functions."""

import json
from pathlib import Path

import pytest

from etl import utils


class TestLoadJson:
    """Test JSON loading functionality."""

    def test_load_json_valid_file(self, tmp_path):
        """Test loading valid JSON file."""
        test_file = tmp_path / "test.json"
        test_data = {"key": "value", "number": 42}
        
        with open(test_file, "w") as f:
            json.dump(test_data, f)
        
        result = utils.load_json(test_file)
        assert result == test_data

    def test_load_json_nonexistent_file(self):
        """Test loading non-existent file returns empty dict."""
        result = utils.load_json(Path("/nonexistent/file.json"))
        assert result == {}


class TestSaveJson:
    """Test JSON saving functionality."""

    def test_save_json(self, tmp_path):
        """Test saving JSON file."""
        test_file = tmp_path / "output.json"
        test_data = {"key": "value", "number": 42}
        
        utils.save_json(test_data, test_file)
        
        assert test_file.exists()
        with open(test_file) as f:
            saved_data = json.load(f)
        assert saved_data == test_data
