# ETL Pipeline

Extract, Transform, and Load (ETL) pipeline to process Chinese-language documents from the Chinese Archives project.

## Project Structure

```
etl/
├── src/etl/                 # Main package source code
│   ├── __init__.py
│   ├── config.py            # Configuration management
│   └── utils.py             # Utility functions
├── tests/                   # Test suite
├── notebooks/               # Jupyter notebooks
│   ├── reference/           # Documented reference notebooks
│   └── exploration/         # Personal exploration (not tracked)
├── data/                    # Data directory (mostly gitignored)
│   ├── sources/             # Source PDFs and metadata
│   ├── analytics/           # Analytics and intermediate results
│   ├── exports/             # Final processed outputs
│   └── temp/                # Temporary working files
├── config/                  # Configuration files
├── scripts/                 # Standalone Python scripts
├── pyproject.toml           # Project metadata and dependencies
├── requirements.txt         # Core dependencies
├── requirements-dev.txt     # Development tools
├── requirements-notebooks.txt # Jupyter dependencies
└── environment.yml          # Conda environment file
```

## Setup

### Option 1: Using Conda (Recommended)

```bash
# Create conda environment
conda env create -f environment.yml

# Activate environment
conda activate etl

# Verify installation
python -c "import etl; print(etl.__version__)"
```

### Option 2: Using venv (Python Virtual Environment)

```bash
# Create virtual environment
python -m venv venv

# Activate environment
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows

# Install dependencies
pip install -r requirements.txt
pip install -e .  # Install package in development mode
```

### Option 3: Using pip with standard Python

```bash
# Install in your current Python environment
pip install -r requirements.txt
pip install -e .
```

## Development Setup

To set up the development environment with testing and linting tools:

```bash
# After activating your environment
pip install -r requirements-dev.txt
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=src/etl

# Run specific test file
pytest tests/test_utils.py
```

### Code Quality

```bash
# Format code
black src/ tests/

# Check code style
flake8 src/ tests/

# Sort imports
isort src/ tests/

# Type checking
mypy src/etl/
```

## Jupyter Notebooks

To use Jupyter notebooks for exploration and analysis:

```bash
# Install notebook dependencies
pip install -r requirements-notebooks.txt

# Launch JupyterLab
jupyter lab

# Or use classic Jupyter
jupyter notebook
```

See [notebooks/README.md](notebooks/README.md) for guidelines on using notebooks.

## Configuration

Configuration files are stored in the `config/` directory. The default configuration is loaded from `config/default_config.yaml`.

To use custom configuration:

```python
from etl.config import Config

# Load custom configuration
custom_config = Config.load_yaml("custom_config.yaml")
```

## Project Paths

The ETL package automatically sets up the following paths:

- `PROJECT_ROOT`: Root directory of the project
- `DATA_DIR`: `data/` directory for all data storage
- `CONFIG_DIR`: `config/` directory for configuration files
- `SOURCE_DATA`: `data/sources/` for input PDFs and metadata
- `ANALYTICS_DATA`: `data/analytics/` for analysis results
- `EXPORTS_DATA`: `data/exports/` for final outputs

## Logging

Logs are automatically saved to `data/logs/`. Log level can be controlled via the `LOG_LEVEL` environment variable:

```bash
LOG_LEVEL=DEBUG python your_script.py
```

## Adding Dependencies

### Core Dependencies

Edit `requirements.txt` and reinstall:

```bash
pip install -r requirements.txt -e .
```

### Development Dependencies

Edit `requirements-dev.txt` and install:

```bash
pip install -r requirements-dev.txt
```

### Conda Dependencies

Edit `environment.yml` and recreate:

```bash
conda env update -f environment.yml --prune
```

## Data Storage

All data should be stored in the `data/` directory structure. Note that most data files are gitignored and should be stored in B2 buckets or external storage.

## License

MIT License - See LICENSE file for details
