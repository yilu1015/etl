# ETL Configuration

This directory contains configuration files for the ETL pipeline.

## Configuration Files

### pipeline_config.json

Default PaddleOCR configuration including:

- Model paths and versions
- Language settings
- Processing parameters (batch size, device, etc.)

### predict_params.json

Default prediction parameters for OCR inference:

- Confidence thresholds
- Output formats
- Detection parameters

### etl_workflows.yaml

Workflow orchestration and scheduling configuration (future use)

## Usage

Configuration files are loaded by the respective ETL scripts. You can override defaults by:

1. Modifying these files directly
2. Passing command-line arguments to scripts
3. Setting environment variables

## Best Practices

- Keep sensitive values (API keys, credentials) out of these files
- Use environment variables or separate `.env` files for secrets
- Version control these files but exclude environment-specific overrides
