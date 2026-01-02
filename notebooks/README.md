# Jupyter Notebooks

This directory contains Jupyter notebooks for pipeline development, analysis, and validation.

## Notebook Organization

### reference/ (Version Controlled)

Keeper notebooks that document important analyses and workflows:

- **pipeline_overview.ipynb** - High-level overview of the ETL pipeline architecture
- **validate_ocr_output.ipynb** - Methods for validating OCR output quality
- **error_analysis.ipynb** - Analysis of pipeline errors and edge cases

### exploration/ (Git Ignored)

Experimental and development notebooks for:

- Testing batch logic
- Visualizing batches and results
- Prototyping new features
- Ad-hoc data exploration

Note: The `exploration/` directory is excluded from version control by default. Use it freely for experimental work.

## Best Practices

1. Keep reference notebooks clean and well-documented
2. Use exploration notebooks for testing and development
3. Move validated explorations to reference/ with clear documentation
4. Include markdown cells explaining methodology
5. Avoid committing large data or model files
