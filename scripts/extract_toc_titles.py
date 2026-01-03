"""
Extract TOC entries using multiple LLMs.

This step takes parse_structure results and extracts document titles from the
table of contents using configured language models.

Usage:
    # Process specific citekeys
    python scripts/run_extract_toc_titles.py \
        --source-job-id 2026-01-02_16-30-20 \
        --citekeys dagz_v01 dagz_v02 \
        --models gpt4o gemini2_flash

    # Process with wildcards
    python scripts/run_extract_toc_titles.py \
        --source-job-id 2026-01-02_16-30-20 \
        --citekeys 'dagz_v*' \
        --models gpt4o

    # Sequential processing (no parallelization)
    python scripts/run_extract_toc_titles.py \
        --source-job-id 2026-01-02_16-30-20 \
        --citekeys '*' \
        --no-parallel

    # Dry run to preview
    python scripts/run_extract_toc_titles.py \
        --source-job-id 2026-01-02_16-30-20 \
        --citekeys '*' \
        --dry-run

Pipeline:
    sync_ocr → parse_structure → extract_toc_titles → [compare_toc_models] → [segment_documents]
"""

import argparse
import json
import logging
import sys
import os
import re
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from fnmatch import fnmatch
from concurrent.futures import ThreadPoolExecutor, as_completed
import yaml
from tqdm import tqdm

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from etl_metadata import (
    save_step_metadata,
    validate_job_id,
    ANALYTICS_ROOT
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress verbose langextract logging
logging.getLogger('langextract').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)  # Also suppress HTTP request logs
logging.getLogger('httpcore').setLevel(logging.WARNING)

# Constants
STEP_NAME = "extract_toc_titles"
OUTPUT_DIR = ANALYTICS_ROOT / STEP_NAME
CONFIG_PATH = Path(__file__).parent.parent / "config" / "extract_docs_config.yaml"


def load_config() -> dict:
    """Load extraction configuration from YAML."""
    if not CONFIG_PATH.exists():
        logger.error(f"Config file not found: {CONFIG_PATH}")
        sys.exit(1)
    
    with CONFIG_PATH.open("r") as f:
        return yaml.safe_load(f)


def validate_and_expand_citekeys(
    patterns: List[str],
    source_job_id: str
) -> List[str]:
    """
    Validate that requested citekeys exist in source job and expand wildcards.
    
    Args:
        patterns: List of citekey patterns (e.g., ['dagz_v*', 'gwcl2003'])
        source_job_id: parse_structure job ID
    
    Returns:
        List of validated citekeys that exist in source job
    
    Raises:
        SystemExit if validation fails
    """
    # Load source job metadata
    source_metadata_path = ANALYTICS_ROOT / "parse_structure" / "job_metadata" / f"{source_job_id}.json"
    
    if not source_metadata_path.exists():
        logger.error(f"Source job metadata not found: {source_metadata_path}")
        logger.error(f"Job ID: {source_job_id}")
        sys.exit(1)
    
    with source_metadata_path.open("r") as f:
        source_metadata = json.load(f)
    
    # Get list of successfully processed citekeys from source
    all_citekeys = source_metadata.get("citekeys", {}).get("list", [])
    failed_citekeys = set(source_metadata.get("citekeys", {}).get("failed_list", []))
    available_citekeys = [ck for ck in all_citekeys if ck not in failed_citekeys]
    
    if not available_citekeys:
        logger.error(f"No successfully processed citekeys in source job {source_job_id}")
        logger.error(f"All {len(all_citekeys)} citekeys failed in parse_structure")
        sys.exit(1)
    
    logger.info(f"Source job {source_job_id} has {len(available_citekeys)} available citekeys")
    
    # Expand patterns and validate
    matched_citekeys = set()
    
    for pattern in patterns:
        if "*" in pattern or "?" in pattern:
            # Wildcard pattern
            matches = [ck for ck in available_citekeys if fnmatch(ck, pattern)]
            if not matches:
                logger.warning(f"Pattern '{pattern}' matched no citekeys in source job")
                logger.warning(f"Available citekeys: {available_citekeys}")
            else:
                logger.info(f"Pattern '{pattern}' matched {len(matches)} citekeys: {matches}")
                matched_citekeys.update(matches)
        else:
            # Exact match - validate exists in source
            if pattern in available_citekeys:
                matched_citekeys.add(pattern)
                logger.info(f"Validated citekey: {pattern}")
            else:
                logger.error(f"Citekey '{pattern}' not found in source job {source_job_id}")
                if pattern in failed_citekeys:
                    logger.error(f"  → Citekey failed in parse_structure step")
                else:
                    logger.error(f"  → Citekey not processed in source job")
                logger.error(f"Available citekeys: {available_citekeys}")
                sys.exit(1)
    
    if not matched_citekeys:
        logger.error("No citekeys matched the provided patterns")
        sys.exit(1)
    
    return sorted(matched_citekeys)


def extract_toc_text(citekey: str, source_job_id: str) -> Optional[Dict]:
    """
    Extract TOC text from parse_structure and OCR results.
    
    Args:
        citekey: Citation key
        source_job_id: parse_structure job ID
    
    Returns:
        Dict with TOC text and metadata, or None if extraction fails
    """
    # Load parse_structure result
    parse_dir = ANALYTICS_ROOT / "parse_structure" / citekey / source_job_id
    parse_file = parse_dir / f"{citekey}.json"
    
    if not parse_file.exists():
        logger.error(f"  Parse structure not found: {parse_file}")
        return None
    
    with parse_file.open("r") as f:
        parse_data = json.load(f)
    
    # Get TOC page range
    toc = parse_data.get("toc", {})
    toc_start = toc.get("first_pdf_page")
    toc_end = toc.get("last_pdf_page")
    
    if not toc_start or not toc_end:
        logger.error(f"  Invalid TOC range in parse_structure: {toc}")
        return None
    
    # Load OCR results
    ocr_job_id = parse_data.get("source_job_id")
    if not ocr_job_id:
        logger.error(f"  No OCR job ID in parse_structure")
        return None
    
    ocr_dir = ANALYTICS_ROOT / "ocr" / citekey / ocr_job_id
    ocr_file = ocr_dir / f"{citekey}.json"
    
    if not ocr_file.exists():
        logger.error(f"  OCR results not found: {ocr_file}")
        return None
    
    with ocr_file.open("r") as f:
        ocr_data = json.load(f)
    
    # Extract content from TOC pages
    toc_pages = []
    
    for page_data in ocr_data.get("pages", []):
        pdf_page = page_data.get("page")
        
        if toc_start <= pdf_page <= toc_end:
            # Extract content blocks
            content_blocks = []
            
            for block in page_data.get("json", {}).get("res", {}).get("parsing_res_list", []):
                block_label = block.get("block_label")
                
                # Include content, text, and doc_title blocks
                if block_label in ["content", "text", "doc_title"]:
                    content_blocks.append({
                        "order": block.get("block_order"),
                        "text": block.get("block_content", "")
                    })
            
            # Sort by block order and concatenate
            content_blocks.sort(key=lambda x: x["order"] if x["order"] is not None else float("inf"))
            page_content = "\n".join(b["text"] for b in content_blocks if b["text"])
            
            if page_content.strip():
                toc_pages.append({
                    "pdf_page": pdf_page,
                    "content": page_content
                })
    
    if not toc_pages:
        logger.error(f"  No content extracted from TOC pages {toc_start}-{toc_end}")
        return None
    
    # Concatenate all pages
    full_text = "\n\n".join(p["content"] for p in toc_pages)
    
    return {
        "citekey": citekey,
        "toc_text": full_text,
        "toc_page_range": f"{toc_start}-{toc_end}",
        "num_pages": len(toc_pages),
        "text_length": len(full_text),
        "source_job_id": source_job_id,
        "ocr_job_id": ocr_job_id
    }


def extract_with_model(
    toc_data: Dict,
    model_key: str,
    config: dict,
    output_dir: Path,
    job_id: str,
    pbar: Optional[tqdm] = None
) -> Dict:
    """
    Extract TOC entries using specified model.
    
    Args:
        toc_data: TOC text and metadata from extract_toc_text()
        model_key: Model configuration key (e.g., "gpt4o")
        config: Full config dict
        output_dir: Output directory for this citekey/job
        job_id: Current job ID
        pbar: Optional progress bar to update
    
    Returns:
        Result dict with status, extractions, timing, or error info
    """
    import langextract as lx
    from langextract.data import Document
    from langextract.core import tokenizer
    from langextract.providers import openai as lx_openai
    from dotenv import load_dotenv
    
    # Load environment variables
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path)
    
    citekey = toc_data["citekey"]
    start_time = time.time()
    
    try:
        # Get model config
        if model_key not in config["models"]:
            raise ValueError(f"Model '{model_key}' not found in config")
        
        model_config = config["models"][model_key]
        extraction_params = config["extraction_params"]
        
        # Merge model-specific overrides
        extraction_settings = {**extraction_params}
        if "extraction_overrides" in model_config:
            extraction_settings.update(model_config["extraction_overrides"])
        
        # Get API key
        api_key_env = model_config["api_key_env"]
        api_key = os.environ.get(api_key_env)
        
        if not api_key:
            raise ValueError(f"Missing API key: {api_key_env} not set in environment")
        
        # Prepare extraction
        document = Document(text=toc_data["toc_text"])
        prompt = config["extraction_prompt"]
        
        # Load examples
        examples = []
        for example in config["examples"]:
            example_data = lx.data.ExampleData(
                text=example["text"],
                extractions=[
                    lx.data.Extraction(
                        extraction_class=entry["extraction_class"],
                        extraction_text=entry["extraction_text"],
                        attributes=entry.get("attributes", {}),
                    )
                    for entry in example["extractions"]
                ],
            )
            examples.append(example_data)
        
        unicode_tokenizer = tokenizer.UnicodeTokenizer()
        
        # Run extraction based on provider
        extraction_start = time.time()
        
        if model_config["provider"] == "gemini":
            result = lx.extract(
                text_or_documents=[document],
                prompt_description=prompt,
                examples=examples,
                model_id=model_config["name"],
                api_key=api_key,
                tokenizer=unicode_tokenizer,
                extraction_passes=extraction_settings["extraction_passes"],
                max_workers=extraction_settings.get("max_workers", 10),
                max_char_buffer=extraction_settings["max_char_buffer"],
                batch_length=extraction_settings["batch_length"],
                debug=extraction_settings.get("debug", False),
            )
        else:
            # OpenAI-compatible API
            model_params = {
                "model_id": model_config["name"],
                "api_key": api_key,
                "temperature": model_config.get("temperature", 0.0),
            }
            
            if model_config.get("base_url"):
                model_params["base_url"] = model_config["base_url"]
            
            # Add token limit parameter
            params = model_config.get("params", {})
            if params:
                for param_key, param_value in params.items():
                    if any(alias in param_key.lower() for alias in ["max", "token", "completion", "output"]):
                        model_params[param_key] = param_value
                        break
            
            model = lx_openai.OpenAILanguageModel(**model_params)
            
            result = lx.extract(
                model=model,
                text_or_documents=[document],
                prompt_description=prompt,
                examples=examples,
                tokenizer=unicode_tokenizer,
                extraction_passes=extraction_settings["extraction_passes"],
                max_char_buffer=extraction_settings["max_char_buffer"],
                batch_length=extraction_settings["batch_length"],
                debug=extraction_settings.get("debug", False),
                fence_output=extraction_settings.get("fence_output", False),
                use_schema_constraints=extraction_settings.get("use_schema_constraints", False),
            )
        
        extraction_time = time.time() - extraction_start
        
        # Validate result
        if result is None:
            raise ValueError("Model returned None - API call may have failed")
        
        result_list = list(result)
        if not result_list:
            raise ValueError("Model returned empty results")
        
        annotated_doc = result_list[0]
        
        # Sanitize extractions
        def sanitize_json_string(s: str) -> str:
            """Remove or escape control characters that break JSON."""
            s = re.sub(r'[\x00-\x08\x0b-\x0c\x0e-\x1f\x7f]', '', s)
            s = s.replace('\\', '\\\\')
            s = s.replace('"', '\\"')
            s = s.replace('\n', '\\n')
            s = s.replace('\t', '\\t')
            s = s.replace('\r', '\\r')
            return s
        
        extractions_data = []
        for extraction in annotated_doc.extractions:
            attrs = getattr(extraction, "attributes", {})
            
            # Format list fields
            for list_field in ["authors", "references"]:
                if list_field in attrs and attrs[list_field]:
                    if isinstance(attrs[list_field], str):
                        attrs[list_field] = [
                            item.strip()
                            for item in attrs[list_field].split(",")
                            if item.strip()
                        ]
                    elif not isinstance(attrs[list_field], list):
                        attrs[list_field] = [str(attrs[list_field])]
                else:
                    attrs[list_field] = []
            
            # Sanitize all strings
            extractions_data.append({
                "extraction_text": sanitize_json_string(extraction.extraction_text),
                "extraction_class": extraction.extraction_class,
                "start_char_idx": getattr(extraction, "start_char_idx", None),
                "end_char_idx": getattr(extraction, "end_char_idx", None),
                "attributes": {
                    k: (
                        sanitize_json_string(v) if isinstance(v, str) else
                        [sanitize_json_string(i) if isinstance(i, str) else i for i in v]
                        if isinstance(v, list) else v
                    )
                    for k, v in attrs.items()
                }
            })
        
        total_time = time.time() - start_time
        
        # Build standardized result
        result_data = {
            "citekey": citekey,
            "source_job_id": toc_data["source_job_id"],
            "extract_toc_titles_job_id": job_id,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "model": {
                "key": model_key,
                "name": model_config["name"],
                "provider": model_config["provider"]
            },
            "extraction_settings": {
                "passes": extraction_settings["extraction_passes"],
                "max_char_buffer": extraction_settings["max_char_buffer"],
                "batch_length": extraction_settings["batch_length"]
            },
            "toc_metadata": {
                "page_range": toc_data["toc_page_range"],
                "num_pages": toc_data["num_pages"],
                "text_length": toc_data["text_length"]
            },
            "extractions": {
                "count": len(extractions_data),
                "entries": extractions_data
            },
            "timing": {
                "extraction_seconds": round(extraction_time, 2),
                "total_seconds": round(total_time, 2)
            },
            "status": "success"
        }
        
        # Save result
        result_file = output_dir / f"{citekey}_{model_key}.json"
        with result_file.open("w", encoding="utf-8") as f:
            json.dump(result_data, f, indent=2, ensure_ascii=False)
        
        # Update progress bar
        if pbar:
            pbar.set_postfix_str(f"{citekey}/{model_key}: {len(extractions_data)} entries, {total_time:.1f}s")
            pbar.update(1)
        
        return result_data
        
    except Exception as e:
        total_time = time.time() - start_time
        
        # Create errors subdirectory
        error_dir = output_dir / "errors"
        error_dir.mkdir(exist_ok=True)
        
        # Build error data
        error_data = {
            "citekey": citekey,
            "source_job_id": toc_data["source_job_id"],
            "extract_toc_titles_job_id": job_id,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "model": {
                "key": model_key,
                "name": config["models"].get(model_key, {}).get("name", "Unknown"),
                "provider": config["models"].get(model_key, {}).get("provider", "Unknown")
            },
            "error": {
                "type": type(e).__name__,
                "message": str(e)
            },
            "timing": {
                "total_seconds": round(total_time, 2)
            },
            "status": "error"
        }
        
        # Save error
        error_file = error_dir / f"{citekey}_{model_key}_error.json"
        with error_file.open("w", encoding="utf-8") as f:
            json.dump(error_data, f, indent=2, ensure_ascii=False)
        
        # Update progress bar
        if pbar:
            pbar.set_postfix_str(f"{citekey}/{model_key}: ERROR - {type(e).__name__}")
            pbar.update(1)
        
        return error_data


def process_citekey(
    citekey: str,
    source_job_id: str,
    models: List[str],
    config: dict,
    job_id: str,
    use_parallel: bool = True,
    pbar: Optional[tqdm] = None
) -> Dict:
    """
    Process one citekey with all specified models.
    
    Args:
        citekey: Citation key
        source_job_id: parse_structure job ID
        models: List of model keys to use
        config: Full config dict
        job_id: Current job ID
        use_parallel: Whether to run models in parallel
        pbar: Optional progress bar
    
    Returns:
        Summary dict with per-model results and timing
    """
    citekey_start_time = time.time()
    
    # Extract TOC text
    toc_data = extract_toc_text(citekey, source_job_id)
    if not toc_data:
        return {
            "citekey": citekey,
            "status": "error",
            "error": "Failed to extract TOC text",
            "models": {},
            "timing": {
                "total_seconds": round(time.time() - citekey_start_time, 2)
            }
        }
    
    # Create output directory
    output_dir = OUTPUT_DIR / citekey / job_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Run models (parallel or sequential)
    model_results = {}
    
    if use_parallel:
        # Parallel execution with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=len(models)) as executor:
            future_to_model = {
                executor.submit(
                    extract_with_model,
                    toc_data,
                    model_key,
                    config,
                    output_dir,
                    job_id,
                    pbar
                ): model_key
                for model_key in models
            }
            
            for future in as_completed(future_to_model):
                model_key = future_to_model[future]
                try:
                    result = future.result()
                    model_results[model_key] = {
                        "status": result["status"],
                        "extraction_count": result.get("extractions", {}).get("count", 0) if result["status"] == "success" else 0,
                        "timing": result.get("timing", {}),
                        "error": result.get("error") if result["status"] == "error" else None
                    }
                except Exception as e:
                    model_results[model_key] = {
                        "status": "error",
                        "extraction_count": 0,
                        "timing": {},
                        "error": {
                            "type": type(e).__name__,
                            "message": str(e)
                        }
                    }
    else:
        # Sequential execution
        for model_key in models:
            result = extract_with_model(toc_data, model_key, config, output_dir, job_id, pbar)
            model_results[model_key] = {
                "status": result["status"],
                "extraction_count": result.get("extractions", {}).get("count", 0) if result["status"] == "success" else 0,
                "timing": result.get("timing", {}),
                "error": result.get("error") if result["status"] == "error" else None
            }
    
    # Update symlink
    latest_link = OUTPUT_DIR / citekey / "latest"
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    latest_link.symlink_to(job_id, target_is_directory=True)
    
    # Compute summary
    successful = sum(1 for r in model_results.values() if r["status"] == "success")
    total_time = time.time() - citekey_start_time
    
    return {
        "citekey": citekey,
        "status": "completed" if successful > 0 else "error",
        "models": model_results,
        "successful_models": successful,
        "total_models": len(models),
        "timing": {
            "total_seconds": round(total_time, 2)
        }
    }


def print_dry_run_summary(citekeys: List[str], models: List[str], source_job_id: str, use_parallel: bool):
    """Print dry-run preview."""
    print(f"\n{'='*70}")
    print("DRY RUN - No files will be processed")
    print('='*70)
    print(f"\nSource job: {source_job_id}")
    print(f"Citekeys to process: {len(citekeys)}")
    for ck in citekeys:
        print(f"  - {ck}")
    print(f"\nModels to use: {len(models)}")
    for model in models:
        print(f"  - {model}")
    print(f"\nExecution mode: {'Parallel' if use_parallel else 'Sequential'}")
    print(f"Total extractions: {len(citekeys)} citekeys × {len(models)} models = {len(citekeys) * len(models)} runs")
    print('='*70)


def main():
    parser = argparse.ArgumentParser(
        description="Extract TOC document titles using multiple LLMs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process ALL citekeys from source job (default)
  python scripts/extract_toc_titles.py \\
      --source-job-id 2026-01-02_16-30-20

  # Process specific citekeys
  python scripts/extract_toc_titles.py \\
      --source-job-id 2026-01-02_16-30-20 \\
      --citekeys dagz_v01 dagz_v02 \\
      --models gpt4o gemini2_flash

  # Process with wildcards
  python scripts/extract_toc_titles.py \\
      --source-job-id 2026-01-02_16-30-20 \\
      --citekeys 'dagz_v*' \\
      --models gpt4o

  # Sequential processing
  python scripts/extract_toc_titles.py \\
      --source-job-id 2026-01-02_16-30-20 \\
      --no-parallel

  # Preview without processing
  python scripts/extract_toc_titles.py \\
      --source-job-id 2026-01-02_16-30-20 \\
      --dry-run
        """
    )
    
    parser.add_argument(
        "--source-job-id",
        required=True,
        help="parse_structure job ID to use as source"
    )
    parser.add_argument(
        "--citekeys",
        nargs="+",
        help="Citekeys to process (default: all from source job; supports wildcards like 'dagz_v*')"
    )
    parser.add_argument(
        "--models",
        nargs="+",
        help="Model keys to use (default: all models in config)"
    )
    parser.add_argument(
        "--no-parallel",
        action="store_true",
        help="Disable parallel model execution (run sequentially)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be processed without running extraction"
    )
    
    args = parser.parse_args()
    
    # Validate source job ID
    source_job_id = validate_job_id(args.source_job_id, "--source-job-id")
    
    # Load config
    config = load_config()
    
    # Validate and expand citekeys (default to ['*'] if not specified)
    citekey_patterns = args.citekeys or ['*']
    citekeys = validate_and_expand_citekeys(citekey_patterns, source_job_id)
    
    # Default to all models if not specified
    models = args.models or list(config["models"].keys())
    
    # Validate models
    for model_key in models:
        if model_key not in config["models"]:
            logger.error(f"Unknown model: {model_key}")
            logger.error(f"Available models: {list(config['models'].keys())}")
            sys.exit(1)
    
    use_parallel = not args.no_parallel
    
    # Dry run preview
    if args.dry_run:
        print_dry_run_summary(citekeys, models, source_job_id, use_parallel)
        sys.exit(0)
    
    # Generate job ID
    job_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    job_start_time = time.time()
    
    logger.info(f"\n{'#'*70}")
    logger.info(f"EXTRACT TOC TITLES JOB: {job_id}")
    logger.info(f"Source: parse_structure/{source_job_id}")
    logger.info(f"Citekeys: {len(citekeys)}")
    logger.info(f"Models: {models}")
    logger.info(f"Execution: {'Parallel' if use_parallel else 'Sequential'}")
    logger.info('#'*70)
    
    # Process all citekeys with progress bar
    total_tasks = len(citekeys) * len(models)
    results = []
    
    with tqdm(total=total_tasks, desc="Extracting TOC titles", unit="extraction") as pbar:
        for citekey in citekeys:
            result = process_citekey(
                citekey,
                source_job_id,
                models,
                config,
                job_id,
                use_parallel,
                pbar
            )
            results.append(result)
    
    # Compute statistics
    total_time = time.time() - job_start_time
    total_citekeys = len(results)
    successful_citekeys = sum(1 for r in results if r["status"] == "completed")
    failed_citekeys = total_citekeys - successful_citekeys
    
    model_stats = {}
    for model_key in models:
        successful = sum(1 for r in results if r["models"].get(model_key, {}).get("status") == "success")
        total_extractions = sum(r["models"].get(model_key, {}).get("extraction_count", 0) for r in results)
        total_model_time = sum(r["models"].get(model_key, {}).get("timing", {}).get("total_seconds", 0) for r in results)
        avg_time = total_model_time / successful if successful > 0 else 0
        
        model_stats[model_key] = {
            "successful": successful,
            "failed": total_citekeys - successful,
            "total_extractions": total_extractions,
            "total_time_seconds": round(total_model_time, 2),
            "avg_time_seconds": round(avg_time, 2)
        }
    
    # Build metadata
    metadata = {
        "status": "completed",
        "source_job_id": source_job_id,
        "execution_mode": "parallel" if use_parallel else "sequential",
        "citekeys": {
            "total": total_citekeys,
            "successful": successful_citekeys,
            "failed": failed_citekeys,
            "list": citekeys,
            "failed_list": [r["citekey"] for r in results if r["status"] != "completed"]
        },
        "models": {
            "list": models,
            "statistics": model_stats
        },
        "extraction_config": {
            "passes": config["extraction_params"]["extraction_passes"],
            "max_char_buffer": config["extraction_params"]["max_char_buffer"],
            "batch_length": config["extraction_params"]["batch_length"]
        },
        "timing": {
            "total_seconds": round(total_time, 2),
            "total_minutes": round(total_time / 60, 2)
        },
        "results": results
    }
    
    # Save metadata
    save_step_metadata(STEP_NAME, job_id, metadata, OUTPUT_DIR)
    
    # Print summary
    logger.info(f"\n{'#'*70}")
    logger.info("EXTRACTION COMPLETE")
    logger.info('#'*70)
    logger.info(f"Job ID: {job_id}")
    logger.info(f"Total time: {total_time/60:.1f} minutes")
    logger.info(f"Citekeys: {successful_citekeys}/{total_citekeys} successful")
    logger.info(f"\nModel Performance:")
    for model_key, stats in model_stats.items():
        logger.info(
            f"  {model_key:20} "
            f"{stats['successful']}/{total_citekeys} successful, "
            f"{stats['total_extractions']} extractions, "
            f"avg {stats['avg_time_seconds']:.1f}s/doc"
        )
    
    if failed_citekeys > 0:
        logger.info(f"\nFailed citekeys:")
        for r in results:
            if r["status"] != "completed":
                logger.info(f"  - {r['citekey']}: {r.get('error', 'Unknown error')}")
    
    logger.info('#'*70)


if __name__ == "__main__":
    main()