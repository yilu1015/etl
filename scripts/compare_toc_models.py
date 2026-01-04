"""
Compare TOC extraction results across multiple models and generate consensus.

This step:
1. Validates that all models succeeded for each citekey
2. Compares extractions across models using multiple metrics
3. Generates consensus extractions with confidence scores
4. Flags quality issues and conflicts

Usage:
    # Compare all citekeys from extraction job
    python scripts/run_compare_toc_models.py \
        --source-job-id 2026-01-03_16-54-45

    # Compare specific citekeys
    python scripts/run_compare_toc_models.py \
        --source-job-id 2026-01-03_16-54-45 \
        --citekeys dagz_v02 jgyl2011_v01

    # Require minimum models (skip if fewer succeeded)
    python scripts/run_compare_toc_models.py \
        --source-job-id 2026-01-03_16-54-45 \
        --min-models 3

Pipeline:
    extract_toc_titles → compare_toc_models → segment_documents
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict, Counter
from difflib import SequenceMatcher
import yaml

sys.path.insert(0, str(Path(__file__).parent))
from etl_metadata import (
    save_step_metadata,
    validate_job_id,
    ANALYTICS_ROOT
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

STEP_NAME = "compare_toc_models"
OUTPUT_DIR = ANALYTICS_ROOT / STEP_NAME


def load_extraction_results(citekey: str, source_job_id: str) -> Tuple[Dict[str, Dict], List[str]]:
    """
    Load all model extraction results for a citekey.
    
    Returns:
        (model_results_dict, failed_models_list)
    """
    extraction_dir = ANALYTICS_ROOT / "extract_toc_titles" / citekey / source_job_id
    
    if not extraction_dir.exists():
        logger.error(f"  Extraction directory not found: {extraction_dir}")
        return {}, []
    
    model_results = {}
    failed_models = []
    
    # Load successful extractions
    for result_file in extraction_dir.glob(f"{citekey}_*.json"):
        if "error" in result_file.name:
            continue
        
        model_key = result_file.stem.replace(f"{citekey}_", "")
        
        try:
            with result_file.open("r") as f:
                data = json.load(f)
            
            if data.get("status") == "success":
                model_results[model_key] = data
            else:
                failed_models.append(model_key)
        except Exception as e:
            logger.error(f"  Failed to load {result_file.name}: {e}")
            failed_models.append(model_key)
    
    # Check for error files
    error_dir = extraction_dir / "errors"
    if error_dir.exists():
        for error_file in error_dir.glob(f"{citekey}_*_error.json"):
            model_key = error_file.stem.replace(f"{citekey}_", "").replace("_error", "")
            if model_key not in failed_models:
                failed_models.append(model_key)
    
    return model_results, failed_models


def normalize_text(text: str) -> str:
    """Normalize text for comparison (remove whitespace, punctuation variations)."""
    import re
    # Remove all whitespace
    text = re.sub(r'\s+', '', text)
    # Normalize quotes
    text = text.replace('「', '').replace('」', '').replace('『', '').replace('』', '')
    text = text.replace('"', '').replace('"', '').replace(''', '').replace(''', '')
    # Normalize brackets
    text = text.replace('（', '(').replace('）', ')').replace('【', '[').replace('】', ']')
    return text.lower()


def compute_text_similarity(text1: str, text2: str) -> float:
    """Compute similarity between two texts (0.0 to 1.0)."""
    norm1 = normalize_text(text1)
    norm2 = normalize_text(text2)
    
    if not norm1 or not norm2:
        return 0.0
    
    return SequenceMatcher(None, norm1, norm2).ratio()


def find_matching_entries(
    entry: Dict,
    candidates: List[Dict],
    title_threshold: float = 0.85,
    page_tolerance: int = 2
) -> List[Tuple[int, Dict, float]]:
    """
    Find entries in candidates that match the given entry.
    
    Returns:
        List of (index, candidate, similarity_score)
    """
    matches = []
    entry_title = entry["extraction_text"]
    entry_page = entry["attributes"].get("page")
    
    for idx, candidate in enumerate(candidates):
        cand_title = candidate["extraction_text"]
        cand_page = candidate["attributes"].get("page")
        
        # Compute title similarity
        title_sim = compute_text_similarity(entry_title, cand_title)
        
        if title_sim < title_threshold:
            continue
        
        # Check page proximity (if both have pages)
        page_match = True
        if entry_page and cand_page:
            try:
                page_diff = abs(int(entry_page) - int(cand_page))
                if page_diff > page_tolerance:
                    page_match = False
            except (ValueError, TypeError):
                pass  # One or both pages are invalid
        
        if page_match:
            matches.append((idx, candidate, title_sim))
    
    # Sort by similarity
    matches.sort(key=lambda x: x[2], reverse=True)
    return matches


# Replace vote_on_field function (around line 143)

def vote_on_field(values: List[any], field_name: str) -> Tuple[any, float]:
    """
    Vote on field value across models.
    
    Returns:
        (consensus_value, confidence_score)
    """
    if not values:
        return None, 0.0
    
    # Filter out None/null values
    valid_values = [v for v in values if v is not None and v != "null" and v != ""]
    
    if not valid_values:
        return None, 0.0
    
    # Determine field type from first valid value
    first_value = valid_values[0]
    
    # For page numbers (integers or numeric strings)
    if field_name == "page" or isinstance(first_value, int):
        # Convert all to integers
        int_values = []
        for v in valid_values:
            try:
                int_values.append(int(v))
            except (ValueError, TypeError):
                pass  # Skip invalid values
        
        if not int_values:
            return None, 0.0
        
        counter = Counter(int_values)
        most_common, count = counter.most_common(1)[0]
        
        # Check if values are close (within tolerance)
        if max(int_values) - min(int_values) <= 2:
            consensus = int(sum(int_values) / len(int_values))
            confidence = 0.8  # High confidence for close values
        else:
            consensus = most_common
            confidence = count / len(int_values)
        
        # Return as string to match original format
        return str(consensus), confidence
    
    # For strings (titles, dates)
    elif isinstance(first_value, str):
        # Normalize for comparison
        normalized = [normalize_text(v) for v in valid_values]
        counter = Counter(normalized)
        most_common_norm, count = counter.most_common(1)[0]
        
        # Find original value
        consensus = valid_values[0]
        for i, norm in enumerate(normalized):
            if norm == most_common_norm:
                consensus = valid_values[i]
                break
        
        confidence = count / len(valid_values)
        return consensus, confidence
    
    # For lists (authors, references), merge unique items
    elif isinstance(first_value, list):
        all_items = []
        for v in valid_values:
            if isinstance(v, list):
                all_items.extend(v)
        
        if not all_items:
            return [], 0.0
        
        # Count occurrences
        counter = Counter(all_items)
        
        # Include items that appear in >50% of models
        threshold = len(valid_values) * 0.5
        consensus = [item for item, count in counter.items() if count >= threshold]
        
        confidence = len(consensus) / len(set(all_items)) if all_items else 0.0
        
        return consensus, confidence
    
    # Default: most common
    counter = Counter([str(v) for v in valid_values])
    consensus, count = counter.most_common(1)[0]
    confidence = count / len(valid_values)
    
    return consensus, confidence

def generate_consensus(
    model_results: Dict[str, Dict],
    title_threshold: float = 0.85
) -> List[Dict]:
    """
    Generate consensus extractions from multiple models.
    
    Returns:
        List of consensus entries with confidence scores
    """
    if not model_results:
        return []
    
    # Extract all entries from each model
    model_entries = {}
    for model_key, result in model_results.items():
        entries = result.get("extractions", {}).get("entries", [])
        model_entries[model_key] = entries
    
    # Start with entries from first model as base
    base_model = list(model_results.keys())[0]
    base_entries = model_entries[base_model]
    
    consensus_entries = []
    used_indices = {model: set() for model in model_entries.keys()}
    
    for base_entry in base_entries:
        # Find matching entries in other models
        entry_votes = {
            "title": [base_entry["extraction_text"]],
            "date": [base_entry["attributes"].get("date")],
            "page": [base_entry["attributes"].get("page")],
            "references": [base_entry["attributes"].get("references", [])],
            "authors": [base_entry["attributes"].get("authors", [])]
        }
        
        match_info = {base_model: {"similarity": 1.0, "entry": base_entry}}
        
        for model_key, entries in model_entries.items():
            if model_key == base_model:
                continue
            
            # Find best match
            matches = find_matching_entries(
                base_entry,
                [e for i, e in enumerate(entries) if i not in used_indices[model_key]],
                title_threshold
            )
            
            if matches:
                idx, matched_entry, similarity = matches[0]
                
                # Mark as used
                actual_idx = [i for i, e in enumerate(entries) if i not in used_indices[model_key]][idx]
                used_indices[model_key].add(actual_idx)
                
                # Add votes
                entry_votes["title"].append(matched_entry["extraction_text"])
                entry_votes["date"].append(matched_entry["attributes"].get("date"))
                entry_votes["page"].append(matched_entry["attributes"].get("page"))
                entry_votes["references"].append(matched_entry["attributes"].get("references", []))
                entry_votes["authors"].append(matched_entry["attributes"].get("authors", []))
                
                match_info[model_key] = {"similarity": similarity, "entry": matched_entry}
            # ← FIX: Add else clause to vote "empty" when model didn't match
            else:
                # Model didn't extract this entry - vote for empty/null
                entry_votes["title"].append(None)  # Will be filtered by vote_on_field
                entry_votes["date"].append(None)
                entry_votes["page"].append(None)
                entry_votes["references"].append([])
                entry_votes["authors"].append([])
        
        # Vote on each field
        consensus_title, title_confidence = vote_on_field(entry_votes["title"], "title")
        consensus_date, date_confidence = vote_on_field(entry_votes["date"], "date")
        consensus_page, page_confidence = vote_on_field(entry_votes["page"], "page")
        consensus_refs, refs_confidence = vote_on_field(entry_votes["references"], "references")
        consensus_authors, authors_confidence = vote_on_field(entry_votes["authors"], "authors")
            
        # Compute overall confidence
        num_models_matched = len(match_info)
        match_coverage = num_models_matched / len(model_results)
        
        field_confidences = [
            title_confidence,
            date_confidence if consensus_date else 0.5,
            page_confidence if consensus_page else 0.5,
            refs_confidence if consensus_refs else 1.0,
            authors_confidence if consensus_authors else 1.0
        ]
        avg_field_confidence = sum(field_confidences) / len(field_confidences)
        
        overall_confidence = (match_coverage * 0.6) + (avg_field_confidence * 0.4)
        
        consensus_entry = {
            "extraction_text": consensus_title,
            "extraction_class": "toc_entry",
            "attributes": {
                "date": consensus_date,
                "page": consensus_page,
                "references": consensus_refs or [],
                "authors": consensus_authors or []
            },
            "consensus_metadata": {
                "matched_models": list(match_info.keys()),
                "num_models": len(model_results),
                "coverage": round(match_coverage, 3),
                "confidence_scores": {
                    "title": round(title_confidence, 3),
                    "date": round(date_confidence, 3),
                    "page": round(page_confidence, 3),
                    "references": round(refs_confidence, 3),
                    "authors": round(authors_confidence, 3),
                    "overall": round(overall_confidence, 3)
                },
                "model_values": {
                    model: {
                        "title": info["entry"]["extraction_text"],
                        "date": info["entry"]["attributes"].get("date"),
                        "page": info["entry"]["attributes"].get("page"),
                        "similarity": round(info["similarity"], 3)
                    }
                    for model, info in match_info.items()
                }
            }
        }
        
        consensus_entries.append(consensus_entry)
    
    # Sort by page number
    def get_page_sort_key(entry):
        page = entry["attributes"].get("page")
        if page is None or page == "null":
            return 999999
        try:
            return int(page)
        except (ValueError, TypeError):
            return 999999
    
    consensus_entries.sort(key=get_page_sort_key)
    
    return consensus_entries


def identify_quality_issues(consensus_entries: List[Dict], model_results: Dict[str, Dict]) -> Dict:
    """Identify quality issues in consensus extractions."""
    issues = {
        "low_confidence_entries": [],
        "missing_fields": [],
        "conflicting_values": [],
        "unique_to_model": defaultdict(list)
    }
    
    for idx, entry in enumerate(consensus_entries):
        conf_meta = entry["consensus_metadata"]
        conf_scores = conf_meta["confidence_scores"]
        
        # Low overall confidence
        if conf_scores["overall"] < 0.7:
            issues["low_confidence_entries"].append({
                "index": idx,
                "title": entry["extraction_text"][:50],
                "confidence": conf_scores["overall"],
                "matched_models": len(conf_meta["matched_models"])
            })
        
        # Missing critical fields
        missing = []
        if not entry["attributes"].get("date"):
            missing.append("date")
        if not entry["attributes"].get("page"):
            missing.append("page")
        
        if missing:
            issues["missing_fields"].append({
                "index": idx,
                "title": entry["extraction_text"][:50],
                "missing": missing
            })
        
        # Conflicting values (low field confidence)
        conflicts = []
        for field, score in conf_scores.items():
            if field == "overall":
                continue
            if score < 0.6:
                conflicts.append(field)
        
        if conflicts:
            issues["conflicting_values"].append({
                "index": idx,
                "title": entry["extraction_text"][:50],
                "fields": conflicts,
                "model_values": conf_meta["model_values"]
            })
    
    # Find entries unique to specific models (not in consensus)
    for model_key, result in model_results.items():
        entries = result.get("extractions", {}).get("entries", [])
        
        for entry in entries:
            # Check if in consensus
            found = False
            for cons_entry in consensus_entries:
                if compute_text_similarity(
                    entry["extraction_text"],
                    cons_entry["extraction_text"]
                ) > 0.85:
                    found = True
                    break
            
            if not found:
                issues["unique_to_model"][model_key].append({
                    "title": entry["extraction_text"][:50],
                    "page": entry["attributes"].get("page")
                })
    
    return issues


def compare_citekey(
    citekey: str,
    source_job_id: str,
    min_models: int,
    job_id: str
) -> Dict:
    """Compare extraction results for one citekey across all models."""
    logger.info(f"\n{'='*70}")
    logger.info(f"Comparing: {citekey}")
    logger.info('='*70)
    
    # Load all model results
    model_results, failed_models = load_extraction_results(citekey, source_job_id)
    
    if failed_models:
        logger.warning(f"  Failed models: {failed_models}")
    
    num_models = len(model_results)
    logger.info(f"  Successful models: {num_models}/{num_models + len(failed_models)}")
    
    # Check minimum models requirement
    if num_models < min_models:
        logger.error(f"  Insufficient models ({num_models} < {min_models} required)")
        return {
            "citekey": citekey,
            "status": "insufficient_models",
            "num_models": num_models,
            "required_models": min_models,
            "failed_models": failed_models
        }
    
    # Log extraction counts per model
    for model_key, result in model_results.items():
        count = result.get("extractions", {}).get("count", 0)
        logger.info(f"    {model_key:20} {count} entries")
    
    # Generate consensus
    logger.info(f"  Generating consensus...")
    consensus_entries = generate_consensus(model_results)
    logger.info(f"  Consensus: {len(consensus_entries)} entries")
    
    # Identify quality issues
    quality_issues = identify_quality_issues(consensus_entries, model_results)
    
    num_issues = (
        len(quality_issues["low_confidence_entries"]) +
        len(quality_issues["missing_fields"]) +
        len(quality_issues["conflicting_values"]) +
        sum(len(v) for v in quality_issues["unique_to_model"].values())
    )
    
    if num_issues > 0:
        logger.warning(f"  ⚠️  Found {num_issues} quality issues")
    else:
        logger.info(f"  ✓ No quality issues detected")
    
    # Create output directory
    output_dir = OUTPUT_DIR / citekey / job_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save consensus
    consensus_data = {
        "citekey": citekey,
        "source_job_id": source_job_id,
        "compare_toc_models_job_id": job_id,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "models": {
            "successful": list(model_results.keys()),
            "failed": failed_models,
            "count": num_models
        },
        "consensus": {
            "count": len(consensus_entries),
            "entries": consensus_entries
        },
        "quality_issues": quality_issues,
        "status": "success"
    }
    
    consensus_file = output_dir / f"{citekey}_consensus.json"
    with consensus_file.open("w", encoding="utf-8") as f:
        json.dump(consensus_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"  Saved consensus to {consensus_file.name}")
    
    # Update symlink
    latest_link = OUTPUT_DIR / citekey / "latest"
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    latest_link.symlink_to(job_id, target_is_directory=True)
    
    return {
        "citekey": citekey,
        "status": "success",
        "num_models": num_models,
        "consensus_count": len(consensus_entries),
        "num_issues": num_issues,
        "failed_models": failed_models
    }


def main():
    parser = argparse.ArgumentParser(
        description="Compare TOC extraction results across models",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--source-job-id",
        required=True,
        help="extract_toc_titles job ID"
    )
    parser.add_argument(
        "--citekeys",
        nargs="+",
        help="Citekeys to compare (default: all from source job)"
    )
    parser.add_argument(
        "--min-models",
        type=int,
        default=2,
        help="Minimum successful models required (default: 2)"
    )
    
    args = parser.parse_args()
    
    source_job_id = validate_job_id(args.source_job_id, "--source-job-id")
    
    # Load source job metadata
    source_metadata_path = ANALYTICS_ROOT / "extract_toc_titles" / "job_metadata" / f"{source_job_id}.json"
    
    if not source_metadata_path.exists():
        logger.error(f"Source job not found: {source_job_id}")
        sys.exit(1)
    
    with source_metadata_path.open("r") as f:
        source_metadata = json.load(f)
    
    # Get citekeys
    if args.citekeys:
        citekeys = args.citekeys
    else:
        citekeys = source_metadata.get("citekeys", {}).get("list", [])
        failed_citekeys = source_metadata.get("citekeys", {}).get("failed_list", [])
        citekeys = [ck for ck in citekeys if ck not in failed_citekeys]
    
    if not citekeys:
        logger.error("No citekeys to compare")
        sys.exit(1)
    
    job_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    logger.info(f"\n{'#'*70}")
    logger.info(f"COMPARE TOC MODELS JOB: {job_id}")
    logger.info(f"Source: extract_toc_titles/{source_job_id}")
    logger.info(f"Citekeys: {len(citekeys)}")
    logger.info(f"Minimum models: {args.min_models}")
    logger.info('#'*70)
    
    # Compare all citekeys
    results = []
    for citekey in citekeys:
        result = compare_citekey(citekey, source_job_id, args.min_models, job_id)
        results.append(result)
    
    # Compute statistics
    total_citekeys = len(results)
    successful = sum(1 for r in results if r["status"] == "success")
    insufficient = sum(1 for r in results if r["status"] == "insufficient_models")
    
    total_issues = sum(r.get("num_issues", 0) for r in results)
    
    # Build metadata
    metadata = {
        "status": "completed",
        "source_job_id": source_job_id,
        "min_models_required": args.min_models,
        "citekeys": {
            "total": total_citekeys,
            "successful": successful,
            "insufficient_models": insufficient,
            "list": citekeys
        },
        "quality_summary": {
            "total_issues": total_issues,
            "citekeys_with_issues": sum(1 for r in results if r.get("num_issues", 0) > 0)
        },
        "results": results
    }
    
    save_step_metadata(STEP_NAME, job_id, metadata, OUTPUT_DIR)
    
    # Print summary
    logger.info(f"\n{'#'*70}")
    logger.info("COMPARISON COMPLETE")
    logger.info('#'*70)
    logger.info(f"Job ID: {job_id}")
    logger.info(f"Citekeys: {successful}/{total_citekeys} successful")
    logger.info(f"Quality issues: {total_issues} across {metadata['quality_summary']['citekeys_with_issues']} citekeys")
    
    if insufficient > 0:
        logger.warning(f"\n{insufficient} citekeys had insufficient models")
    
    logger.info('#'*70)


if __name__ == "__main__":
    main()