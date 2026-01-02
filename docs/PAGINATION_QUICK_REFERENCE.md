# Quick Reference: OCR Pagination Detection

## The Challenge

OCR engines sometimes miss page number blocks, especially on pages with other content (like TOC title "目录"). Without inference, this leads to missing the first page of the table of contents.

## The Solution

**If keyword found on page X but first number detected on page X+N with value Y:**

```
inferred_page = Y - N
confidence = 0.85 (reduced from 1.0)
flag = "missing_page_number_on_keyword_page"
```

## Example: dwgz1992

| Event | PDF Page | Real Page | Status |
|-------|----------|-----------|--------|
| "目录" keyword | 7 | - | ✓ Detected |
| Page number | 7 | ? | ✗ Missing |
| First detected | 9 | 2 | ✓ Found |
| **Inferred** | **7** | **1** | ✓ Calculated |

**Result**: TOC = pages 1–27 (not 2–27) ✓

## Key Metrics

### Body Detection

- Method: Continuous sequence of detected page numbers
- Confidence: 0.40×length + 0.30×offset_consistency + 0.20×position + 0.10×detection
- Max gap: 5 (tolerance for missing detections)

### TOC Detection  

- Methods: Different-offset / Same-offset keyword / Inference
- Confidence: 0.95–1.0 (reduced to 0.85–0.98 if inferred)
- Signals: Keyword position + number sequence analysis

## Results Summary

```
Total Documents: 7
Body Success: 7/7 (100%)
TOC Success: 7/7 (100%)
Inference Used: 1/7 (dwgz1992)

Avg Body Confidence: 100.0%
Avg TOC Confidence: 98.4%
```

## Files to Know

| File | Purpose |
|------|---------|
| `notebooks/exploration/segment_pdfs_guide.ipynb` | Full implementation + examples |
| `data/analytics/job_metadata/2026-01-02_00-33-11_pagination.json` | Complete metadata for all 7 documents |
| `docs/OCR_PAGINATION_INFERENCE.md` | Detailed problem + solution documentation |
| `docs/PAGINATION_RESULTS.md` | Final results and integration guide |

## Key Data Fields

### For Body

- `first_real_page`: Starting real page number (1, 2, 3, ...)
- `last_real_page`: Ending real page number  
- `pdf_page_offset`: Conversion factor (real = pdf + offset)
- `confidence`: 0-1 confidence score

### For TOC  

- `first_real_page`: TOC start (usually 1)
- `last_real_page`: TOC end (varies)
- `start_detection_method`: "page_number_detected" or "keyword_inferred"
- `start_confidence`: 1.0 (detected) or 0.85 (inferred)
- `inferred_flags`: List of reasons if inferred (e.g., "missing_page_number_on_keyword_page_with_different_offset")

## Integration Example

```python
import json

# Load metadata
with open("data/analytics/job_metadata/2026-01-02_00-33-11_pagination.json") as f:
    metadata = json.load(f)

# Get dwgz1992
doc = next(d for d in metadata['documents'] if d['citekey'] == 'dwgz1992')

# Extract TOC bounds
toc = doc['toc_mapping']
toc_pdf_start = toc['first_real_page'] - toc['pdf_page_offset']  # 7
toc_pdf_end = toc['last_real_page'] - toc['pdf_page_offset']     # 33

# Check if inferred
if toc['inferred_flags']:
    print(f"⚠️  Inferred: {toc['inferred_flags']}")
    print(f"Manual review recommended for first page")
else:
    print(f"✓ Detected with 100% confidence")
```

## FAQ

**Q: Why is dwgz1992 TOC confidence 98% instead of 100%?**
A: Because page 1 of the TOC is inferred (no number detected on keyword page). Confidence is reduced to signal that manual review is recommended.

**Q: What if keyword isn't found?**
A: Algorithm falls back to strategy A (different-offset pagination). If that fails, no TOC is detected.

**Q: Can inferred pages be wrong?**
A: Unlikely if offset is consistent. Keyword detection is actually more reliable than number detection, and the math is simple. But explicitly flagged for manual verification.

**Q: How to use pagination metadata?**
A: Convert between real page numbers (what users see) and PDF page numbers (file order) using offset. Use confidence to prioritize high-confidence documents in ETL pipeline.

**Q: What does "max_gap=5" mean?**
A: Algorithm allows 5 consecutive missing page number detections before breaking a sequence. Handles OCR errors while detecting real page breaks.

---

For detailed explanation, see `docs/OCR_PAGINATION_INFERENCE.md`
