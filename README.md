## JotPsych — Clinic Intelligence Scraper

Extract key clinic metadata from a website (single URL or CSV batch) using a polite crawler and an LLM with a strict JSON schema.

Output schema:
```json
{
  "clinic_info": {
    "specialty": "string",
    "modalities": "string",
    "location": "string",
    "clinic_size": "string"
  }
}
```

### What it does
- Crawls same-domain pages (About, Team/Providers/Physicians, Services/Specialties, Locations/Contact, etc.)
- Cleans visible text and reads JSON‑LD if present
- Builds lightweight “evidence” (candidate locations, provider name/count hints, specialty/modality tokens)
- Calls an LLM (Gemini 2.5 Pro by default) with a strict response schema and temperature=0
- Iteratively expands the crawl if any field is still unknown; optional final exhaustive crawl fallback
- Writes JSONL/JSON/CSV with pretty defaults for readability

---

## Requirements
- Python 3.10+
- Gemini API key exported as `GEMINI_API_KEY` (or `GOOGLE_API_KEY`)
- Recommended: a `.env` file (auto‑loaded if `python-dotenv` is installed)

Install deps:
```bash
pip install -r requirements.txt
```

`.env` example:
```bash
GEMINI_API_KEY=your_key_here
```

---

## Quick start

### Interactive (recommended)
Run without arguments and follow the prompts:
```bash
python jotpsych_scraper.py
```

### One URL
```bash
python jotpsych_scraper.py \
  --url https://exampleclinic.com \
  --provider gemini \
  --out results.jsonl
```

### CSV batch (CSV must contain a `url` column)
```bash
python jotpsych_scraper.py \
  --input_csv example_clinics.csv \
  --provider gemini \
  --out results.jsonl
```

---

## Output formats
- `.jsonl` (default): pretty JSON blocks by default; add `--compact` for one‑line records
- `.json`: pretty JSON (single object for one URL, or list for many)
- `.csv`: flattened `clinic_info` fields into columns

Example pretty block (JSONL):
```json
{
  "clinic_info": {
    "specialty": "",
    "modalities": "",
    "location": "",
    "clinic_size": ""
  }
}
```

---

## Flags you’ll actually use
- `--provider`: `gemini` (default), `openai`, or `anthropic`
- `--max_pages` (default 20): initial page budget
- `--max_depth` (default 2): initial crawl depth
- `--no_exhaust`: disable iterative expansion when unknowns remain
- `--max_total_pages` (default 120): cap when expanding pages
- `--max_total_depth` (default 3): cap when expanding depth
- `--exhaust_all_if_unknown`: if unknowns remain, crawl all same‑domain HTML pages up to a high safety cap
- `--pretty` / `--compact`: formatting control (JSON/JSONL)
- `--out`: choose `.jsonl`, `.json`, or `.csv`

---

## How it works
1) Discovery (same‑domain BFS):
   - Starts at the homepage; ranks links with strong priors (About, Team/Providers/Physicians, Services/Specialties, Locations/Contact, Directions/Map/Address)
   - Skips non‑HTML assets and off‑domain links
2) Page processing:
   - Visible text extraction (drops scripts/styles/nav/footers/cookie banners) and JSON‑LD parsing
   - Each page becomes `{url, text, jsonld}`
3) Evidence builder:
   - Candidate locations from JSON‑LD and City, ST patterns in text
   - Provider name/credential patterns and numeric hints (e.g., “team of 12 clinicians”)
   - Specialty/modality tokens from text/JSON‑LD
4) LLM extraction:
   - Gemini 2.5 Pro with a strict schema, temperature=0, system prompt from `AI_PROMPT.md`
5) Unknowns → expand:
   - Increases pages (then depth) within caps; optionally runs an exhaustive same‑domain crawl
6) Output:
   - JSONL/JSON/CSV written to `--out`; last result printed to stdout for 1‑URL runs

A flow diagram is available in `diagram.md`.

---

## Troubleshooting
- “Missing GEMINI_API_KEY …”: export the key or add to `.env`
- Output is one line per record: add `--pretty` or use `.json` output, or omit `--compact`
- No results: site blocks bots or is client‑rendered; try increasing `--max_pages/--max_depth` or consider a headless fetch strategy
- Still “unknown” fields: use `--exhaust_all_if_unknown` or run with a larger `--max_total_pages`/`--max_total_depth`

---

## Repo layout
```
.
├─ jotpsych_scraper.py     # Main CLI/crawler/LLM
├─ AI_PROMPT.md            # System prompt and extraction rules
├─ example_clinics.csv     # Example CSV for batch runs
├─ requirements.txt        # Dependencies
├─ diagram.md              # Flow diagram (view on GitHub)
└─ README.md               # This file
```

---

## Notes
- The default path uses Gemini. OpenAI/Anthropic adapters are present in code and can be enabled via `--provider` if their SDKs and keys are configured.
- The model is instructed to return “unknown” when evidence is insufficient. Iterative expansion and exhaustive mode help reduce unknowns.