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

## Development Process

This project was architected through collaborative sessions with GPT and Claude, iteratively refining the approach to clinic website intelligence extraction. The development process involved several key phases:

### 1. Initial Architecture & Design
- **Problem Definition**: Need to extract structured clinic metadata (specialty, modalities, location, size) from diverse clinic websites
- **AI Collaboration**: Worked with GPT and Claude to design a multi-stage pipeline combining web crawling, content extraction, and LLM-based intelligence
- **Schema Design**: Defined a clean, minimal output schema focusing on the most valuable clinic attributes

### 2. Technical Implementation Strategy
- **Polite Crawling**: Implemented respectful web scraping with robots.txt compliance, rate limiting, and proper user agents
- **Content Discovery**: Developed intelligent link ranking system to prioritize relevant pages (About, Team, Services, Locations)
- **Evidence Building**: Created lightweight preprocessing to extract candidate locations, provider hints, and specialty tokens
- **LLM Integration**: Integrated multiple providers (Gemini, OpenAI, Anthropic) with strict JSON schema enforcement

### 3. Iterative Refinement
- **Unknown Handling**: Implemented iterative expansion when initial crawl yields insufficient data
- **Exhaustive Fallback**: Added option for comprehensive same-domain crawling when unknowns persist
- **Output Flexibility**: Built support for multiple output formats (JSONL, JSON, CSV) with pretty formatting
- **Error Resilience**: Added robust error handling for network issues, parsing failures, and API limits

## How the Code Works

The system follows a sophisticated multi-stage pipeline as illustrated in the flow diagram (`diagram.md`):

### Stage 1: URL Normalization & Discovery
```python
# Normalize input URLs and check robots.txt compliance
urls = normalize_urls(input_urls)
robots_allowed = check_robots_txt(urls)
```

### Stage 2: Intelligent Page Discovery
The crawler uses a priority-based ranking system to discover relevant pages:
- **High Priority**: About, Team/Providers, Services/Specialties, Locations/Contact
- **Medium Priority**: Treatments, Conditions, Office information
- **Filtering**: Excludes non-HTML assets, off-domain links, and irrelevant content

### Stage 3: Content Extraction & Cleaning
```python
# Extract visible text, removing boilerplate
visible_text = extract_visible_text(html)
json_ld = parse_json_ld(html)
page_data = {"url": url, "text": visible_text, "jsonld": json_ld}
```

### Stage 4: Evidence Building
The system preprocesses content to build lightweight evidence:
- **Location Detection**: Extracts city/state patterns and JSON-LD address data
- **Provider Hints**: Identifies provider names, credentials, and count indicators
- **Specialty Tokens**: Collects clinical specialty and modality keywords

### Stage 5: LLM Intelligence Extraction
```python
# Send structured data to LLM with strict schema
response = llm_client.generate_content(
    system_prompt=AI_PROMPT,
    user_content=structured_evidence,
    response_schema=ClinicInfoSchema,
    temperature=0  # Deterministic output
)
```

### Stage 6: Iterative Expansion
If any fields return "unknown", the system:
1. Increases page budget (up to `max_total_pages`)
2. Increases crawl depth (up to `max_total_depth`) 
3. Optionally runs exhaustive same-domain crawl

### Stage 7: Output Generation
Results are written in multiple formats:
- **JSONL**: One JSON object per clinic (default)
- **JSON**: Single object or array of objects
- **CSV**: Flattened fields for spreadsheet analysis

## Key Technical Features

### Multi-Provider LLM Support
```python
# Supports Gemini (default), OpenAI, and Anthropic
providers = {
    "gemini": GeminiProvider,
    "openai": OpenAIProvider, 
    "anthropic": AnthropicProvider
}
```

### Robust Error Handling
- Network timeouts and retries
- Graceful degradation when pages fail to load
- Validation of LLM responses with Pydantic schemas
- Comprehensive logging of crawl progress

### Performance Optimizations
- Concurrent page fetching with configurable limits
- Intelligent caching of robots.txt and page content
- Memory-efficient processing of large websites
- Configurable rate limiting to respect server resources

## Notes
- The default path uses Gemini. OpenAI/Anthropic adapters are present in code and can be enabled via `--provider` if their SDKs and keys are configured.
- The model is instructed to return "unknown" when evidence is insufficient. Iterative expansion and exhaustive mode help reduce unknowns.