flowchart TD
    A[Start: Input URL or CSV of URLs] --> B[Normalize URL(s) and check robots.txt]
    B --> C{Fetch homepage HTML}
    C -->|200 OK| D[Parse DOM, strip boilerplate, extract visible text]
    C -->|Error/timeout| Z[Record error, continue to next URL]
    D --> E[Discover candidate links on same domain
        - about, team, services, providers, locations]
    E --> F[Rank & dedupe; take top N (default 4)]
    F --> G[Fetch each candidate page (concurrent)]
    G --> H[Clean & compress text; collect JSON-LD if present]
    H --> I[Assemble model input: list of {url, text, jsonld}]
    I --> J[LLM extraction (Gemini 2.5 Pro JSON schema)]
    J --> K[Validate with Pydantic; fill unknowns if missing]
    K --> L[Emit JSON to stdout + write JSONL/CSV]
    L --> M[Optionally run across CSV list; aggregate outputs]
    M --> N[Done]
