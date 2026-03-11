# Pipeline Scaffold

A 4-stage automated pipeline that reads mixed data files, synthesizes them via Claude into a memo and structured JSON, populates an Excel model, and drafts a summary email.

```
data/*.csv,.xlsx,.pdf,.txt
  → [Stage 1] Per-file summaries (map-reduce for large files)
  → [Stage 2a] Executive memo synthesis
  → [Stage 2b] Structured JSON extraction
  → [Stage 3] Excel model population + formula recalc
  → [Stage 4] Email draft
  → outputs/: timestamped Excel, email draft, pipeline.log
```

## Setup

```bash
git clone https://github.com/Nayanglxy/pipeline-scaffold.git
cd pipeline-scaffold
pip install -r requirements.txt
```

Set your Anthropic API key:

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

## Project Structure

```
pipeline/
├── pipeline_scaffold.py          # main orchestrator
├── stages/
│   ├── stage1_ingest.py          # file reading + per-file Claude summaries
│   ├── stage2_analyze.py         # memo synthesis + JSON extraction
│   ├── stage3_populate.py        # Excel population with key validation
│   └── stage4_finalize.py        # email drafting
├── utils/
│   ├── claude_client.py          # Anthropic client with retry + token tracking
│   ├── checkpoint.py             # versioned checkpoint read/write
│   ├── file_handlers.py          # readers for .csv, .xlsx, .pdf, .txt
│   └── excel_helpers.py          # copy-on-write Excel, cell mapping, recalc
├── config/
│   ├── prompts.yaml              # all Claude prompts (customization surface)
│   └── cell_mapping.json         # Excel cell → data key mapping
├── data/                         # drop source files here
├── model/                        # place base_model.xlsx here
├── checkpoints/                  # versioned stage outputs (auto-generated)
└── outputs/                      # final deliverables (auto-generated)
```

## Configuration

Before running, you need to configure two files to match your Excel model:

### `config/cell_mapping.json`

Maps data keys to Excel cells. Edit the sheet names, cell references, and keys to match your `base_model.xlsx`:

```json
{
  "inputs": {
    "flat": [
      {"key": "total_revenue", "sheet": "Model", "cell": "B3"}
    ],
    "ranges": [
      {"key": "monthly_revenue", "sheet": "Model", "start_cell": "C3", "direction": "horizontal"}
    ]
  },
  "outputs": {
    "flat": [
      {"key": "net_income", "sheet": "Model", "cell": "B10"}
    ]
  }
}
```

### `config/prompts.yaml`

All Claude prompts live here. Look for `[CUSTOMIZE]` markers to adapt prompts to your domain. The Stage 2b `schema.required_keys` must match the keys in `cell_mapping.json`.

## Usage

1. Drop your source files (`.csv`, `.xlsx`, `.pdf`, `.txt`) into `data/`
2. Place your Excel template as `model/base_model.xlsx`
3. Update `config/cell_mapping.json` and `config/prompts.yaml` to match your model
4. Run:

```bash
python pipeline_scaffold.py
```

### Options

```bash
# Use a specific model
python pipeline_scaffold.py --model claude-sonnet-4-20250514

# Resume a failed run from Stage 3
python pipeline_scaffold.py --resume-run-id <run_id> --start-stage 3
```

### Outputs

After a successful run, check `outputs/` for:

- `{timestamp}_model.xlsx` — populated Excel model
- `email_draft_{run_id}.md` — draft email for leadership
- `pipeline.log` — run log with token usage summary

## How It Works

| Stage | What happens |
|-------|-------------|
| **1 — Ingest** | Reads each file in `data/`, summarizes via Claude. Large files are chunked and map-reduced. |
| **2a — Memo** | All file summaries are synthesized into a single executive memo. |
| **2b — Extract** | A separate Claude call extracts structured JSON from the memo against a rigid schema. |
| **3 — Populate** | Validates extracted keys against `cell_mapping.json`, copies `base_model.xlsx` (never mutates the original), writes values, attempts LibreOffice formula recalc. |
| **4 — Email** | Combines memo + model inputs/outputs into context, Claude drafts a summary email. If formulas weren't recalculated, adds a caveat. |

Each stage writes a checkpoint to `checkpoints/`, enabling resume on failure.

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Empty data directory | Fails fast with clear error |
| Unsupported file type | Skipped with warning |
| Key mismatch (2b data vs cell mapping) | Fails before touching Excel, lists missing and available keys |
| Claude returns malformed JSON | Tries `json.loads`, then regex extraction from code fences, then fails with raw output |
| LibreOffice not installed | Skips recalc, flags `formulas_recalculated=false`, Stage 4 adds caveat to email |
| Pipeline crash mid-run | Resume with `--resume-run-id` and `--start-stage` |
| API rate limits / errors | Anthropic SDK retries up to 5 times with exponential backoff |
| Token usage | Always logged to `pipeline.log`, even on failure |
