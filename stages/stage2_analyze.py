"""Stage 2: Synthesize memo (2a) and extract structured JSON (2b).

Fix 1: Two separate Claude calls — 2a does unconstrained prose, 2b does
rigid JSON extraction against a defined schema.
"""

import json
import logging
import re

from utils.checkpoint import save_checkpoint

logger = logging.getLogger(__name__)


class JSONExtractionError(Exception):
    """Raised when Stage 2b output cannot be parsed as JSON."""


def run_stage2a(
    summaries: list[dict],
    client,
    prompts: dict,
    run_id: str,
    checkpoint_dir: str,
) -> str:
    """Synthesize per-file summaries into an executive memo."""
    combined = "\n\n".join(
        f"### {s['filename']}\n{s['summary']}" for s in summaries
    )
    system = prompts["stage2a"]["system"]
    memo = client.call("stage2a", system, combined, max_tokens=4096, temperature=0.3)

    save_checkpoint("stage2a", run_id, {"memo": memo}, checkpoint_dir)
    logger.info("Stage 2a: memo generated (%d chars)", len(memo))
    return memo


def run_stage2b(
    memo: str,
    client,
    prompts: dict,
    run_id: str,
    checkpoint_dir: str,
) -> dict:
    """Extract structured JSON from the memo against a rigid schema."""
    schema_keys = prompts["stage2b"]["schema"]["required_keys"]
    schema_description = "\n".join(
        f'  - "{k["key"]}": {k["type"]} — {k["description"]}'
        for k in schema_keys
    )
    system = prompts["stage2b"]["system"].format(schema_keys=schema_description)

    raw = client.call("stage2b", system, memo, max_tokens=2048, temperature=0.0)
    data = _parse_json(raw)

    save_checkpoint("stage2b", run_id, data, checkpoint_dir)
    logger.info("Stage 2b: extracted %d keys", len(data))
    return data


def _parse_json(raw: str) -> dict:
    """Try json.loads first, then extract from fenced code block, then fail."""
    # Attempt 1: direct parse
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Attempt 2: extract from ```json ... ``` or ``` ... ```
    match = re.search(r"```(?:json)?\s*\n?(.*?)\n?\s*```", raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Attempt 3: find first { ... } block
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    raise JSONExtractionError(
        f"Could not parse JSON from Stage 2b response.\nRaw output:\n{raw[:2000]}"
    )
