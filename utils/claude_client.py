"""Wrapped Anthropic client with retry and token tracking."""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone

import anthropic

logger = logging.getLogger(__name__)

# Approximate pricing per 1M tokens (Claude Sonnet 4 defaults; adjust as needed)
INPUT_COST_PER_M = 3.00
OUTPUT_COST_PER_M = 15.00


@dataclass
class _CallRecord:
    stage: str
    input_tokens: int
    output_tokens: int
    model: str


class ClaudeClient:
    """Thin wrapper around anthropic.Anthropic with per-call token tracking."""

    def __init__(self, model: str = "claude-sonnet-4-20250514"):
        self.model = model
        self._client = anthropic.Anthropic(max_retries=5)
        self._records: list[_CallRecord] = []

    def call(
        self,
        stage_label: str,
        system_prompt: str,
        user_content: str,
        max_tokens: int = 4096,
        temperature: float = 0.2,
    ) -> str:
        """Send a message to Claude and return the text response."""
        logger.info("[%s] Calling Claude (%s) ...", stage_label, self.model)
        response = self._client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=temperature,
            system=system_prompt,
            messages=[{"role": "user", "content": user_content}],
        )
        usage = response.usage
        self._records.append(
            _CallRecord(
                stage=stage_label,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                model=self.model,
            )
        )
        logger.info(
            "[%s] Tokens — in: %d, out: %d",
            stage_label,
            usage.input_tokens,
            usage.output_tokens,
        )
        return response.content[0].text

    def estimate_tokens(self, messages: list[dict]) -> int:
        """Use the Anthropic count_tokens endpoint for chunking decisions."""
        result = self._client.messages.count_tokens(
            model=self.model,
            messages=messages,
        )
        return result.input_tokens

    def get_usage_summary(self) -> dict:
        """Return aggregate token usage and estimated cost."""
        total_in = sum(r.input_tokens for r in self._records)
        total_out = sum(r.output_tokens for r in self._records)
        cost = (total_in / 1_000_000) * INPUT_COST_PER_M + (
            total_out / 1_000_000
        ) * OUTPUT_COST_PER_M
        return {
            "total_input_tokens": total_in,
            "total_output_tokens": total_out,
            "estimated_cost_usd": round(cost, 4),
            "calls": len(self._records),
            "per_stage": self._per_stage_breakdown(),
        }

    def _per_stage_breakdown(self) -> dict:
        breakdown: dict[str, dict] = {}
        for r in self._records:
            if r.stage not in breakdown:
                breakdown[r.stage] = {"input_tokens": 0, "output_tokens": 0, "calls": 0}
            breakdown[r.stage]["input_tokens"] += r.input_tokens
            breakdown[r.stage]["output_tokens"] += r.output_tokens
            breakdown[r.stage]["calls"] += 1
        return breakdown

    def write_usage_log(self, log_path: str) -> None:
        """Append a formatted usage summary to the given log file."""
        summary = self.get_usage_summary()
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        lines = [
            "",
            f"=== Token Usage Summary ({ts}) ===",
            f"Total input tokens:  {summary['total_input_tokens']:,}",
            f"Total output tokens: {summary['total_output_tokens']:,}",
            f"Estimated cost:      ${summary['estimated_cost_usd']:.4f}",
            f"API calls:           {summary['calls']}",
            "",
        ]
        for stage, info in summary["per_stage"].items():
            lines.append(
                f"  {stage}: in={info['input_tokens']:,} out={info['output_tokens']:,} calls={info['calls']}"
            )
        lines.append("")
        os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
        with open(log_path, "a") as f:
            f.write("\n".join(lines))
        logger.info("Usage log written to %s", log_path)
