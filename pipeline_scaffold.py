#!/usr/bin/env python3
"""Pipeline orchestrator: Data → Memo → Excel → Email.

Usage:
    python pipeline_scaffold.py
    python pipeline_scaffold.py --resume-run-id abc123 --start-stage 3
    python pipeline_scaffold.py --model claude-sonnet-4-20250514
"""

import argparse
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

import yaml

# Ensure project root is on sys.path so `utils` and `stages` resolve
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from stages.stage1_ingest import run_stage1
from stages.stage2_analyze import run_stage2a, run_stage2b
from stages.stage3_populate import run_stage3
from stages.stage4_finalize import run_stage4
from utils.checkpoint import load_checkpoint
from utils.claude_client import ClaudeClient


class PipelineError(Exception):
    """Pipeline-specific errors with stage context."""

    def __init__(self, message: str, stage: str | None = None):
        self.stage = stage
        super().__init__(f"[Stage {stage}] {message}" if stage else message)


def setup_logging(log_path: str) -> None:
    """Configure logging to both console and file."""
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_path, mode="a"),
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )


def load_prompts(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the data-to-email pipeline.")
    p.add_argument(
        "--resume-run-id",
        default=None,
        help="Resume a previous run by its run_id.",
    )
    p.add_argument(
        "--start-stage",
        type=int,
        default=1,
        choices=[1, 2, 3, 4],
        help="Stage to start from (use with --resume-run-id to skip earlier stages).",
    )
    p.add_argument(
        "--model",
        default="claude-sonnet-4-20250514",
        help="Anthropic model ID to use.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Paths
    data_dir = os.path.join(PROJECT_ROOT, "data")
    model_dir = os.path.join(PROJECT_ROOT, "model")
    config_dir = os.path.join(PROJECT_ROOT, "config")
    checkpoint_dir = os.path.join(PROJECT_ROOT, "checkpoints")
    output_dir = os.path.join(PROJECT_ROOT, "outputs")
    base_model_path = os.path.join(model_dir, "base_model.xlsx")
    prompts_path = os.path.join(config_dir, "prompts.yaml")
    cell_mapping_path = os.path.join(config_dir, "cell_mapping.json")

    # Run identity
    run_id = args.resume_run_id or uuid.uuid4().hex[:12]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = os.path.join(output_dir, "pipeline.log")

    setup_logging(log_path)
    logger = logging.getLogger("pipeline")
    logger.info("=" * 60)
    logger.info("Pipeline run: %s  (start_stage=%d)", run_id, args.start_stage)
    logger.info("=" * 60)

    # Validate prerequisites
    if not os.path.isdir(data_dir):
        raise PipelineError(f"Data directory not found: {data_dir}")
    if not os.path.isfile(prompts_path):
        raise PipelineError(f"Prompts config not found: {prompts_path}")
    if not os.path.isfile(cell_mapping_path):
        raise PipelineError(f"Cell mapping not found: {cell_mapping_path}")

    prompts = load_prompts(prompts_path)
    client = ClaudeClient(model=args.model)

    # Stage results — loaded from checkpoint when skipping
    stage1_result = None
    memo = None
    stage2b_data = None
    stage3_result = None

    try:
        # ── Stage 1 ──────────────────────────────────────────────
        if args.start_stage <= 1:
            logger.info("▶ Stage 1: Ingest")
            stage1_result = run_stage1(
                data_dir, client, prompts, run_id, checkpoint_dir
            )
        else:
            stage1_result = load_checkpoint("stage1", run_id, checkpoint_dir)
            if stage1_result is None:
                raise PipelineError(
                    "No Stage 1 checkpoint found for resume", stage="1"
                )

        # ── Stage 2a ─────────────────────────────────────────────
        if args.start_stage <= 2:
            logger.info("▶ Stage 2a: Memo synthesis")
            memo = run_stage2a(
                stage1_result["summaries"],
                client,
                prompts,
                run_id,
                checkpoint_dir,
            )
        else:
            cp = load_checkpoint("stage2a", run_id, checkpoint_dir)
            if cp is None:
                raise PipelineError(
                    "No Stage 2a checkpoint found for resume", stage="2a"
                )
            memo = cp["memo"]

        # ── Stage 2b ─────────────────────────────────────────────
        if args.start_stage <= 2:
            logger.info("▶ Stage 2b: JSON extraction")
            stage2b_data = run_stage2b(
                memo, client, prompts, run_id, checkpoint_dir
            )
        else:
            stage2b_data = load_checkpoint("stage2b", run_id, checkpoint_dir)
            if stage2b_data is None:
                raise PipelineError(
                    "No Stage 2b checkpoint found for resume", stage="2b"
                )

        # ── Stage 3 ──────────────────────────────────────────────
        if args.start_stage <= 3:
            logger.info("▶ Stage 3: Excel population")
            if not os.path.isfile(base_model_path):
                raise PipelineError(
                    f"Base model not found: {base_model_path}", stage="3"
                )
            stage3_result = run_stage3(
                stage2b_data,
                base_model_path,
                output_dir,
                cell_mapping_path,
                run_id,
                timestamp,
                checkpoint_dir,
            )
        else:
            stage3_result = load_checkpoint("stage3", run_id, checkpoint_dir)
            if stage3_result is None:
                raise PipelineError(
                    "No Stage 3 checkpoint found for resume", stage="3"
                )

        # ── Stage 4 ──────────────────────────────────────────────
        logger.info("▶ Stage 4: Email draft")
        stage4_result = run_stage4(
            stage3_result,
            memo,
            client,
            prompts,
            run_id,
            output_dir,
            checkpoint_dir,
        )

        logger.info("=" * 60)
        logger.info("Pipeline complete!")
        logger.info("  Excel: %s", stage3_result.get("excel_path"))
        logger.info("  Email: %s", stage4_result.get("email_path"))
        logger.info("  Log:   %s", log_path)
        logger.info("=" * 60)

    except Exception:
        logger.exception("Pipeline failed")
        raise

    finally:
        # Fix 8: always write token usage, even on failure
        client.write_usage_log(log_path)


if __name__ == "__main__":
    main()
