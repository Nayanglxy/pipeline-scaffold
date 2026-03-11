"""Versioned checkpoint read/write for pipeline stages."""

import glob
import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "1.0"


@dataclass
class Checkpoint:
    schema_version: str
    stage: str
    timestamp: str
    run_id: str
    data: dict = field(default_factory=dict)


def save_checkpoint(
    stage: str,
    run_id: str,
    data: Any,
    checkpoint_dir: str,
) -> str:
    """Write a checkpoint to disk. Returns the path written."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    cp = Checkpoint(
        schema_version=SCHEMA_VERSION,
        stage=stage,
        timestamp=ts,
        run_id=run_id,
        data=data,
    )
    os.makedirs(checkpoint_dir, exist_ok=True)
    filename = f"{stage}_{run_id}_{ts}.json"
    path = os.path.join(checkpoint_dir, filename)
    with open(path, "w") as f:
        json.dump(asdict(cp), f, indent=2, default=str)
    logger.info("Checkpoint saved: %s", path)
    return path


def load_checkpoint(
    stage: str,
    run_id: str,
    checkpoint_dir: str,
) -> dict | None:
    """Load the most recent checkpoint for a given stage + run_id.

    Returns the data payload or None if no checkpoint found.
    """
    pattern = os.path.join(checkpoint_dir, f"{stage}_{run_id}_*.json")
    matches = sorted(glob.glob(pattern))
    if not matches:
        logger.warning("No checkpoint found for stage=%s run_id=%s", stage, run_id)
        return None

    path = matches[-1]  # most recent by timestamp in filename
    logger.info("Loading checkpoint: %s", path)
    with open(path) as f:
        envelope = json.load(f)

    if envelope.get("schema_version") != SCHEMA_VERSION:
        logger.warning(
            "Schema version mismatch: checkpoint has %s, expected %s",
            envelope.get("schema_version"),
            SCHEMA_VERSION,
        )

    return envelope.get("data")
