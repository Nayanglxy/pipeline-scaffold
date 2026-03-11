"""Stage 1: Read source data files and produce per-file Claude summaries."""

import logging
import os

from utils.checkpoint import save_checkpoint
from utils.file_handlers import (
    SUPPORTED_EXTENSIONS,
    chunk_text,
    needs_chunking,
    read_file,
)

logger = logging.getLogger(__name__)


def run_stage1(
    data_dir: str,
    client,
    prompts: dict,
    run_id: str,
    checkpoint_dir: str,
) -> dict:
    """Ingest all files in data_dir and return per-file summaries.

    Returns: {"summaries": [{"filename": str, "summary": str}, ...]}
    """
    files = sorted(
        f
        for f in os.listdir(data_dir)
        if os.path.splitext(f)[1].lower() in SUPPORTED_EXTENSIONS
    )
    if not files:
        raise FileNotFoundError(
            f"No supported files found in {data_dir}. "
            f"Supported types: {SUPPORTED_EXTENSIONS}"
        )

    logger.info("Stage 1: found %d files to ingest", len(files))
    s1_prompts = prompts["stage1"]
    summaries = []

    for filename in files:
        path = os.path.join(data_dir, filename)
        content = read_file(path)

        if needs_chunking(content, client):
            summary = _map_reduce_summarize(filename, content, client, s1_prompts)
        else:
            summary = _single_pass_summarize(filename, content, client, s1_prompts)

        summaries.append({"filename": filename, "summary": summary})
        logger.info("Stage 1: summarized %s (%d chars)", filename, len(summary))

    result = {"summaries": summaries}
    save_checkpoint("stage1", run_id, result, checkpoint_dir)
    return result


def _single_pass_summarize(
    filename: str, content: str, client, prompts: dict
) -> str:
    system = prompts["file_summary"]["system"]
    user = prompts["file_summary"]["user_template"].format(
        filename=filename, content=content
    )
    return client.call("stage1", system, user)


def _map_reduce_summarize(
    filename: str, content: str, client, prompts: dict
) -> str:
    chunks = chunk_text(content)
    logger.info("Stage 1: %s split into %d chunks", filename, len(chunks))

    chunk_summaries = []
    for i, chunk in enumerate(chunks, 1):
        system = prompts["chunk_summary"]["system"]
        user = prompts["chunk_summary"]["user_template"].format(
            filename=filename,
            chunk_index=i,
            total_chunks=len(chunks),
            content=chunk,
        )
        summary = client.call("stage1_chunk", system, user)
        chunk_summaries.append(f"--- Chunk {i} ---\n{summary}")

    # Combine chunk summaries
    system = prompts["combine_summaries"]["system"]
    user = prompts["combine_summaries"]["user_template"].format(
        filename=filename,
        chunk_summaries="\n\n".join(chunk_summaries),
    )
    return client.call("stage1_combine", system, user)
