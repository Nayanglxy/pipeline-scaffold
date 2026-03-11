"""Per-type file readers and chunking utilities."""

import csv
import io
import logging
import os

logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {".txt", ".csv", ".xlsx", ".pdf"}


def read_txt(path: str) -> str:
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.read()


def read_csv(path: str) -> str:
    """Read CSV and return a markdown-table-like text representation."""
    import pandas as pd

    df = pd.read_csv(path)
    from tabulate import tabulate

    return tabulate(df, headers="keys", tablefmt="github", showindex=False)


def read_xlsx(path: str) -> str:
    """Read all sheets of an Excel file and return text representation."""
    import pandas as pd
    from tabulate import tabulate

    xls = pd.ExcelFile(path)
    parts = []
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        parts.append(f"## Sheet: {sheet_name}\n")
        parts.append(tabulate(df, headers="keys", tablefmt="github", showindex=False))
        parts.append("")
    return "\n".join(parts)


def read_pdf(path: str) -> str:
    """Extract text from a PDF using pdfplumber."""
    import pdfplumber

    pages = []
    with pdfplumber.open(path) as pdf:
        for i, page in enumerate(pdf.pages, 1):
            text = page.extract_text() or ""
            if text.strip():
                pages.append(f"[Page {i}]\n{text}")
    return "\n\n".join(pages)


def read_file(path: str) -> str:
    """Dispatch to the correct reader based on file extension."""
    ext = os.path.splitext(path)[1].lower()
    readers = {
        ".txt": read_txt,
        ".csv": read_csv,
        ".xlsx": read_xlsx,
        ".pdf": read_pdf,
    }
    reader = readers.get(ext)
    if reader is None:
        raise ValueError(f"Unsupported file type: {ext} (file: {path})")
    logger.info("Reading %s (%s)", os.path.basename(path), ext)
    return reader(path)


def chunk_text(text: str, max_chunk_tokens: int = 60_000) -> list[str]:
    """Split text on paragraph boundaries to stay under a token budget.

    Uses a rough 4-chars-per-token heuristic for splitting.  For precise
    token counts, the caller should use ClaudeClient.estimate_tokens().
    """
    char_budget = max_chunk_tokens * 4  # rough heuristic
    if len(text) <= char_budget:
        return [text]

    paragraphs = text.split("\n\n")
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for para in paragraphs:
        para_len = len(para) + 2  # account for \n\n separator
        if current_len + para_len > char_budget and current:
            chunks.append("\n\n".join(current))
            current = []
            current_len = 0
        current.append(para)
        current_len += para_len

    if current:
        chunks.append("\n\n".join(current))

    return chunks


def needs_chunking(text: str, client=None) -> bool:
    """Check whether text exceeds the ~80K token budget for a single call.

    If a ClaudeClient is provided, uses the precise count_tokens API;
    otherwise falls back to a character heuristic.
    """
    threshold = 80_000
    if client is not None:
        try:
            count = client.estimate_tokens(
                [{"role": "user", "content": text}]
            )
            return count > threshold
        except Exception:
            logger.warning("Token counting failed, falling back to heuristic")
    return len(text) > threshold * 4
