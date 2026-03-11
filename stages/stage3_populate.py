"""Stage 3: Validate keys, copy-on-write Excel, write values, recalc, read outputs."""

import logging

from utils.checkpoint import save_checkpoint
from utils.excel_helpers import (
    copy_base_model,
    load_cell_mapping,
    read_output_cells,
    recalculate_formulas,
    validate_keys,
    write_values_to_excel,
)

logger = logging.getLogger(__name__)


class KeyValidationError(Exception):
    """Raised when Stage 2b keys don't match cell_mapping.json."""


def run_stage3(
    stage2b_data: dict,
    base_model_path: str,
    output_dir: str,
    cell_mapping_path: str,
    run_id: str,
    timestamp: str,
    checkpoint_dir: str,
) -> dict:
    """Populate the Excel model and return input/output values + metadata."""

    # Step 1: load cell mapping
    mapping = load_cell_mapping(cell_mapping_path)
    logger.info("Stage 3: loaded cell mapping (schema v%s)", mapping.schema_version)

    # Step 2: validate keys — fail before touching Excel (Fix 4)
    missing = validate_keys(mapping, stage2b_data)
    if missing:
        raise KeyValidationError(
            f"Stage 2b data is missing keys required by cell_mapping.json.\n"
            f"  Missing: {missing}\n"
            f"  Available: {sorted(stage2b_data.keys())}\n"
            f"Fix: update prompts.yaml Stage 2b schema or cell_mapping.json "
            f"so the keys align."
        )

    # Step 3: copy base model (Fix 2 — never mutate the original)
    excel_path = copy_base_model(base_model_path, output_dir, timestamp)

    # Step 4: write values
    write_values_to_excel(excel_path, mapping, stage2b_data)

    # Step 5: attempt formula recalculation (Fix 3)
    formulas_recalculated = recalculate_formulas(excel_path)

    # Step 6: read output cells
    outputs = read_output_cells(excel_path, mapping)

    result = {
        "excel_path": excel_path,
        "inputs_written": {
            m.key: stage2b_data.get(m.key)
            for m in mapping.input_flat
        },
        "outputs": outputs,
        "formulas_recalculated": formulas_recalculated,
    }
    save_checkpoint("stage3", run_id, result, checkpoint_dir)
    logger.info(
        "Stage 3: complete (recalc=%s, outputs=%s)",
        formulas_recalculated,
        outputs,
    )
    return result
