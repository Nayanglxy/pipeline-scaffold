"""Stage 4: Read Excel outputs and draft a summary email via Claude."""

import logging
import os

from utils.checkpoint import save_checkpoint

logger = logging.getLogger(__name__)


def run_stage4(
    stage3_result: dict,
    memo: str,
    client,
    prompts: dict,
    run_id: str,
    output_dir: str,
    checkpoint_dir: str,
) -> dict:
    """Draft a summary email from the memo and model I/O."""
    s4_prompts = prompts["stage4"]

    # Build formula caveat
    if stage3_result.get("formulas_recalculated"):
        formula_caveat = ""
    else:
        formula_caveat = s4_prompts.get("formula_caveat_text", "")

    system = s4_prompts["system"].format(formula_caveat=formula_caveat)

    # Build user content with all context
    inputs_section = "\n".join(
        f"  {k}: {v}" for k, v in stage3_result.get("inputs_written", {}).items()
    )
    outputs_section = "\n".join(
        f"  {k}: {v}" for k, v in stage3_result.get("outputs", {}).items()
    )
    user_content = (
        f"## Executive Memo\n{memo}\n\n"
        f"## Model Inputs Written\n{inputs_section}\n\n"
        f"## Model Outputs\n{outputs_section}"
    )

    email_draft = client.call(
        "stage4", system, user_content, max_tokens=2048, temperature=0.3
    )

    # Write email to file
    os.makedirs(output_dir, exist_ok=True)
    email_path = os.path.join(output_dir, f"email_draft_{run_id}.md")
    with open(email_path, "w") as f:
        f.write(email_draft)
    logger.info("Stage 4: email draft written to %s", email_path)

    result = {
        "email_path": email_path,
        "email_draft": email_draft,
    }
    save_checkpoint("stage4", run_id, result, checkpoint_dir)
    return result
