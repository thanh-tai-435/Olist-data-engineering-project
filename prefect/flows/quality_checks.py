"""
Prefect tasks/flows: Soda Core data quality checks.
Chạy sau mỗi layer transform để gate pipeline.
"""
import sys
import logging
from pathlib import Path
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

_QUALITY_DIR = Path(__file__).parents[1] / "quality"
if str(_QUALITY_DIR) not in sys.path:
    sys.path.insert(0, str(_QUALITY_DIR))


@task(
    name="soda-quality-check",
    retries=0,
    description="Run Soda Core checks for a Medallion layer.",
)
def soda_check_task(layer: str) -> dict:
    log = get_run_logger()
    log.info("Running Soda checks for layer: %s", layer)

    from soda_runner import run_checks  # noqa: PLC0415
    result = run_checks(layer)

    emoji = "✅" if result["failed"] == 0 and result["errored"] == 0 else "❌"
    create_markdown_artifact(
        key=f"soda-{layer}",
        markdown=(
            f"## {emoji} Soda Quality — {layer.capitalize()}\n\n"
            f"| Status  | Count |\n"
            f"|---------|-------|\n"
            f"| ✓ Passed  | {result['passed']} |\n"
            f"| ⚠ Warned  | {result['warned']} |\n"
            f"| ✗ Failed  | {result['failed']} |\n"
            f"| ✗ Errored | {result['errored']} |\n"
            f"| **Total** | **{result['total']}** |\n"
        ),
        description=f"Soda Core data quality results for {layer} layer",
    )

    log.info(
        "Soda [%s]: %d passed, %d warned, %d failed",
        layer, result["passed"], result["warned"], result["failed"],
    )
    return result


def raise_if_failed(result: dict) -> None:
    """Raise after the artifact is already persisted so the UI always shows the result."""
    failed = result.get("failed", 0)
    if failed > 0:
        raise RuntimeError(f"Soda [{result['layer']}]: {failed} check(s) failed")


@flow(
    name="quality-checks",
    description="Run Soda Core checks for a single layer.",
    log_prints=True,
)
def quality_checks_flow(layer: str) -> dict:
    return soda_check_task(layer)


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", choices=["bronze", "silver", "gold"], required=True)
    args = parser.parse_args()
    quality_checks_flow(layer=args.layer)
