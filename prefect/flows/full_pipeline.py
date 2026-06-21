"""
Flow: Full Pipeline
Bronze ingestion → Silver → Quality check → Gold → Quality check.

New features vs original:
  - pause_on_quality_fail: pause flow run when a Soda gate fails (requires Prefect server)
  - on_failure hook: POST to NOTIFY_WEBHOOK_URL on any failure
  - --serve flag: start with daily 02:00 UTC schedule instead of a one-shot run
"""
import argparse

from bronze_ingestion import bronze_ingestion_flow
from notifications import notify_failure
from quality_checks import raise_if_failed, soda_check_task
from spark_transforms import gold_transform_flow, silver_transform_flow

from prefect import flow, get_run_logger, pause_flow_run


def _quality_gate(layer: str, result: dict, pause: bool, log) -> None:
    """Optionally pause the flow run for human review; otherwise raise immediately."""
    failed = result.get("failed", 0) + result.get("errored", 0)
    if failed == 0:
        return
    log.warning("Quality gate FAILED for layer=%s (%d check(s) failed).", layer, failed)
    if pause:
        log.warning("Pausing flow run — resume from Prefect UI after fixing the data.")
        pause_flow_run(timeout=3600, key=f"quality-gate-{layer}")
    else:
        raise_if_failed(result)


@flow(
    name="full-pipeline",
    description=(
        "End-to-end: Bronze ingestion → Silver → Gold with Soda quality gates. "
        "Set pause_on_quality_fail=True to require manual approval when checks fail."
    ),
    log_prints=True,
    on_failure=[notify_failure],
)
def full_pipeline_flow(
    skip_bronze: bool = False,
    skip_quality: bool = False,
    overwrite_bronze: bool = False,
    pause_on_quality_fail: bool = False,
):
    log = get_run_logger()
    log.info("=== FULL PIPELINE START ===")
    log.info(
        "skip_bronze=%s  skip_quality=%s  overwrite=%s  pause_on_fail=%s",
        skip_bronze, skip_quality, overwrite_bronze, pause_on_quality_fail,
    )

    if not skip_bronze:
        log.info("Step 1/5: Bronze ingestion (overwrite=%s)", overwrite_bronze)
        bronze_ingestion_flow(overwrite=overwrite_bronze)

        if not skip_quality:
            log.info("Step 2/5: Bronze quality check")
            bronze_qc = soda_check_task("bronze")
            _quality_gate("bronze", bronze_qc, pause_on_quality_fail, log)
    else:
        log.info("Step 1-2/5: Bronze SKIPPED")

    log.info("Step 3/5: Silver transform")
    silver_transform_flow()

    if not skip_quality:
        log.info("Step 4/5: Silver quality check")
        silver_qc = soda_check_task("silver")
        _quality_gate("silver", silver_qc, pause_on_quality_fail, log)

    log.info("Step 5/5: Gold transform")
    gold_transform_flow()

    if not skip_quality:
        log.info("Step 6/6: Gold quality check")
        gold_qc = soda_check_task("gold")
        _quality_gate("gold", gold_qc, pause_on_quality_fail, log)

    log.info("=== FULL PIPELINE COMPLETE ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run or serve the full Medallion pipeline.")
    parser.add_argument("--serve",              action="store_true",
                        help="Serve with daily 02:00 UTC schedule (runs until Ctrl-C)")
    parser.add_argument("--skip-bronze",        action="store_true")
    parser.add_argument("--skip-quality",       action="store_true")
    parser.add_argument("--overwrite-bronze",   action="store_true")
    parser.add_argument("--pause-on-fail",      action="store_true",
                        help="Pause flow run when a quality gate fails (requires Prefect server)")
    args = parser.parse_args()

    if args.serve:
        full_pipeline_flow.serve(
            name="daily-medallion",
            cron="0 2 * * *",
            parameters={
                "skip_quality":          args.skip_quality,
                "pause_on_quality_fail": True,
            },
        )
    else:
        full_pipeline_flow(
            skip_bronze=args.skip_bronze,
            skip_quality=args.skip_quality,
            overwrite_bronze=args.overwrite_bronze,
            pause_on_quality_fail=args.pause_on_fail,
        )
