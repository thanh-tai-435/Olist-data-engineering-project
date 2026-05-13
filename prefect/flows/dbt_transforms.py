"""
Flows: dbt Silver & Gold transforms.
Chạy dbt models theo từng layer trên DuckDB + Iceberg.
"""
import os
import subprocess
from pathlib import Path
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/app/dbt"))


@task(name="dbt-run", retries=1, retry_delay_seconds=30)
def run_dbt(select: str, full_refresh: bool = False) -> str:
    log = get_run_logger()
    cmd = [
        "dbt", "run",
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROJECT_DIR),
        "--select", select,
    ]
    if full_refresh:
        cmd.append("--full-refresh")

    log.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.stdout:
        log.info(result.stdout)
    if result.stderr:
        log.warning(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed (exit {result.returncode})\n{result.stderr}")

    return result.stdout


@task(name="dbt-test", retries=0)
def run_dbt_test(select: str) -> str:
    log = get_run_logger()
    cmd = [
        "dbt", "test",
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROJECT_DIR),
        "--select", select,
    ]
    log.info(f"Testing: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.stdout:
        log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"dbt test FAILED\n{result.stderr}")

    return result.stdout


@task(name="dbt-docs-generate")
def generate_dbt_docs() -> None:
    subprocess.run([
        "dbt", "docs", "generate",
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROJECT_DIR),
    ], check=True)


@flow(
    name="silver-transform",
    description="dbt: Bronze → Silver (staging + intermediate models).",
    log_prints=True,
)
def silver_transform_flow(full_refresh: bool = False):
    log = get_run_logger()
    log.info(f"Silver transform | full_refresh={full_refresh}")

    output = run_dbt("staging intermediate", full_refresh=full_refresh)
    run_dbt_test("staging intermediate")

    create_markdown_artifact(
        key="silver-dbt-output",
        markdown=f"```\n{output}\n```",
        description="dbt Silver layer output",
    )
    log.info("Silver transform DONE.")


@flow(
    name="gold-transform",
    description="dbt: Silver → Gold (marts: fct_orders, fct_funnel, dim_*).",
    log_prints=True,
)
def gold_transform_flow(full_refresh: bool = False):
    log = get_run_logger()
    log.info(f"Gold transform | full_refresh={full_refresh}")

    output = run_dbt("marts", full_refresh=full_refresh)
    run_dbt_test("marts")

    create_markdown_artifact(
        key="gold-dbt-output",
        markdown=f"```\n{output}\n```",
        description="dbt Gold layer output",
    )
    log.info("Gold transform DONE.")


if __name__ == "__main__":
    silver_transform_flow()
    gold_transform_flow()
