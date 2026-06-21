"""Validate docker-compose.yml structure — no Docker daemon required."""
from pathlib import Path

import pytest
import yaml

COMPOSE_FILE = Path("docker-compose.yml")

CORE_SERVICES = [
    "postgres", "iceberg-rest",
    "redpanda", "redpanda-console",
    "prefect-server", "prefect-worker",
    "mlflow", "ml-serving",
]

LINEAGE_SERVICES = ["marquez", "marquez-web"]


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))


def test_compose_file_exists():
    assert COMPOSE_FILE.exists()


def test_core_services_present(compose):
    services = compose["services"]
    missing = [s for s in CORE_SERVICES if s not in services]
    assert not missing, f"Missing core services: {missing}"


def test_lineage_services_under_lineage_profile(compose):
    services = compose["services"]
    for svc in LINEAGE_SERVICES:
        assert svc in services, f"Missing service: {svc}"
        profiles = services[svc].get("profiles", [])
        assert "lineage" in profiles, \
            f"Service '{svc}' must be under 'lineage' profile"


def test_prefect_worker_has_openlineage_url(compose):
    env = compose["services"]["prefect-worker"]["environment"]
    assert "OPENLINEAGE_URL" in env, "prefect-worker missing OPENLINEAGE_URL"


def test_prefect_worker_has_spark_jars_volume(compose):
    volumes = compose["services"]["prefect-worker"].get("volumes", [])
    assert any("spark/jars" in v for v in volumes), \
        "prefect-worker must mount spark/jars"


def test_all_services_on_lakehouse_network(compose):
    for name, svc in compose["services"].items():
        nets = svc.get("networks", [])
        assert "lakehouse" in nets, \
            f"Service '{name}' not connected to lakehouse network"


def test_postgres_has_healthcheck(compose):
    assert "healthcheck" in compose["services"]["postgres"]


def test_iceberg_rest_depends_on_healthy_postgres(compose):
    deps = compose["services"]["iceberg-rest"].get("depends_on", {})
    assert "postgres" in deps
    assert deps["postgres"].get("condition") == "service_healthy"


def test_marquez_depends_on_healthy_postgres(compose):
    deps = compose["services"]["marquez"].get("depends_on", {})
    assert "postgres" in deps
    assert deps["postgres"].get("condition") == "service_healthy"
