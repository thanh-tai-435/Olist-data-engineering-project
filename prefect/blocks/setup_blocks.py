"""
One-time setup: register R2 + service credentials as Prefect Secret blocks.

Run once after `docker compose up prefect-server`:
    export PREFECT_API_URL=http://localhost:4200/api
    # (copy values from your .env)
    export S3_ACCESS_KEY=...  S3_SECRET_KEY=...  S3_ENDPOINT=...
    python prefect/blocks/setup_blocks.py

Blocks created:
    r2-access-key       S3_ACCESS_KEY
    r2-secret-key       S3_SECRET_KEY
    r2-endpoint         S3_ENDPOINT
    r2-region           S3_REGION
    mlflow-tracking-uri MLFLOW_TRACKING_URI
    iceberg-rest-uri    ICEBERG_REST_URI
"""
import os
import sys
from prefect.blocks.system import Secret


# (env_var, required)
_CREDENTIALS = {
    "r2-access-key":        ("S3_ACCESS_KEY",       True),
    "r2-secret-key":        ("S3_SECRET_KEY",        True),
    "r2-endpoint":          ("S3_ENDPOINT",          False),
    "r2-region":            ("S3_REGION",            False),
    "mlflow-tracking-uri":  ("MLFLOW_TRACKING_URI",  False),
    "iceberg-rest-uri":     ("ICEBERG_REST_URI",      False),
}


def setup_blocks() -> None:
    created = []
    for block_name, (env_key, required) in _CREDENTIALS.items():
        val = os.environ.get(env_key, "")
        if not val:
            if required:
                print(f"  ✗ {block_name!r}: {env_key} not set (required)", file=sys.stderr)
                sys.exit(1)
            print(f"  skip {block_name!r}  ({env_key} not set)")
            continue
        Secret(value=val).save(name=block_name, overwrite=True)
        created.append(block_name)
        print(f"  ✅ {block_name!r}")

    print(f"\n✅ {len(created)} Secret blocks saved to Prefect server.")


def load_credentials_from_blocks() -> bool:
    """
    Load credentials from Prefect Secret blocks into env vars.
    Skips any block that is missing or if the env var is already set.
    Returns True if at least one value was loaded.
    """
    loaded = 0
    for block_name, (env_key, _) in _CREDENTIALS.items():
        if os.environ.get(env_key):
            continue
        try:
            val = Secret.load(block_name).get()
            if val:
                os.environ[env_key] = val
                loaded += 1
        except Exception:
            pass
    return loaded > 0


if __name__ == "__main__":
    setup_blocks()
