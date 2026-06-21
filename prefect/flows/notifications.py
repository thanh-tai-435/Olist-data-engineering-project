"""
Shared Prefect state-change hooks.

Usage:
    from notifications import notify_failure

    @flow(on_failure=[notify_failure])
    def my_flow(): ...

Set NOTIFY_WEBHOOK_URL (env var) to receive alerts via Slack / Discord / any
incoming-webhook endpoint. Without it, failures are only printed to logs.
"""
import os

import requests


def notify_failure(flow, flow_run, state) -> None:
    """Log failure + POST to NOTIFY_WEBHOOK_URL if configured."""
    msg = (
        f"❌ Prefect flow *{flow.name}* failed\n"
        f"Run: `{flow_run.name}`\n"
        f"Error: {state.message or '(no message)'}"
    )
    print(msg)

    webhook = os.environ.get("NOTIFY_WEBHOOK_URL")
    if not webhook:
        return
    try:
        requests.post(webhook, json={"text": msg}, timeout=10)
    except Exception as exc:
        print(f"[notify_failure] webhook POST failed: {exc}")
