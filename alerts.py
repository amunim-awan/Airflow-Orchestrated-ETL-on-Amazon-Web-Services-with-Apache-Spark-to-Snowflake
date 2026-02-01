"""Notifications (Slack webhook)."""
from __future__ import annotations

import json
import requests


def notify_slack(webhook_url: str, message: str) -> None:
    if not webhook_url:
        return
    payload = {"text": message}
    resp = requests.post(webhook_url, data=json.dumps(payload), headers={"Content-Type": "application/json"}, timeout=20)
    resp.raise_for_status()
