#!/usr/bin/env python3
r"""Provision the Apple Box Conveyor Quality Inspector demo without PostgreSQL.

Required env vars:
  OPENAI_API_KEY     — OpenAI API key

Optional env vars (for this init script):
  EMQX_BASE_URL      — EMQX server base URL (default: http://localhost:18083/)
  EMQX_API_CREDS     — Basic-auth "key:secret" (default: key:secret)
  OPENAI_BASE_URL    — OpenAI-compatible base URL (default: https://api.openai.com/v1)
  OPENAI_MODEL       — Model name              (default: gpt-5.4-mini)

Usage:
  EMQX_BASE_URL='http://localhost:18083/' \
  EMQX_API_CREDS='key:secret' \
  OPENAI_API_KEY='your-openai-api-key' \
  OPENAI_BASE_URL='https://api.openai.com/v1' \
  OPENAI_MODEL='gpt-5.4-mini' \
  python3 demo_apple_box_nopg_init.py
"""

import base64
import json
import os
import sys
import urllib.error
import urllib.request


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name)
    if value is None:
        if default is None:
            raise RuntimeError(f"required env var is missing: {name}")
        return default
    return value


EMQX_BASE_URL = env("EMQX_BASE_URL", "http://localhost:18083/").rstrip("/")
CORE_BASE_URL = f"{EMQX_BASE_URL}/api/v5"
BASE_URL = f"{CORE_BASE_URL}/plugin_api/emqx_agent"
CREDS = env("EMQX_API_CREDS", "key:secret")

OPENAI_BASE_URL = env("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_MODEL = env("OPENAI_MODEL", "gpt-5.4-mini")
OPENAI_API_KEY = env("OPENAI_API_KEY")

PROVIDER_NAME = "apple-inspector"
PIPELINE_ID = "apple-box-inspection"

SK_SHOT = "box-shot"
SK_ALERT = "box-alert"
SK_STATUS = "box-status"


def empty_object_schema() -> dict:
    return {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    }


def inspection_schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "status": {"type": "string", "enum": ["approved", "rejected"]},
            "reason": {"type": "string"},
        },
        "required": ["status", "reason"],
        "additionalProperties": False,
    }


def alert_payload_schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "reason": {"type": "string"},
            "defect_type": {
                "type": "array",
                "items": {"type": "string"},
            },
            "severity": {"type": "string", "enum": ["low", "medium", "high"]},
        },
        "required": ["reason", "defect_type", "severity"],
        "additionalProperties": False,
    }


# ── HTTP helpers ──────────────────────────────────────────────────────────────


def auth_header() -> str:
    raw = CREDS.encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def api_request(
    method: str,
    path: str,
    body: dict | None = None,
    *,
    base_url: str = BASE_URL,
    ok_codes: tuple[int, ...] = (200, 201, 204),
):
    url = f"{base_url}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url=url, method=method, data=data)
    req.add_header("Authorization", auth_header())
    if body is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = resp.read().decode("utf-8")
            print(f"  HTTP {method} {path} -> {resp.status}")
            if resp.status not in ok_codes:
                raise RuntimeError(
                    f"{method} {path} failed: HTTP {resp.status}: {payload}"
                )
            return payload
    except urllib.error.HTTPError as e:
        payload = e.read().decode("utf-8", errors="replace")
        print(f"  HTTP {method} {path} -> {e.code}")
        if e.code in ok_codes:
            return payload
        raise RuntimeError(f"{method} {path} failed: HTTP {e.code}: {payload}") from e


def api_delete_maybe(path: str, *, base_url: str = BASE_URL) -> None:
    """DELETE the resource; ignore 404."""
    try:
        api_request("DELETE", path, base_url=base_url, ok_codes=(200, 204, 404))
    except RuntimeError:
        pass


def deactivate_pipeline_maybe(pid: str) -> None:
    try:
        payload = api_request("GET", f"/pipelines/{pid}")
    except RuntimeError:
        return
    pipeline = json.loads(payload)
    if pipeline.get("active"):
        api_request("PUT", f"/pipelines/{pid}", {**pipeline, "active": False})


# ── Tools ─────────────────────────────────────────────────────────────────────


def delete_old_assets() -> None:
    deactivate_pipeline_maybe(PIPELINE_ID)
    api_delete_maybe(f"/pipelines/{PIPELINE_ID}")
    api_delete_maybe(f"/tools/message__request/{SK_SHOT}")
    api_delete_maybe(f"/tools/message__publish/{SK_ALERT}")
    api_delete_maybe(f"/tools/message__publish/{SK_STATUS}")
    api_delete_maybe(f"/ai/providers/{PROVIDER_NAME}", base_url=CORE_BASE_URL)


def create_ai_provider() -> None:
    api_delete_maybe(f"/ai/providers/{PROVIDER_NAME}", base_url=CORE_BASE_URL)
    api_request(
        "POST",
        "/ai/providers",
        {
            "name": PROVIDER_NAME,
            "type": "openai",
            "api_key": OPENAI_API_KEY,
            "base_url": OPENAI_BASE_URL,
        },
        base_url=CORE_BASE_URL,
        ok_codes=(200, 201, 204),
    )
    print(f"  AI provider {PROVIDER_NAME!r} created")


def create_tools() -> None:
    api_request(
        "POST",
        "/tools",
        {
            "type": "message__request",
            "id": SK_SHOT,
            "desc": "Request a box snapshot photo from the SPA client",
            "topic_prefix": "box/shot/",
            "request_payload_schema": json.dumps(empty_object_schema()),
        },
    )
    print(f"  tool {SK_SHOT!r} created")

    api_request(
        "POST",
        "/tools",
        {
            "type": "message__publish",
            "id": SK_ALERT,
            "desc": "Publish a box quality alert to the SPA",
            "topic_prefix": "box/alert/",
            "payload_schema": json.dumps(alert_payload_schema()),
        },
    )
    print(f"  tool {SK_ALERT!r} created")

    api_request(
        "POST",
        "/tools",
        {
            "type": "message__publish",
            "id": SK_STATUS,
            "desc": "Publish final box inspection status to the SPA",
            "topic_prefix": "box/status/",
            "payload_schema": json.dumps(inspection_schema()),
        },
    )
    print(f"  tool {SK_STATUS!r} created")


INSPECTOR_INSTRUCTIONS = (
    "You are an apple quality inspector for a conveyor line. "
    "You will receive box_id and conveyor_id as input. "
    "Use the message_request_box_shot tool with topic=box_id "
    "to request a photo of the crate. "
    "Carefully examine the photo for rotten, moldy, bruised, or damaged apples. "
    "If you detect any defects, call message_publish_box_alert with: "
    "  reason (string), defect_type (list of strings), severity (low/medium/high). "
    "When you have reached a verdict, call set_result with: "
    "  status: 'approved' if all apples look fresh and healthy, "
    "          'rejected' if any defect is found; "
    "  reason: one sentence explaining your decision."
)


def create_pipeline() -> None:
    api_request(
        "POST",
        "/pipelines",
        {
            "pipeline_id": PIPELINE_ID,
            "active": True,
            "trigger": {"topic": "$evt/conveyor/+/box/done"},
            "steps": [
                {
                    "id": "inspect",
                    "type": "llm_loop",
                    "provider_name": PROVIDER_NAME,
                    "model": OPENAI_MODEL,
                    "persistent": False,
                    "instructions": INSPECTOR_INSTRUCTIONS,
                    "tools": [
                        f"message__request@{SK_SHOT}",
                        f"message__publish@{SK_ALERT}",
                    ],
                    "input": {
                        "box_id": "$.event.box_id",
                        "conveyor_id": "$.event.conveyor_id",
                    },
                    "set_result_schema": inspection_schema(),
                    "result_path": "$.inspection",
                },
                {
                    "id": "notify",
                    "type": "call_tool",
                    "tool": f"message__publish@{SK_STATUS}",
                    "args": {
                        "topic": "$.event.box_id",
                        "payload": "$.inspection",
                    },
                    "result_path": "$.notify_result",
                },
            ],
        },
    )
    print(f"  pipeline {PIPELINE_ID!r} created")


def main() -> int:
    print("==> Removing any existing apple-box assets")
    delete_old_assets()

    print("==> Creating tools")
    create_tools()

    print("==> Creating AI provider")
    create_ai_provider()

    print("==> Creating pipeline")
    create_pipeline()

    print("\nDone.")
    print(f"SPA UI:    {BASE_URL.rstrip('/')}/apple-box/ui")
    print(f"Admin UI:  {BASE_URL.rstrip('/')}/ui")
    print(f"Pipeline:  {PIPELINE_ID}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)
