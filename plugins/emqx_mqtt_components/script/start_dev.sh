#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"
EMQX_DIR="$ROOT_DIR/_build/$PROFILE/rel/emqx"
EMQX_BIN="$EMQX_DIR/bin/emqx"
EMQX_HOST="${EMQX_HOST:-box2}"

cd "$ROOT_DIR"
if [[ -x "$EMQX_BIN" ]]; then
    echo "Stopping previous EMQX node if running..."
    "$EMQX_BIN" stop >/dev/null 2>&1 || true
fi

PROFILE="$PROFILE" make
EMQX_DASHBOARD__LISTENERS__HTTP__BIND="0.0.0.0:18083" \
EMQX_DASHBOARD__LISTENERS__HTTPS__BIND="0.0.0.0:18084" \
EMQX_LISTENERS__TCP__DEFAULT__BIND="0.0.0.0:1883" \
EMQX_LISTENERS__SSL__DEFAULT__BIND="0.0.0.0:8883" \
EMQX_LISTENERS__WS__DEFAULT__BIND="0.0.0.0:8083" \
EMQX_LISTENERS__WSS__DEFAULT__BIND="0.0.0.0:8084" \
EMQX_LISTENERS__TCP__DEFAULT__ENABLE_AUTHN=false \
EMQX_LISTENERS__SSL__DEFAULT__ENABLE_AUTHN=false \
EMQX_LISTENERS__WS__DEFAULT__ENABLE_AUTHN=false \
EMQX_LISTENERS__WSS__DEFAULT__ENABLE_AUTHN=false \
EMQX_AUTHORIZATION__SOURCES='[]' \
EMQX_AUTHORIZATION__NO_MATCH=allow \
PROFILE="$PROFILE" ./scripts/run-plugin-dev.sh emqx_mqtt_components "$@"

echo "MQTT Components UI: http://$EMQX_HOST:18083/api/v5/plugin_api/emqx_mqtt_components/ui"

exec tail -F "$EMQX_DIR/log/emqx.log".{1,2,3,4,5}
