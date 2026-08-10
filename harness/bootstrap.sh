#!/usr/bin/env bash
# Wait for the harness stack to be ready, then create the TiCDC changefeed that
# sinks canal-json to Pulsar. Safe to re-run: an existing changefeed is left
# alone rather than recreated.
set -euo pipefail

TOPIC="${TOPIC:-cdcfresh}"
CHANGEFEED_ID="${CHANGEFEED_ID:-cdcfresh}"
TIMEOUT="${TIMEOUT:-180}"
cd "$(dirname "$0")"

wait_for() {
	local name=$1 probe=$2 deadline=$((SECONDS + TIMEOUT))
	printf 'waiting for %s' "$name"
	until eval "$probe" >/dev/null 2>&1; do
		if ((SECONDS >= deadline)); then
			printf ' timed out after %ss\n' "$TIMEOUT" >&2
			return 1
		fi
		printf '.'
		sleep 2
	done
	printf ' ready\n'
}

wait_for "TiDB"   'curl -sf http://127.0.0.1:10080/status'
wait_for "TiCDC"  'curl -sf http://127.0.0.1:8300/status'
wait_for "Pulsar" '[ "$(curl -sf http://127.0.0.1:8080/admin/v2/brokers/health)" = "ok" ]'

# TiCDC reaches Pulsar over the compose network, not the host ports.
SINK_URI="pulsar://pulsar:6650/persistent://public/default/${TOPIC}?protocol=canal-json"

if docker compose exec -T ticdc /cdc cli changefeed query \
	--server=http://127.0.0.1:8300 --changefeed-id="$CHANGEFEED_ID" >/dev/null 2>&1; then
	echo "changefeed '$CHANGEFEED_ID' already exists — leaving it alone"
else
	echo "creating changefeed '$CHANGEFEED_ID' -> $SINK_URI"
	docker compose exec -T ticdc /cdc cli changefeed create \
		--server=http://127.0.0.1:8300 \
		--changefeed-id="$CHANGEFEED_ID" \
		--sink-uri="$SINK_URI"
fi

docker compose exec -T ticdc /cdc cli changefeed list --server=http://127.0.0.1:8300

cat <<EOF

harness ready
  TiDB    mysql://root@127.0.0.1:4000/
  TiCDC   http://127.0.0.1:8300
  Pulsar  pulsar://127.0.0.1:6650  (admin http://127.0.0.1:8080)
  topic   persistent://public/default/${TOPIC}
EOF
