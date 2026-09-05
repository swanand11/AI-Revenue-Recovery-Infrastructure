#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-ai-revenue-recovery-infrastructure}"
export MOCK_SCENARIO="${MOCK_SCENARIO:-random_failure}"
export MOCK_INTERVAL_SECONDS="${MOCK_INTERVAL_SECONDS:-5}"
export SETTLEMENT_BATCH_INTERVAL_SECONDS="${SETTLEMENT_BATCH_INTERVAL_SECONDS:-15}"
export SETTLEMENT_OUTCOME_PATTERN="${SETTLEMENT_OUTCOME_PATTERN:-success}"

if [[ "${1:-}" == "--down" ]]; then
  docker compose down
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required" >&2
  exit 1
fi

if command -v gnome-terminal >/dev/null 2>&1; then
  TERMINAL="gnome-terminal"
elif command -v konsole >/dev/null 2>&1; then
  TERMINAL="konsole"
elif command -v xfce4-terminal >/dev/null 2>&1; then
  TERMINAL="xfce4-terminal"
elif command -v kitty >/dev/null 2>&1; then
  TERMINAL="kitty"
elif command -v xterm >/dev/null 2>&1; then
  TERMINAL="xterm"
else
  echo "No supported terminal emulator found (gnome-terminal, konsole, xfce4-terminal, kitty, or xterm)." >&2
  echo "Run the documented per-terminal commands in run.md instead." >&2
  exit 1
fi

launch_terminal() {
  local title="$1"
  local command="$2"
  local wrapped
  printf -v wrapped 'cd %q; echo; echo "=== %s ==="; %s; exit_code=$?; echo; echo "Process exited with code $exit_code. Press Enter to close."; read -r' "$ROOT_DIR" "$title" "$command"

  case "$TERMINAL" in
    gnome-terminal)
      gnome-terminal --title="$title" -- bash -lc "$wrapped" &
      ;;
    konsole)
      konsole --new-window -p tabtitle="$title" -e bash -lc "$wrapped" &
      ;;
    xfce4-terminal)
      xfce4-terminal --title="$title" --command="bash -lc \"$wrapped\"" &
      ;;
    kitty)
      kitty --title "$title" bash -lc "$wrapped" &
      ;;
    xterm)
      xterm -title "$title" -e bash -lc "$wrapped" &
      ;;
  esac
  sleep 0.4
}

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo "Building Compose images once..."
  docker compose build
fi

echo "Starting the RevTrace stack in separate log terminals using $TERMINAL..."

launch_terminal "01 Kafka" "docker compose up kafka kafka-init"
launch_terminal "02 Splunk" "docker compose up splunk"
launch_terminal "03 Mock pipeline" "docker compose up mock-pipeline"
launch_terminal "04 Ingestion bridge" "docker compose up ingestion-bridge"
launch_terminal "05 Detection service" "docker compose up detection-service"
launch_terminal "06 Redis" "docker compose up redis"
launch_terminal "07 Intent agent" "docker compose up intent-agent"
launch_terminal "08 Provider agent" "docker compose up provider-agent"
launch_terminal "09 Transaction agent" "docker compose up transaction-agent"
launch_terminal "10 Economics agent" "docker compose up economics-agent"
launch_terminal "11 Risk agent" "docker compose up risk-agent"
launch_terminal "12 Recovery API" "docker compose up recovery-api"
launch_terminal "13 Recovery service" "docker compose up recovery-service"
launch_terminal "14 Splunk forwarder" "docker compose up splunk-forwarder"
launch_terminal "15 Settlement service" "docker compose up settlement-service"
launch_terminal "16 Dashboard" "docker compose up dashboard"

echo
echo "All service terminals launched. Dashboard: http://localhost:8080"
echo "Stop the stack with: ./run.sh --down"
wait
