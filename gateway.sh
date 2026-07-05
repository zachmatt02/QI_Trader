#!/usr/bin/env bash
# gateway.sh - Manage the IBKR Client Portal Gateway and dashboard processes

LOG_FILE="gateway.log"
GATEWAY_CLASS="ibgroup.web.core.clientportal.gw.GatewayStart"
DASHBOARD_LOG="dashboard.log"
DASHBOARD_SCRIPT="dashboard.py"

stop_gateway() {
    # Find the process ID of the running Gateway Java application
    PID=$(pgrep -f "$GATEWAY_CLASS")
    if [ -n "$PID" ]; then
        echo "Stopping IBKR Gateway (PID: $PID)..."
        kill "$PID"
        
        # Wait for the process to terminate
        for i in {1..10}; do
            if ! ps -p "$PID" > /dev/null 2>&1; then
                echo "Gateway stopped successfully."
                return 0
            fi
            sleep 0.5
        done
        
        # Force kill if it hasn't stopped
        echo "Gateway did not stop. Forcing shutdown..."
        kill -9 "$PID"
        echo "Gateway force killed."
    else
        echo "Gateway is not running."
    fi
}

start_gateway() {
    # Check if already running
    PID=$(pgrep -f "$GATEWAY_CLASS")
    if [ -n "$PID" ]; then
        echo "Gateway is already running (PID: $PID)."
        return 0
    fi

    echo "Starting Gateway detached..."

    # Get the directory of this script
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    cd "$SCRIPT_DIR/clientportal" || exit 1

    # Run detached using nohup, writing logs to the parent directory
    nohup ./bin/run.sh root/conf.yaml > "$SCRIPT_DIR/$LOG_FILE" 2>&1 &

    cd "$SCRIPT_DIR" || exit 1

    # Wait briefly to check if the Java process starts successfully
    sleep 1.5
    NEW_PID=$(pgrep -f "$GATEWAY_CLASS")
    if [ -n "$NEW_PID" ]; then
        echo "Gateway started successfully in background (PID: $NEW_PID)."
        echo "Logs are being written to: $SCRIPT_DIR/$LOG_FILE"
    else
        echo "Warning: Gateway started in background, but the Java process was not immediately detected."
        echo "Please check the log file for errors: $SCRIPT_DIR/$LOG_FILE"
    fi
}

stop_dashboard() {
    PID=$(pgrep -f "$DASHBOARD_SCRIPT")
    if [ -n "$PID" ]; then
        echo "Stopping dashboard (PID: $PID)..."
        kill "$PID"

        for i in {1..10}; do
            if ! ps -p "$PID" > /dev/null 2>&1; then
                echo "Dashboard stopped successfully."
                return 0
            fi
            sleep 0.5
        done

        echo "Dashboard did not stop. Forcing shutdown..."
        kill -9 "$PID"
        echo "Dashboard force killed."
    else
        echo "Dashboard is not running."
    fi
}

start_dashboard() {
    # Check if already running
    PID=$(pgrep -f "$DASHBOARD_SCRIPT")
    if [ -n "$PID" ]; then
        echo "Dashboard is already running (PID: $PID). Stop it first (-e) to change DRY_RUN."
        return 0
    fi

    echo "Starting dashboard detached (DRY_RUN=$DRY_RUN)..."

    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    cd "$SCRIPT_DIR" || exit 1

    nohup env DRY_RUN="$DRY_RUN" ./venv/bin/python "$DASHBOARD_SCRIPT" > "$SCRIPT_DIR/$DASHBOARD_LOG" 2>&1 &

    sleep 1.5
    NEW_PID=$(pgrep -f "$DASHBOARD_SCRIPT")
    if [ -n "$NEW_PID" ]; then
        echo "Dashboard started successfully in background (PID: $NEW_PID)."
        echo "Logs are being written to: $SCRIPT_DIR/$DASHBOARD_LOG"
    else
        echo "Warning: Dashboard started in background, but the process was not immediately detected."
        echo "Please check the log file for errors: $SCRIPT_DIR/$DASHBOARD_LOG"
    fi
}

# Handle arguments
DRY_RUN=0   # orders enabled by default; pass -dr to run in preview-only mode
for arg in "$@"; do
  case $arg in
    -e)
      stop_dashboard
      stop_gateway
      exit 0
      ;;
    -dr)
      DRY_RUN=1
      ;;
    *)
      echo "Usage: $0 [-e] [-dr]"
      echo "  (No arguments): Starts the gateway and dashboard detached (DRY_RUN off — orders can be submitted)."
      echo "  -dr           : Starts with DRY_RUN on (preview only, order submission disabled)."
      echo "  -e            : Exits/stops the running gateway and dashboard."
      exit 1
      ;;
  esac
done

# If no arguments/options are provided, start the gateway and dashboard
start_gateway
start_dashboard
