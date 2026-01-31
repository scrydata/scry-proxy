#!/bin/bash
# Profile scry-proxy using perf and generate flamegraphs
#
# Usage:
#   ./profile-scry.sh [duration_seconds]
#
# Prerequisites:
#   - Docker compose services running with profiling overlay
#   - cd benchmarks
#   - docker compose -f docker-compose.yml -f docker-compose.profile.yml up -d

set -e

DURATION=${1:-30}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
PROFILE_NAME="scry_profile_${TIMESTAMP}"

echo "=== Scry Proxy CPU Profiler ==="
echo "Duration: ${DURATION}s"
echo "Output: profiles/${PROFILE_NAME}.svg"
echo ""

# Check if scry container is running
if ! docker compose -f docker-compose.yml -f docker-compose.profile.yml ps scry | grep -q "running"; then
    echo "Error: scry container not running"
    echo "Start with: docker compose -f docker-compose.yml -f docker-compose.profile.yml up -d"
    exit 1
fi

echo "Starting perf recording..."
echo "(Run your benchmark in another terminal now)"
echo ""

# Record perf data inside the container
docker compose -f docker-compose.yml -f docker-compose.profile.yml exec scry bash -c "
    cd /profiles
    # Find the scry-proxy PID
    PID=\$(pgrep scry-proxy)
    if [ -z \"\$PID\" ]; then
        echo 'Error: scry-proxy process not found'
        exit 1
    fi
    echo \"Profiling PID \$PID for ${DURATION} seconds...\"

    # Record with call graphs (-g), targeting specific PID
    perf record -F 99 -g -p \$PID -o perf.data -- sleep ${DURATION}

    echo 'Generating flamegraph...'
    perf script -i perf.data | inferno-collapse-perf | inferno-flamegraph > ${PROFILE_NAME}.svg

    echo 'Done!'
    ls -la ${PROFILE_NAME}.svg
"

echo ""
echo "=== Profile Complete ==="
echo "Flamegraph saved to: benchmarks/profiles/${PROFILE_NAME}.svg"
echo "Open in a browser to explore the results"
