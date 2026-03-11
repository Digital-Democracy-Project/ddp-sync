#!/bin/bash
# Quick health check for all DDP services
# Usage: ./check-services.sh

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

check_service() {
    local name=$1
    local url=$2
    local response
    response=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "$url")
    if [ "$response" = "200" ]; then
        echo -e "${GREEN}✓${NC} $name ($url) — HTTP $response"
    else
        echo -e "${RED}✗${NC} $name ($url) — HTTP $response"
    fi
}

echo "=== DDP Service Health ==="
check_service "DDP-API"   "http://localhost:5000/health"
check_service "VoteBot"   "http://localhost:8000/votebot/v1/health"
check_service "DDP-Sync"  "http://localhost:8001/ddp-sync/v1/health"

echo ""
echo "=== Systemd Status ==="
for svc in ddp-api votebot ddp-sync; do
    status=$(systemctl is-active $svc 2>/dev/null)
    if [ "$status" = "active" ]; then
        echo -e "${GREEN}✓${NC} $svc — $status"
    else
        echo -e "${RED}✗${NC} $svc — $status"
    fi
done

echo ""
echo "=== Scheduler ==="
curl -s http://localhost:8001/ddp-sync/v1/schedule | python3 -m json.tool 2>/dev/null || echo "Could not reach scheduler"
