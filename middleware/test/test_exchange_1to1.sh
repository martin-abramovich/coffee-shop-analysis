#!/bin/bash
set -e

COMPOSE_FILE="middleware/test/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" up -d consumer1_exchange > /dev/null 2>&1
docker compose -f "$COMPOSE_FILE" run --rm producer_exchange > /dev/null 2>&1
sleep 2

LOGS=$(docker compose -f "$COMPOSE_FILE" logs consumer1_exchange)

if echo "$LOGS" | grep -q "msg-0"; then
    echo "✅ Test Exchange 1→1 PASÓ"
    docker compose -f "$COMPOSE_FILE" down consumer1_exchange > /dev/null 2>&1
    exit 0
else
    echo "❌ Test Exchange 1→1 FALLÓ"
    echo "$LOGS"
    docker compose -f "$COMPOSE_FILE" down consumer1_exchange producer_exchange > /dev/null 2>&1
    exit 1
fi
