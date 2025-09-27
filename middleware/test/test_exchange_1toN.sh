#!/bin/bash
set -e

COMPOSE_FILE="middleware/test/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" up -d consumer1_exchange consumer2_exchange > /dev/null 2>&1
docker compose -f "$COMPOSE_FILE" run --rm producer_exchange > /dev/null 2>&1
sleep 3

LOGS1=$(docker compose -f "$COMPOSE_FILE" logs consumer1_exchange)
LOGS2=$(docker compose -f "$COMPOSE_FILE" logs consumer2_exchange)

if echo "$LOGS1" | grep -q "msg-0" && echo "$LOGS2" | grep -q "msg-0"; then
    echo "✅ Test Exchange 1→N PASÓ"
    docker compose -f "$COMPOSE_FILE" down consumer1_exchange consumer2_exchange producer_exchange > /dev/null 2>&1
    exit 0
else
    echo "❌ Test Exchange 1→N FALLÓ"
    echo "Logs consumer1:"
    echo "$LOGS1"
    echo "Logs consumer2:"
    echo "$LOGS2"
    docker compose -f "$COMPOSE_FILE" down consumer1_exchange consumer2_exchange producer_exchange > /dev/null 2>&1
    exit 1
fi
