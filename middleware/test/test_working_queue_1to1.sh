#!/bin/bash
set -e

COMPOSE_FILE="middleware/test/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" up -d consumer1 > /dev/null 2>&1
sleep 2

docker compose -f "$COMPOSE_FILE" run --rm producer > /dev/null 2>&1

sleep 2  

LOGS=$(docker compose -f "$COMPOSE_FILE" logs consumer1)
LOGSP=$(docker compose -f "$COMPOSE_FILE" logs producer)


if echo "$LOGS" | grep -q "msg-0"; then
    echo "✅ Test Working Queue 1→1 PASÓ"
    docker compose -f "$COMPOSE_FILE" down consumer1 producer > /dev/null 2>&1
    exit 0
else
    echo "❌ Test Working Queue 1→1 FALLÓ"
    echo "Logs del consumidor:"
    echo "$LOGS"
    echo "Logs del producer:"
    echo "$LOGSP"
    docker compose -f "$COMPOSE_FILE" down consumer1 producer > /dev/null 2>&1
    exit 1
fi
