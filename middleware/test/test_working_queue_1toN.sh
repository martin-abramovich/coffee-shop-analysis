#!/bin/bash
set -e

COMPOSE_FILE="middleware/test/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" up -d consumer1 consumer2 > /dev/null 2>&1
sleep 2

docker compose -f "$COMPOSE_FILE" run --rm producer > /dev/null 2>&1
sleep 2

LOGS1=$(docker compose -f "$COMPOSE_FILE" logs consumer1)
LOGS2=$(docker compose -f "$COMPOSE_FILE" logs consumer2)

COUNT1=$(echo "$LOGS1" | grep -c "msg-")
COUNT2=$(echo "$LOGS2" | grep -c "msg-")
TOTAL=$((COUNT1+COUNT2))

if [ $TOTAL -eq 4 ] && [ $COUNT1 -ge 1 ] && [ $COUNT2 -ge 1 ]; then
    echo "✅ Test Working Queue 1→N PASÓ"
    docker compose -f "$COMPOSE_FILE" down consumer1 consumer2 producer > /dev/null 2>&1
    exit 0
else
    echo "❌ Test Working Queue 1→N FALLÓ"
    echo "Logs consumer1:"
    echo "$LOGS1"
    echo "Logs consumer2:"
    echo "$LOGS2"
    docker compose -f "$COMPOSE_FILE" down consumer1 consumer2 producer > /dev/null 2>&1
    exit 1
fi
