#!/bin/bash
set -e
COMPOSE_FILE="middleware/test/docker-compose.yml"

docker compose -f "$COMPOSE_FILE" down > /dev/null 2>&1