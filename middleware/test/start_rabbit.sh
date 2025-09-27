#!/bin/bash
set -e
COMPOSE_FILE="middleware/test/docker-compose.yml"

# Levanta RabbitMQ solo
docker compose -f "$COMPOSE_FILE" up -d rabbitmq > /dev/null 2>&1

# Espera a que esté listo
echo "⏳ Esperando a que RabbitMQ inicialice..."
