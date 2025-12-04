#!/bin/bash

# Script para generar docker-compose.yml con escalabilidad configurable
# Uso: ./generate-docker-compose.sh [opciones]
#
# Opciones:
#   --filter-year N       Número de workers filter_year (default: 3)
#   --filter-hour N       Número de workers filter_hour (default: 2)
#   --filter-amount N     Número de workers filter_amount (default: 2)
#   --group-by-q2 N       Número de workers group_by_query2 (default: 2)
#   --group-by-q3 N       Número de workers group_by_query3 (default: 2)
#   --group-by-q4 N       Número de workers group_by_query4 (default: 2)
#   -h, --help            Mostrar ayuda

set -e

# Valores por defecto
NUM_FILTER_YEAR=3
NUM_FILTER_HOUR=2
NUM_FILTER_AMOUNT=2
NUM_GROUP_BY_Q2=2
NUM_GROUP_BY_Q3=2
NUM_GROUP_BY_Q4=2

# Parsear argumentos
while [[ $# -gt 0 ]]; do
    case $1 in
        --filter-year)
            NUM_FILTER_YEAR="$2"
            shift 2
            ;;
        --filter-hour)
            NUM_FILTER_HOUR="$2"
            shift 2
            ;;
        --filter-amount)
            NUM_FILTER_AMOUNT="$2"
            shift 2
            ;;
        --group-by-q2)
            NUM_GROUP_BY_Q2="$2"
            shift 2
            ;;
        --group-by-q3)
            NUM_GROUP_BY_Q3="$2"
            shift 2
            ;;
        --group-by-q4)
            NUM_GROUP_BY_Q4="$2"
            shift 2
            ;;
        -h|--help)
            echo "Uso: $0 [opciones]"
            echo ""
            echo "Opciones:"
            echo "  --filter-year N       Número de workers filter_year (default: 3)"
            echo "  --filter-hour N       Número de workers filter_hour (default: 2)"
            echo "  --filter-amount N     Número de workers filter_amount (default: 2)"
            echo "  --group-by-q2 N       Número de workers group_by_query2 (default: 2)"
            echo "  --group-by-q3 N       Número de workers group_by_query3 (default: 2)"
            echo "  --group-by-q4 N       Número de workers group_by_query4 (default: 2)"
            echo "  -h, --help            Mostrar ayuda"
            echo ""
            echo "Ejemplo:"
            echo "  $0 --filter-year 5 --filter-hour 3 --filter-amount 3"
            exit 0
            ;;
        *)
            echo "Error: Opción desconocida: $1"
            echo "Usa -h o --help para ver las opciones disponibles"
            exit 1
            ;;
    esac
done

OUTPUT_FILE="docker-compose.yml"

echo "Generando docker-compose.yml con:"
echo "  - Filter Year workers: $NUM_FILTER_YEAR"
echo "  - Filter Hour workers: $NUM_FILTER_HOUR"
echo "  - Filter Amount workers: $NUM_FILTER_AMOUNT"
echo "  - GroupBy Query2 workers: $NUM_GROUP_BY_Q2"
echo "  - GroupBy Query3 workers: $NUM_GROUP_BY_Q3"
echo "  - GroupBy Query4 workers: $NUM_GROUP_BY_Q4"
echo ""

# Generar docker-compose.yml
cat > "$OUTPUT_FILE" << 'EOF_HEADER'
version: "3.8"

services:
  # ==================== MIDDLEWARE ====================
  rabbitmq:
    image: rabbitmq:3-management
    container_name: coffee-rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
      RABBITMQ_LOG_LEVEL: warning
      RABBITMQ_HEARTBEAT: 300  # 5 minutes instead of default 60s
    command: ["rabbitmq-server", "--log-level", "warning"]
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "check_port_connectivity"]
      interval: 5s
      timeout: 10s
      retries: 10
      start_period: 30s
    volumes:
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf
    networks:
      - coffee-network

  # ==================== GATEWAY ====================
  gateway:
    build:
      context: .
      dockerfile: Dockerfile.gateway
    container_name: coffee-gateway
    labels:
      - "role=node"
    ports:
      - "9000:9000"
      - "8888:8888/udp"  # Puerto UDP para healthcheck
    environment:
      RABBITMQ_HOST: ${RABBITMQ_HOST}
      PYTHONUNBUFFERED: ${PYTHONUNBUFFERED}
      HEALTHCHECK_PORT: ${HEALTHCHECK_PORT:-8888}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee-network
    volumes:
      - ./results:/app/results

  # ==================== FILTERS ====================
EOF_HEADER

# Generar Filter Year Workers
echo "  # Filter Year Workers - Auto-generados: $NUM_FILTER_YEAR workers" >> "$OUTPUT_FILE"
for i in $(seq 0 $((NUM_FILTER_YEAR - 1))); do
    cat >> "$OUTPUT_FILE" << EOF_FILTER_YEAR
  filter_year_$i:
    build:
      context: .
      dockerfile: Dockerfile.worker
    container_name: coffee-filter-year-$i
    labels:
      - "role=node"
    command: ["python", "-m", "workers.filter.filter_year"]
    environment:
      RABBITMQ_HOST: \${RABBITMQ_HOST}
      PYTHONUNBUFFERED: \${PYTHONUNBUFFERED}
      HEALTHCHECK_PORT: \${HEALTHCHECK_PORT:-8888}
      WORKER_ID: $i
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee-network

EOF_FILTER_YEAR
done

# Generar Filter Hour Workers
echo "  # Filter Hour Workers - Auto-generados: $NUM_FILTER_HOUR workers" >> "$OUTPUT_FILE"
for i in $(seq 0 $((NUM_FILTER_HOUR - 1))); do
    cat >> "$OUTPUT_FILE" << EOF_FILTER_HOUR
  filter_hour_$i:
    build:
      context: .
      dockerfile: Dockerfile.worker
    container_name: coffee-filter-hour-$i
    labels:
      - "role=node"
    command: ["python", "-m", "workers.filter.filter_hour"]
    environment:
      RABBITMQ_HOST: \${RABBITMQ_HOST}
      PYTHONUNBUFFERED: \${PYTHONUNBUFFERED}
      HEALTHCHECK_PORT: \${HEALTHCHECK_PORT:-8888}
      WORKER_ID: $i
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee-network

EOF_FILTER_HOUR
done

# Generar Filter Amount Workers
echo "  # Filter Amount Workers - Auto-generados: $NUM_FILTER_AMOUNT workers" >> "$OUTPUT_FILE"
for i in $(seq 0 $((NUM_FILTER_AMOUNT - 1))); do
    cat >> "$OUTPUT_FILE" << EOF_FILTER_AMOUNT
  filter_amount_$i:
    build:
      context: .
      dockerfile: Dockerfile.worker
    container_name: coffee-filter-amount-$i
    labels:
      - "role=node"
    command: ["python", "-m", "workers.filter.filter_amount"]
    environment:
      RABBITMQ_HOST: \${RABBITMQ_HOST}
      PYTHONUNBUFFERED: \${PYTHONUNBUFFERED}
      HEALTHCHECK_PORT: \${HEALTHCHECK_PORT:-8888}
      WORKER_ID: $i
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee-network

EOF_FILTER_AMOUNT
done

# Generar GroupBy Workers
cat >> "$OUTPUT_FILE" << 'EOF_GROUPBY_HEADER'
  # ==================== GROUP BY ====================
EOF_GROUPBY_HEADER

echo "  # Group By Query2 Workers - Auto-generados: $NUM_GROUP_BY_Q2 workers" >> "$OUTPUT_FILE"
for i in $(seq 0 $((NUM_GROUP_BY_Q2 - 1))); do
    cat >> "$OUTPUT_FILE" << EOF_GROUPBY_Q2
  group_by_query2_$i:
    build:
      context: .
      dockerfile: Dockerfile.worker
    container_name: coffee-group-by-query2-$i
    labels:
      - "role=node"
    command: ["python", "-m", "workers.group_by.group_by_query2"]
    environment:
      RABBITMQ_HOST: \${RABBITMQ_HOST}
      PYTHONUNBUFFERED: \${PYTHONUNBUFFERED}
      HEALTHCHECK_PORT: \${HEALTHCHECK_PORT:-8888}
      WORKER_ID: $i
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee-network

EOF_GROUPBY_Q2
done

echo "  # Group By Query3 Workers - Auto-generados: $NUM_GROUP_BY_Q3 workers" >> "$OUTPUT_FILE"
for i in $(seq 0 $((NUM_GROUP_BY_Q3 - 1))); do
    cat >> "$OUTPUT_FILE" << EOF_GROUPBY_Q3
  group_by_query3_$i:
    build:
      context: .
      dockerfile: Dockerfile.worker
    container_name: coffee-group-by-query3-$i
    labels:
      - "role=node"
    command: ["python", "-m", "workers.group_by.group_by_query3"]
    environment:
      RABBITMQ_HOST: \${RABBITMQ_HOST}
      PYTHONUNBUFFERED: \${PYTHONUNBUFFERED}
      HEALTHCHECK_PORT: \${HEALTHCHECK_PORT:-8888}
      WORKER_ID: $i
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee-network

EOF_GROUPBY_Q3
done

echo "  # Group By Query4 Workers - Auto-generados: $NUM_GROUP_BY_Q4 workers" >> "$OUTPUT_FILE"
for i in $(seq 0 $((NUM_GROUP_BY_Q4 - 1))); do
    cat >> "$OUTPUT_FILE" << EOF_GROUPBY_Q4
  group_by_query4_$i:
    build:
      context: .
      dockerfile: Dockerfile.worker
    container_name: coffee-group-by-query4-$i
    labels:
      - "role=node"
    command: ["python", "-m", "workers.group_by.group_by_query4"]
    environment:
      RABBITMQ_HOST: \${RABBITMQ_HOST}
      PYTHONUNBUFFERED: \${PYTHONUNBUFFERED}
      HEALTHCHECK_PORT: \${HEALTHCHECK_PORT:-8888}
      WORKER_ID: $i
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee-network

EOF_GROUPBY_Q4
done

# Agregar resto de servicios (aggregators y monitors - no escalables)
cat >> "$OUTPUT_FILE" << 'EOF_FOOTER'
  # ==================== AGGREGATORS ====================
  aggregator_query1:
    build:
      context: .
      dockerfile: Dockerfile.worker
    container_name: coffee-aggregator-query1
    labels:
      - "role=node"
    command: ["python", "-m", "workers.aggregator.aggregator_query1"]
    environment:
      RABBITMQ_HOST: ${RABBITMQ_HOST}
      PYTHONUNBUFFERED: ${PYTHONUNBUFFERED}
      HEALTHCHECK_PORT: ${HEALTHCHECK_PORT:-8888}
      LOG_LEVEL: ${LOG_LEVEL:-INFO}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee-network
    volumes:
    - ./state/agregator1:/app/state

  aggregator_query2:
    build:
      context: .
      dockerfile: Dockerfile.worker
    container_name: coffee-aggregator-query2
    labels:
      - "role=node"
    command: ["python", "-m", "workers.aggregator.aggregator_query2"]
    environment:
      RABBITMQ_HOST: ${RABBITMQ_HOST}
      PYTHONUNBUFFERED: ${PYTHONUNBUFFERED}
      HEALTHCHECK_PORT: ${HEALTHCHECK_PORT:-8888}
      LOG_LEVEL: ${LOG_LEVEL:-INFO}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee-network
    volumes:
    - ./state/agregator2:/app/state

  aggregator_query3:
    build:
     context: .
     dockerfile: Dockerfile.worker
    container_name: coffee-aggregator-query3
    labels:
      - "role=node"
    command: ["python", "-m", "workers.aggregator.aggregator_query3"]
    environment:
      RABBITMQ_HOST: ${RABBITMQ_HOST}
      PYTHONUNBUFFERED: ${PYTHONUNBUFFERED}
      HEALTHCHECK_PORT: ${HEALTHCHECK_PORT:-8888}
      LOG_LEVEL: ${LOG_LEVEL:-INFO}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee-network
    volumes:
    - ./state/agregator3:/app/state

  aggregator_query4:
    build:
     context: .
     dockerfile: Dockerfile.worker
    container_name: coffee-aggregator-query4
    labels:
      - "role=node"
    command: ["python", "-m", "workers.aggregator.aggregator_query4"]
    environment:
      RABBITMQ_HOST: ${RABBITMQ_HOST}
      PYTHONUNBUFFERED: ${PYTHONUNBUFFERED}
      HEALTHCHECK_PORT: ${HEALTHCHECK_PORT:-8888}
      LOG_LEVEL: ${LOG_LEVEL:-INFO}
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - coffee-network
    volumes:
      - ./state/agregator4:/app/state

  # ==================== MONITORS (Redundantes) ====================
  # 3 réplicas de monitores con algoritmo Bully para elección de líder
  monitor_1:
    build:
      context: .
      dockerfile: Dockerfile.monitor
    container_name: coffee-monitor-1
    environment:
      PYTHONUNBUFFERED: ${PYTHONUNBUFFERED}
      MONITOR_ID: 1
      MONITOR_TCP_PORT: 9999
      HEALTHCHECK_PORT: ${HEALTHCHECK_PORT:-8888}
      CHECK_INTERVAL: ${CHECK_INTERVAL:-5.0}
      TIMEOUT: ${TIMEOUT:-2.0}
      MAX_FAILED_ATTEMPTS: ${MAX_FAILED_ATTEMPTS:-3}
      HEARTBEAT_INTERVAL: ${HEARTBEAT_INTERVAL:-3.0}
      LEADER_TIMEOUT: ${LEADER_TIMEOUT:-10.0}
      # MONITORS: "1:monitor_1:9999,2:monitor_2:10000,3:monitor_3:10001"
    depends_on:
      - gateway
    networks:
      - coffee-network
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock  # Socket Docker para revivir contenedores
    restart: unless-stopped

  monitor_2:
    build:
      context: .
      dockerfile: Dockerfile.monitor
    container_name: coffee-monitor-2
    environment:
      PYTHONUNBUFFERED: ${PYTHONUNBUFFERED}
      MONITOR_ID: 2
      MONITOR_TCP_PORT: 10000
      HEALTHCHECK_PORT: ${HEALTHCHECK_PORT:-8888}
      CHECK_INTERVAL: ${CHECK_INTERVAL:-5.0}
      TIMEOUT: ${TIMEOUT:-2.0}
      MAX_FAILED_ATTEMPTS: ${MAX_FAILED_ATTEMPTS:-3}
      HEARTBEAT_INTERVAL: ${HEARTBEAT_INTERVAL:-3.0}
      LEADER_TIMEOUT: ${LEADER_TIMEOUT:-10.0}
    depends_on:
      - gateway
    networks:
      - coffee-network
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock  # Socket Docker para revivir contenedores
    restart: unless-stopped

  monitor_3:
    build:
      context: .
      dockerfile: Dockerfile.monitor
    container_name: coffee-monitor-3
    environment:
      PYTHONUNBUFFERED: ${PYTHONUNBUFFERED}
      MONITOR_ID: 3
      MONITOR_TCP_PORT: 10001
      HEALTHCHECK_PORT: ${HEALTHCHECK_PORT:-8888}
      CHECK_INTERVAL: ${CHECK_INTERVAL:-5.0}
      TIMEOUT: ${TIMEOUT:-2.0}
      MAX_FAILED_ATTEMPTS: ${MAX_FAILED_ATTEMPTS:-3}
      HEARTBEAT_INTERVAL: ${HEARTBEAT_INTERVAL:-3.0}
      LEADER_TIMEOUT: ${LEADER_TIMEOUT:-10.0}
    depends_on:
      - gateway
    networks:
      - coffee-network
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock  # Socket Docker para revivir contenedores
    restart: unless-stopped

  # ==================== CLIENT ====================
  # El cliente se ejecuta manualmente cuando sea necesario
  client:
    build:
      context: .
      dockerfile: Dockerfile.client
    environment:
      GATEWAY_HOST: gateway
      CLIENT_RESULTS_DIR: /app/results
    depends_on:
      - gateway
    networks:
      - coffee-network
    volumes:
      - ./.data:/app/.data:ro # Montar datos como solo lectura
      - ./results:/app/results
    profiles:
      - manual # No se inicia automáticamente
    # Ejemplo de uso:
    # docker-compose run --rm client --data-folder dataset1
    # docker-compose run --rm client --data-folder dataset2 --verbose

networks:
  coffee-network:
    driver: bridge

volumes:
  results:
EOF_FOOTER

echo ""
echo "✅ docker-compose.yml generado exitosamente"
echo ""
echo "Para usar:"
echo "  docker-compose up -d"
echo ""
echo "Para regenerar con diferentes cantidades:"
echo "  ./generate-docker-compose.sh --filter-year 5 --filter-hour 3"
echo ""

