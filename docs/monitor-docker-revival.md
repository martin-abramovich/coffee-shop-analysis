# Monitor con Revival de Contenedores Docker

## Descripción

El sistema de monitoreo implementa la capacidad de **revivir automáticamente contenedores caídos** usando Docker CLI.

### ¿Cómo funciona?

1. **Etiquetado de Contenedores**: Todos los nodos (gateway, workers, aggregators) están etiquetados con `role=node`
2. **Socket Docker Montado**: Los monitores tienen acceso al socket Docker del host (`/var/run/docker.sock`)
3. **Detección de Fallas**: El monitor líder detecta nodos caídos mediante healthchecks UDP
4. **Revival Automático**: Cuando un nodo cae, el líder ejecuta `docker start <container_id>` para revivirlo

## Arquitectura

```
┌─────────────────────────────────────────────┐
│              Docker Host                    │
│                                             │
│  ┌──────────────┐                          │
│  │ Monitor Líder│                          │
│  │  (Python)    │                          │
│  │              │                          │
│  │ /var/run/    │                          │
│  │ docker.sock  │                          │
│  └──────┬───────┘                          │
│         │                                  │
│         │ docker start <container>         │
│         ▼                                  │
│  ┌─────────────────┐                      │
│  │  Worker Caído   │ ──┐                  │
│  │  (label=node)   │   │ Revivirlo        │
│  └─────────────────┘   │                  │
│                        │                  │
└────────────────────────┼──────────────────┘
                         │
                         └─ Docker CLI
```

## Componentes Modificados

### 1. Dockerfile.monitor

Se instaló Docker CLI en el contenedor monitor:

```dockerfile
# Instalar Docker CLI
RUN apt-get update && \
    apt-get install -y docker-ce-cli && \
    apt-get clean
```

### 2. monitor.py

Se agregaron métodos para interactuar con Docker:

- `get_container_id_by_name()`: Obtiene ID de contenedor por nombre
- `get_container_ids_by_label()`: Lista contenedores con label `role=node`
- `is_container_running()`: Verifica si un contenedor está corriendo
- `start_container()`: Ejecuta `docker start` para levantar un contenedor
- `_attempt_container_revival()`: Lógica de revival cuando detecta falla
- `update_container_mapping()`: Mapea nodos a IDs de contenedores

### 3. docker-compose.yml

**Labels agregados** a todos los servicios:
- gateway
- filter_year_0, filter_year_1, filter_year_2
- filter_hour_0, filter_hour_1
- filter_amount_0, filter_amount_1
- group_by_query2_0, group_by_query2_1
- group_by_query3_0, group_by_query3_1
- group_by_query4_0, group_by_query4_1
- aggregator_query1, aggregator_query2, aggregator_query3, aggregator_query4

```yaml
labels:
  - "role=node"
```

**Socket Docker montado** en los 3 monitores:

```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

## Flujo de Revival

1. **Healthcheck Falla**: Monitor líder detecta que un nodo no responde UDP healthcheck
2. **Contador de Fallos**: Después de `MAX_FAILED_ATTEMPTS` (default: 3), marca el nodo como caído
3. **Obtener Container ID**: Busca el ID del contenedor por nombre
4. **Verificar Estado Docker**: Comprueba si el contenedor está realmente parado
5. **Ejecutar Revival**: Ejecuta `docker start <container_id>`
6. **Esperar Inicialización**: Espera 2 segundos para que el contenedor se inicie
7. **Verificar Recovery**: Comprueba si ahora responde al healthcheck
8. **Logging**: Registra todo el proceso con logs detallados

## Logs de Ejemplo

### Contenedor Detectado como Caído

```
✗ [filter_year_1] NODO CAÍDO - filter_year_1:8888 (fallos: 3)
```

### Intento de Revival

```
[REVIVAL] Intentando levantar contenedor filter_year_1 (a1b2c3d4e5f6)...
[REVIVAL] ✓ Contenedor filter_year_1 levantado exitosamente
```

### Revival Exitoso

```
[REVIVAL] ✓✓ filter_year_1 revivido y respondiendo!
✓ [filter_year_1] NODO RECUPERADO - filter_year_1:8888
```

### Revival Parcial

```
[REVIVAL] Contenedor filter_year_1 levantado pero aún no responde healthcheck
```

## Uso

### Levantar el Sistema

```bash
# Build de las imágenes (importante después de los cambios)
docker-compose build

# Levantar todos los servicios
docker-compose up -d
```

### Ver Logs del Monitor Líder

```bash
# Ver logs de todos los monitores
docker-compose logs -f monitor_1 monitor_2 monitor_3

# Identificar cuál es el líder (verás "[LEADER]" en los logs)
# Luego seguir solo ese monitor
docker-compose logs -f monitor_1
```

### Simular Falla de un Nodo

Para probar el sistema de revival:

```bash
# Detener un worker manualmente
docker stop coffee-filter-year-1

# Observar los logs del monitor líder
# Deberías ver:
# 1. Detección de falla
# 2. Intento de revival
# 3. Contenedor levantado
# 4. Nodo recuperado
```

### Verificar Labels

Para ver qué contenedores tienen el label `role=node`:

```bash
docker ps -a --filter "label=role=node" --format "table {{.Names}}\t{{.Status}}\t{{.Label \"role\"}}"
```

### Ver Estado de Contenedores

```bash
# Listar todos los contenedores del proyecto
docker-compose ps

# Ver solo los nodos monitoreados
docker ps --filter "label=role=node"
```

## Variables de Entorno

Configurables en `.env` o directamente en `docker-compose.yml`:

| Variable | Default | Descripción |
|----------|---------|-------------|
| `CHECK_INTERVAL` | 5.0 | Segundos entre healthchecks |
| `TIMEOUT` | 2.0 | Timeout para healthcheck UDP |
| `MAX_FAILED_ATTEMPTS` | 3 | Fallos antes de marcar como caído |
| `HEARTBEAT_INTERVAL` | 3.0 | Intervalo de heartbeat entre monitores |
| `LEADER_TIMEOUT` | 10.0 | Tiempo sin heartbeat para considerar líder caído |

## Consideraciones de Seguridad

⚠️ **IMPORTANTE**: Montar `/var/run/docker.sock` da **acceso total al daemon Docker** del host.

**Esto significa que el monitor puede:**
- Listar todos los contenedores
- Iniciar/detener contenedores
- Inspeccionar contenedores
- Ejecutar comandos en el host

**Mitigaciones implementadas:**
1. El monitor solo ejecuta `docker start` (no comandos destructivos)
2. Solo actúa sobre contenedores con label `role=node`
3. Validación de IDs antes de ejecutar comandos
4. Logging detallado de todas las operaciones

**Alternativas más seguras (no implementadas):**
- Usar Docker API con autenticación
- Usar un proxy restringido del socket Docker (como docker-socket-proxy)
- Implementar RBAC a nivel de contenedor

## Troubleshooting

### El monitor no puede acceder a Docker

**Síntoma:**
```
Error obteniendo contenedores con label role=node: ...
```

**Solución:**
1. Verificar que el socket esté montado:
   ```bash
   docker inspect coffee-monitor-1 | grep -A 5 Mounts
   ```
2. En Windows/Mac con Docker Desktop, asegurarse de que el socket esté habilitado en configuración

### Los contenedores no se reviven

**Posibles causas:**

1. **El contenedor no tiene el label**: Verificar con `docker inspect <container>`
2. **El monitor no es el líder**: Solo el líder ejecuta revivals
3. **El contenedor tiene un error fatal**: Verificar logs del contenedor
4. **Mapeo de nombres incorrecto**: El monitor busca por nombre parcial

**Debugging:**
```bash
# Ver qué nodos está monitoreando
docker logs coffee-monitor-1 | grep "Nodos a monitorear"

# Ver mapeo de contenedores
docker logs coffee-monitor-1 | grep "Mapeado"

# Ver intentos de revival
docker logs coffee-monitor-1 | grep "REVIVAL"
```

### Contenedor se levanta pero no responde healthcheck

**Síntoma:**
```
[REVIVAL] Contenedor X levantado pero aún no responde healthcheck
```

**Causas comunes:**
- El contenedor tarda más de 2 segundos en inicializar
- El contenedor tiene un error al arrancar
- Problema de red/DNS entre contenedores

**Solución:**
```bash
# Ver logs del contenedor revivido
docker logs <container_name>

# Verificar healthcheck manualmente
docker exec coffee-monitor-1 sh -c "echo -n 'PING' | nc -u -w 2 <container_name> 8888"
```

## Pruebas

### Test Básico de Revival

```bash
# 1. Verificar que todo esté up
docker-compose ps

# 2. Identificar el monitor líder
docker-compose logs monitor_1 monitor_2 monitor_3 | grep "LÍDER"

# 3. Detener un nodo
docker stop coffee-filter-year-1

# 4. Observar logs del líder (debería revivirlo en ~15-20 segundos)
docker-compose logs -f monitor_1

# 5. Verificar que el nodo está de vuelta
docker ps | grep filter-year-1
```

### Test de Failover de Líder

```bash
# 1. Identificar líder actual
docker-compose logs monitor_1 monitor_2 monitor_3 | grep "LÍDER"

# 2. Detener el líder
docker stop coffee-monitor-1  # o el que sea líder

# 3. Ver elección de nuevo líder (~10-15 segundos)
docker-compose logs -f monitor_2 monitor_3

# 4. Detener un nodo
docker stop coffee-filter-hour-0

# 5. Verificar que el nuevo líder lo revive
docker-compose logs -f monitor_2  # o el nuevo líder
```

## Limitaciones

1. **Solo revive contenedores detenidos**: Si el contenedor crashea internamente pero el proceso principal sigue corriendo, no se detecta a nivel Docker
2. **No recrea contenedores eliminados**: Solo hace `docker start`, no `docker run`
3. **Sin limits de reintentos**: El monitor intentará revivirlo cada vez que detecte que está caído
4. **Delay de detección**: Toma `CHECK_INTERVAL * MAX_FAILED_ATTEMPTS` segundos detectar falla (~15s por default)

## Mejoras Futuras

- [ ] Contador máximo de intentos de revival por contenedor
- [ ] Notificaciones (Slack, email) cuando se revive un contenedor
- [ ] Métricas (Prometheus) de revivals exitosos/fallidos
- [ ] Healthchecks más sofisticados (HTTP, TCP además de UDP)
- [ ] Recreación de contenedores eliminados (no solo detenidos)
- [ ] Dashboard web para ver estado de nodos y revivals

