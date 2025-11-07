# Documentación de Diseño: Tolerancia a Fallas
---

## 1. Persistencia de Estado

> **Documentación Detallada**: Ver [WAL y Persistencia: Propuesta Técnica](./wal-persistencia-propuesta.md) para explicación profunda de WAL, fsync(), atomic file replace, y propuesta concreta para cada nodo.

Los nodos **stateful** (aggregators, session_tracker, results_handler) mantienen estado en memoria que se pierde si el proceso cae. Necesitamos garantizar durabilidad del estado.

**Problema fundamental:**
- Las syscalls de escritura (`write()`) **NO garantizan** que los datos estén en disco
- El sistema operativo guarda en buffers y cuando considera los manda
- Necesitamos estrategias: **flush, sync, Write Ahead Log (WAL)**

### Nodos Stateful Identificados

1. **Aggregators** (query1, query2, query3, query4)
   - `session_data` con acumuladores por sesión
   - Crítico! pérdida de estado = pérdida de resultados parciales

2. **SessionTracker** (usado en aggregators)
   - Tracking de batch_ids por sesión y tipo de entidad
   - solo metadata de batches
   - Crítico! sin esto no se puede detectar completitud

3. **ResultsHandler** (en gateway)
   - Resultados acumulados antes de escribir CSV
   - se puede regenerar desde aggregators si persisten

### Problema Principal

Las llamadas `write()` no garantizan escritura inmediata en disco. El sistema operativo usa buffers, por lo que:
- Si el proceso cae antes de `flush()`, se pierden datos
- Si hay un fallo de hardware, los datos en buffer se pierden
- No hay garantía de atomicidad (escritura parcial)

### Opciones Evaluadas

#### Opción A: Write Ahead Log (WAL)

**Descripción:**
- Registrar cada operación en un log antes de aplicarla al estado
- Al recuperar, reaplicar todas las operaciones del log
- Similar a cómo funcionan bases de datos (SQLite, PostgreSQL, etc.)

**Implementación:**
```
Estado en memoria: session_data = {...}
WAL en disco: operaciones.log
  - append-only (solo escritura al final)
  - Formato: timestamp|operation|session_id|data_json
  - Ejemplo: "2024-01-15T10:30:45|ACCUMULATE|abc123|{'transaction_id':'tx1','amount':75.5}"
```

**Pros:**
- Durabilidad garantizada (fsync después de cada operación crítica)
- Historial completo de operaciones (útil para debugging)
- Permite recovery exacto reaplicando operaciones
- Escritura secuencial (muy eficiente en disco)
- Patrón probado en bases de datos

**Contras:**
- El log crece indefinidamente (necesita rotación/compactación)
- Recovery puede ser lento si el log es muy grande
- Más complejidad: necesitas parser de operaciones
- Overhead de escritura en cada operación (pero aceptable con fsync selectivo)

**Opciones:**
- **WAL con checkpoint periódico**: Guardar snapshot del estado + WAL desde checkpoint
- **WAL con rotación**: Rotar el log cada N operaciones o tamaño máximo

---

#### Opción B: Atomic File Replace (Staging)

**Descripción:**
- Escribir estado completo en archivo temporal (`state.tmp`)
- Hacer `fsync()` en el archivo temporal
- Reemplazar archivo final (`state.json`) de forma atómica (rename es atómico en la mayoría de sistemas de archivos)

**Implementación:**
```python
# Pseudocódigo
def save_state(session_data):
    # 1. Escribir en archivo temporal
    with open('state.tmp', 'w') as f:
        json.dump(session_data, f)
        f.flush()
        os.fsync(f.fileno())  # Forzar escritura a disco
    
    # 2. Reemplazo atómico
    os.rename('state.tmp', 'state.json')  # Atómico en Linux/Unix
```

**Pros:**
- Simplicidad: solo escribir JSON/estructura serializada
- Atomicidad (rename es atómico)
- Recovery rápido: solo leer un archivo
- No necesita parser de operaciones

**Contras:**
- Escribir estado completo cada vez (ineficiente si el estado es grande)
- Si el estado es muy grande, la escritura puede ser lenta
- No hay historial de operaciones (solo snapshot)
- En Windows, `rename` no es completamente atómico (pero casi)

**Opciones:**
- **Snapshot periódico**: Guardar snapshot cada N operaciones o tiempo
- **Diferencial**: Guardar solo cambios desde último snapshot

---

#### Opción C: Híbrido: WAL + Snapshot Periódico

**Descripción:**
- Combinar WAL (para durabilidad inmediata) con snapshots periódicos (para recovery rápido)
- WAL: operaciones recientes
- Snapshot: estado completo cada N operaciones o tiempo

**Implementación:**
```
Estado actual: session_data (en memoria)
WAL: operations.log (append-only, últimas X operaciones)
Snapshot: state_snapshot.json (cada X operaciones o X minutos)
```

**Pros:**
- Lo mejor de 2 mundos
- Durabilidad inmediata (WAL)
- Recovery rápido (snapshot + WAL desde snapshot)
- Control de crecimiento (rotar WAL después de snapshot)

**Contras:**
- Más complejidad (dos mecanismos)
- Necesita lógica de compactación (aplicar WAL a snapshot)

---

## 2. Healthchecks y Monitoreo

Cada proceso debe poder reportar su estado de salud para que los monitores detecten nodos caídos.

### Opciones

#### Opción A: TCP Healthcheck

**Descripción:**
- Cada nodo abre un socket TCP en un puerto específico
- Monitor hace `connect()` y espera respuesta
- Si no responde en timeout, nodo está caído

**Pros:**
- Confiable (TCP garantiza entrega)

**Contras:**
- Más overhead (handshake TCP)
- Requiere manejo de conexiones

---

#### Opción B: UDP Healthcheck

**Descripción:**
- Cada nodo escucha en puerto UDP
- Monitor envía paquete UDP, nodo responde
- Si no hay respuesta después de N intentos, nodo está caído

**Pros:**
- Muy liviano (sin handshake)
- Bajo overhead
- Adecuado para healthchecks simples

**Contras:**
- No garantiza entrega
- Requiere manejo de reintentos en monitor
- Paquetes pueden perderse

---

### Preguntas

- [ ] ¿Puerto fijo o dinámico por nodo?

---

## 3. Monitores y Elección de Líder

Necesitamos múltiples monitores redundantes que supervisen nodos. Solo uno debe ser "líder" y tomar acciones (reactivar nodos). Si el líder cae, otro debe asumir.

### Opción A: Raft Consensus Algorithm

**Descripción:**
- Algoritmo de consenso distribuido (usado por etcd, Consul)
- Garantiza un solo líder en todo momento

**Pros:**
- Garantías matemáticas de corrección
- Probado en producción a gran escala
- Maneja particiones de red

**Contras:**
- Complejidad alta (implementación completa)
- Overkill para nuestro caso (solo necesitamos elección de líder)
- Requiere mayoría de nodos vivos (quorum)

---

### Opción B: Leader Election Simple con ZooKeeper/etcd

**Descripción:**
- Usar servicio externo (ZooKeeper, etcd) para elección de líder
- Crear nodo efímero, el que lo crea es líder
- Si el líder cae, el nodo se elimina y otro puede crear uno nuevo

**Pros:**
- Implementación simple (usar librería)
- Robusto (ZooKeeper/etcd son estables)
- No necesitas implementar consenso

**Contras:**
- Dependencia externa adicional
- Más servicios que mantener
- Probablemente overkill

---

### Opción C: Leader Election Simple con Redis/Distributed Lock

**Descripción:**
- Usar Redis con SET NX EX (distributed lock con expiración)
- El que obtiene el lock es líder
- Lock expira automáticamente si líder cae (heartbeat)

**Pros:**
- Simple de implementar
- Redis ya podría estar disponible (o fácil de agregar)
- Lock con expiración automática

**Contras:**
- Dependencia de Redis
- Si Redis cae, no hay elección de líder

---

### Opción D: Leader Election Simple Propio (Basado en Timestamp/IP)

**Descripción:**
- Cada monitor tiene ID único (IP + timestamp de inicio)
- Monitores se comunican entre sí (UDP broadcast o lista conocida)
- El monitor con menor ID es líder
- Si líder no responde a heartbeat, siguiente en orden asume

**Implementación:**
```
Monitor 1 (IP: 172.20.0.10, Start: 10:00:00) → ID: "172.20.0.10-100000"
Monitor 2 (IP: 172.20.0.11, Start: 10:00:05) → ID: "172.20.0.11-100005"
Monitor 3 (IP: 172.20.0.12, Start: 10:00:03) → ID: "172.20.0.12-100003"

Líder: Monitor 1 (menor ID)
Si Monitor 1 no responde → Monitor 3 asume (siguiente menor)
```

**Pros:**
- Sin dependencias externas
- Simple de implementar
- Adecuado para nuestro caso (pocos monitores, red confiable Docker)

**Contras:**
- No maneja bien particiones de red complejas
- Posible split-brain si hay problemas de red (pero en Docker network es raro)
- Necesita lista conocida de monitores (configuración)

---

### Preguntas Pendientes

- [ ] ¿Cuántos monitores? (mínimo 2 para redundancia, recomendado 3)
- [ ] ¿Qué hacer en caso de split-brain (nodos se desconectan y creen que cada uno es el nodo principal)? (probablemente no ocurrirá en Docker)
- [ ] ¿El líder también monitorea su propia salud? (sí, otros monitores lo verifican)

---

## 4. Recuperación de Nodos

Cuando un monitor detecta que un nodo está caído, debe poder reactivarlo usando Docker.

### Opción A: Docker API Directa

**Descripción:**
- Monitor usa Docker API (HTTP) directamente
- Comandos: `docker start <container>`, `docker restart <container>`
- Requiere que monitor tenga acceso a Docker socket

**Pros:**
- Control directo y completo
- Puede inspeccionar estado del contenedor
- Flexible (puede hacer restart, start, stop, etc.)

**Contras:**
- Requiere acceso a Docker socket (permisos)
- Monitor debe estar en el mismo host o tener acceso remoto
- Más complejo (manejo de API HTTP)

---

### Opción B: Docker Compose API

**Descripción:**
- Monitor usa `docker-compose` CLI
- Comandos: `docker-compose up -d <service>`, `docker-compose restart <service>`
- Requiere acceso al directorio con `docker-compose.yml`

**Pros:**
- Familiar (ya usamos docker-compose)
- Maneja dependencias automáticamente
- Más simple que API directa

**Contras:**
- Requiere acceso al filesystem con docker-compose.yml
- Depende de CLI (no API programática directa)
- Monitor debe estar en mismo contexto

---

### Opción C: Docker SDK (Python)

**Descripción:**
- Usar librería `docker` de Python
- API programática, más limpia que HTTP directo
- `docker.from_env()` para conectar

**Pros:**
- API limpia y Pythonic
- Manejo de errores mejor
- Documentación buena

**Contras:**
- Dependencia adicional (librería)
- Igual requiere acceso a Docker socket

---

### Preguntas Pendientes

- [ ] ¿Qué hacer si el contenedor no existe? (crearlo desde docker-compose)
- [ ] ¿Backoff exponencial en reintentos? (sí, para evitar loops)
- [ ] ¿Límite de reintentos por nodo? (ej: máximo 5 intentos, luego ???)

---

## 5. Limpieza de Estado Huérfano

Si un cliente se desconecta inesperadamente, su estado queda en aggregators. Necesitamos limpiarlo.

### Opción A: Timeout de Sesión

**Descripción:**
- Cada sesión tiene timeout (ej: 30 minutos sin actividad)
- Si timeout expira, limpiar estado automáticamente
- Gateway detecta desconexión y marca sesión como "dead"

**Pros:**
- Simple de implementar
- Limpia automáticamente sin intervención
- Maneja casos edge (cliente cae sin enviar EOF)

**Contras:**
- Timeout debe ser configurado (¿cuánto es razonable?)
- Puede limpiar sesiones válidas si procesamiento es muy lento
- No es inmediato (espera timeout)

---

### Opción B: Mensaje de Purga Explícito

**Descripción:**
- Gateway detecta desconexión de cliente
- Envía mensaje de "purga" por pipeline a todos los nodos
- Cada nodo elimina estado de esa sesión al recibir purga

**Pros:**
- Limpieza inmediata
- Explícito y controlado
- No depende de timeouts

**Contras:**
- Si gateway cae, no se envía purga (pero según MVP es aceptable)
- Requiere que todos los nodos escuchen mensajes de purga
- Más complejo (nuevo tipo de mensaje)

---

### Opción C: Híbrido: Purga + Timeout de Seguridad

**Descripción:**
- Gateway envía purga cuando detecta desconexión
- Además, timeout de seguridad (ej: 1 hora) para casos donde purga no llegó

**Pros:**
- Lo mejor de ambos: inmediato + seguro
- Maneja edge cases (gateway cae antes de enviar purga)

**Contras:**
- Más complejo (dos mecanismos)

---

### Preguntas Pendientes

- [ ] ¿Formato exacto del mensaje de purga?
- [ ] ¿Broadcast a todos los nodos o solo stateful?
- [ ] ¿Qué hacer con resultados parciales?

---

## 6. Estrategia de Uso de Disco

No podemos persistir todo en disco ni dejar que archivos crezcan indefinidamente. Necesitamos estrategia de gestión.

### Opción A: WAL con Rotación Fija

**Descripción:**
- WAL tiene tamaño máximo (ej: 10MB)
- Cuando se alcanza, rotar: `operations.log.1`, `operations.log.2`, etc.
- Mantener últimos N archivos (ej: 5)

**Pros:**
- Control de espacio
- Historial limitado pero útil

**Contras:**
- Archivos viejos ocupan espacio
- Recovery puede necesitar múltiples archivos

---

### Opción B: WAL con Compactación Periódica

**Descripción:**
- WAL crece hasta snapshot
- Después de snapshot, truncar WAL a 0 (o mantener últimas 100 operaciones)
- Solo mantener WAL desde último snapshot

**Pros:**
- Espacio controlado (solo WAL reciente)
- Recovery simple (snapshot + WAL pequeño)

**Contras:**
- Perdes historial antiguo (pero no necesario para recovery)

---

### Opción C: Ventana Deslizante de Mensajes

**Descripción:**
- Además de WAL, mantener ventana de últimos N mensajes procesados
- Solo para debugging/monitoreo
- No necesario para recovery (eso va en WAL)

**Pros:**
- Útil para debugging
- Control de memoria

**Contras:**
- Más complejo
- Probablemente no necesario

---

### Preguntas

- [ ] ¿Tamaño máximo de snapshot? (¿comprimir si es muy grande?)
- [ ] ¿Qué hacer si snapshot está corrupto?
- [ ] ¿Limpieza automática de snapshots viejos?

---