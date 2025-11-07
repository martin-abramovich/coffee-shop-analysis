# Write Ahead Log (WAL) y Persistencia

## ¿Qué es Write Ahead Log y Cómo Funciona?

### El Problema de las Escrituras en Disco

Cuando un programa escribe datos a un archivo usando `write()`, **NO hay garantía de que los datos estén físicamente en disco**. El sistema operativo usa múltiples capas de buffers:

```
Aplicación → Buffer de usuario (Python) → Buffer del kernel (page cache) → Disco físico
```

**Problemas:**
1. **Datos en buffer del kernel**: El SO puede mantener datos en RAM (page cache) por segundos o minutos antes de escribir a disco
2. **Fallo del proceso**: Si el proceso cae, los datos en buffers se pierden
3. **Fallo del sistema**: Si hay un fallo de hardware/power, todo lo que está solo en RAM se pierde
4. **Escritura parcial**: Si el proceso cae a mitad de escritura, el archivo puede quedar corrupto

### ¿Cómo Garantizar Durabilidad?

Las bases de datos resuelven esto con **Write Ahead Log (WAL)**. El principio es simple pero poderoso:

> Antes de modificar el estado, escribe la operación en un log. Solo después de confirmar que el log está en disco, aplica la operación al estado.

### Flujo de WAL en Detalle

```
1. Operación llega: accumulate_transactions(session_id, rows)
   
2. ANTES de modificar estado en memoria:
   a) Serializar operación: "ACCUMULATE|abc123|{transaction_id:tx1,amount:75.5}"
   b) Escribir al WAL: append a operations.log
   c) CRÍTICO: fsync() - forzar escritura física a disco
   
3. DESPUÉS de confirmar que WAL está en disco:
   a) Aplicar operación al estado en memoria: session_data[session_id]['transactions'].append(...)
   
4. Si el proceso cae en cualquier momento:
   - Si cae ANTES del fsync: operación no está en disco, no se aplica
   - Si cae DESPUÉS del fsync: operación está en disco, se puede recuperar
   - Si cae DESPUÉS de aplicar: operación aplicada, todo OK
```

### ¿Qué es `fsync()`?

`fsync(fd)` es una syscall que **fuerza al sistema operativo a escribir todos los datos pendientes del archivo a disco físico**. Sin fsync:

```python
with open('wal.log', 'a') as f:
    f.write("ACCUMULATE|abc123|{...}\n")
    # Los datos pueden estar solo en buffer del kernel
    # Si el proceso cae ahora, se pierden
```

```python
with open('wal.log', 'a') as f:
    f.write("ACCUMULATE|abc123|{...}\n")
    f.flush()  # Vacía buffer de Python al kernel
    os.fsync(f.fileno())  # Fuerza kernel a escribir a disco físico
```

**Costo de fsync:**
- `fsync()` es **lento** (puede tomar 1-10ms dependiendo del disco)
- Por eso las bases de datos hacen fsync selectivo (no en cada operación, sino en batches o checkpoints)

### WAL en Bases de Datos Reales

**PostgreSQL:**
PostgreSQL escribe primero cada cambio en el WAL para asegurar durabilidad y poder recuperarse ante fallos.
Mantiene solo los WAL recientes en pg_wal/, borrando o reciclando los viejos cuando ya no se necesitan.
Si se activa el archivado, entonces sí guarda todo el histórico para replicación completa y point-in-time recovery.

**SQLite:**
SQLite usa WAL escribiendo todas las modificaciones en un archivo -wal mientras mantiene intacto el archivo principal para que los lectores sigan leyendo sin bloqueo.
Los cambios se escriben en el WAL y luego, en puntos de control, se pasan al archivo de base de datos, evitando crecimiento indefinido del WAL.
Para funcionar, todos los procesos deben estar en la misma máquina y puede haber un solo escritor, pero muchos lectores simultáneos (concurrencia)

**Patrón común:**
Ambos escriben primero todos los cambios en un archivo de log secuencial (WAL) y solo después los aplican al archivo de datos real mediante un proceso de “checkpoint”.
Esto permite recuperación tras fallos, lecturas consistentes mientras se escribe y mejor rendimiento al evitar escrituras aleatorias.
Los cambios no se escriben directamente en la base de datos, sino primero en un log secuencial (WAL).
Lectores siguen usando el archivo base sin bloqueo, mientras escritores agregan entradas al WAL.
Un checkpoint traslada las páginas del WAL al archivo principal y evita que el WAL crezca indefinidamente.

### Ventajas del WAL

1. **Durabilidad garantizada**: Si está en WAL y se hizo fsync, está en disco
2. **Recovery exacto**: Reaplicando operaciones del WAL, podes recuperar el estado exacto
3. **Escritura secuencial**: Append-only es muy eficiente (no hay seeks aleatorios)

### Desventajas y Cómo Mitigarlas

1. **WAL crece indefinidamente**: 
   - **Solución**: Snapshots periódicos + truncar WAL después de snapshot
   
2. **Recovery lento con WAL grande**:
   - **Solución**: Snapshot del estado + solo reaplicar WAL desde snapshot
   
3. **Overhead de fsync**:
   - **Solución**: Batch de operaciones, fsync cada N operaciones o tiempo fijo

---

## Atomic File Replace

### El Problema de la Escritura Parcial

Incluso con `fsync()`, si se escribe directamente al archivo de estado y el proceso se cae en la mitad de la escritura:

```python
with open('state.json', 'w') as f:
    json.dump(session_data, f)  # Si se cae acá, archivo corrupto
    f.flush()
    os.fsync(f.fileno())
```

Si el proceso se cae durante `json.dump()`, el archivo queda parcialmente escrito y corrupto.

### Solución: Atomic File Replace (Staging)

La estrategia es usar un **archivo temporal (staging)** y luego reemplazar el archivo final de forma atómica:

```python
def save_state_atomic(session_data):
    # 1. Escribir en archivo TEMPORAL
    with open('state.tmp', 'w') as f:
        json.dump(session_data, f)
        f.flush()
        os.fsync(f.fileno())  # Garantizar que está en disco
    
    # 2. Reemplazo ATÓMICO
    os.rename('state.tmp', 'state.json')
```

 `rename()` es una syscall que el kernel garantiza como atómica en UNIX!!

**Comportamiento:**
- Si `rename()` tiene éxito: el archivo nuevo reemplaza al viejo instantáneamente
- Si `rename()` falla: el archivo viejo queda intacto

### Flujo Completo: WAL + Atomic File Replace

Para snapshots del estado, combinamos ambas técnicas:

```python
def save_snapshot(session_data):
    # 1. Escribir snapshot completo en archivo temporal
    with open('snapshot.tmp', 'w') as f:
        json.dump(session_data, f)
        f.flush()
        os.fsync(f.fileno())
    
    # 2. Reemplazo
    os.rename('snapshot.tmp', 'snapshot.json')
    
    # 3. Truncar WAL (ya no necesario, está en snapshot)
    with open('wal.log', 'w') as f:
        f.write('')  # Limpiar WAL
        os.fsync(f.fileno())
```

---

## Propuesta para Nodos Stateful del TP

#### 1. AggregatorQuery1

**Estado actual:**
```python
self.session_data = {
    'abc123': {
        'transactions': [
            {'transaction_id': 'tx1', 'final_amount': 75.5},
            {'transaction_id': 'tx2', 'final_amount': 120.0},
            ...
        ],
        'total_received': 150,
        'results_sent': False
    },
    'def456': { ... }
}
```

**Operaciones que modifican estado:**
- `accumulate_transactions(rows, session_id)` - agrega transacciones a lista
- `generate_final_results(session_id)` - marca `results_sent = True`, luego elimina sesión

**Propuesta de persistencia:**

**WAL para operaciones incrementales:**
```
Formato WAL: timestamp|op|session_id|data_json\n

Ejemplos:
2024-01-15T10:30:45.123|ACCUMULATE|abc123|{"rows":[{"transaction_id":"tx1","final_amount":75.5}]}\n
2024-01-15T10:30:46.456|ACCUMULATE|abc123|{"rows":[{"transaction_id":"tx2","final_amount":120.0}]}\n
2024-01-15T10:35:00.789|COMPLETE|abc123|{}\n
```

**Snapshot periódico (cada X operaciones o X minutos):**
```json
{
  "snapshot_version": 1,
  "timestamp": "2024-01-15T10:35:00.789",
  "last_wal_offset": 1234,
  "session_data": {
    "abc123": {
      "transactions": [
        {"transaction_id": "tx1", "final_amount": 75.5},
        {"transaction_id": "tx2", "final_amount": 120.0}
      ],
      "total_received": 150,
      "results_sent": false
    }
  }
}
```

---

#### 2. AggregatorQuery2

**Operaciones:**
- `load_menu_items(rows, session_id)` - carga diccionario de JOIN
- `accumulate_metrics(rows, session_id)` - acumula métricas por (mes, item_id)

**Propuesta:**

Similar a Query1, pero con dos tipos de operaciones:

```
WAL entries:
2024-01-15T10:30:45|LOAD_MENU_ITEMS|abc123|{"items":{"item_123":"Cappuccino","item_456":"Latte"}}\n
2024-01-15T10:30:46|ACCUMULATE_METRICS|abc123|{"metrics":[{"month":"2024-01","item_id":"item_123","quantity":50,"subtotal":1250.0}]}\n
```

**Consideración:**
- `month_item_metrics` usa tuplas como keys, necesitamos serialización especial en JSON -> Hay que convertirlo a string

---

#### 3. SessionTracker

**Estado actual:**
```python
self.sessions = {
    'abc123': {
        'transactions': {
            'ranges': [(0, 10), (15, 20)],  # batch_ids recibidos
            'expected': 25,  # batch_id del EOS
            'done': False
        },
        'menu_items': {
            'ranges': [(0, 5)],
            'expected': 5,
            'done': True
        },
        '_lock': threading.Lock()
    }
}
```

**Operaciones:**
- `update(session_id, entity_type, batch_id, is_eos)` - actualiza tracking de batches

**WAL para cada actualización:**
```
2024-01-15T10:30:45|TRACK_BATCH|abc123|transactions|10|false\n
2024-01-15T10:30:46|TRACK_BATCH|abc123|transactions|11|false\n
2024-01-15T10:35:00|TRACK_EOS|abc123|transactions|25|true\n
```

**Snapshot:**
```json
{
  "sessions": {
    "abc123": {
      "transactions": {
        "ranges": [[0, 10], [15, 20]],
        "expected": 25,
        "done": false
      },
      "menu_items": {
        "ranges": [[0, 5]],
        "expected": 5,
        "done": true
      }
    }
  }
}
```

**Preguntaa:** SessionTracker es usado dentro de aggregators, puede persistirse junto con el estado del aggregator??

---

#### 4. AggregatorQuery3 y Query4

Similar a Query2, con sus estructuras específicas:
- Query3: `semester_store_tpv`, `store_id_to_name`
- Query4: `store_user_transactions`, `store_id_to_name`, `user_id_to_birthdate`

Misma estrategia: WAL + Snapshot periódico.

---

**Problema con uso de disco:** No podemos dejar que archivos crezcan indefinidamente.

**Solución:**

1. **WAL se trunca después de cada snapshot**: Solo mantenemos WAL desde último snapshot
2. **Snapshots rotados**: Mantener últimos 3 snapshots (por si el más reciente está corrupto)
3. **Limpieza automática**: Eliminar snapshots viejos

---