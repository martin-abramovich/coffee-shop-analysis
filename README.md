# Coffee Shop Analysis - Sistema Distribuido

Sistema distribuido para análisis de datos de coffee shops con soporte para múltiples clientes concurrentes y protocolo EOF robusto.

## 🆕 Nuevas Características Implementadas

### ✅ Soporte para Múltiples Clientes Concurrentes
- **Múltiples ejecuciones** sin reinicio del servidor
- **Múltiples clientes** ejecutándose concurrentemente
- **Protocolo EOF robusto** para coordinación N clientes -> M workers
- **Limpieza correcta** de recursos después de cada ejecución
- **Logs de depuración** detallados para visibilidad del protocolo

### ✅ Especificación Dinámica de Carpetas de Datos
- **Carpetas dinámicas**: Cada cliente puede especificar su subcarpeta de datos
- **Múltiples datasets**: Procesar diferentes datasets sin reiniciar el sistema
- **Compatibilidad total**: Funciona con el comportamiento original

### 🔧 Mejoras Técnicas
- Gestión de sesiones con IDs únicos (UUID)
- Protocolo de finalización EOF que coordina N nodos a M nodos
- Gestor de recursos automático con limpieza periódica
- Manejo robusto de errores con reconexión automática
- Threading para manejo concurrente de clientes
- Graceful shutdown con manejo de señales

## 🧪 Tests y Demostraciones

### Test del Protocolo EOF (Independiente)
```bash
# Ejecutar test aislado del protocolo EOF
python run_eof_test.py
```

### Test de Múltiples Sesiones (Requiere Gateway)
```bash
# Ejecutar test de múltiples clientes (requiere gateway activo)
python client/multi_session_client.py
```

### Demostración Completa
```bash
# Ejecutar demostración completa de todas las mejoras
python demo_multiple_clients.py
```

## 📋 Para Ejecutar el Sistema

### Opción 1: Sistema Completo
1. Poner en una carpeta `.data` las carpetas de los archivos:
   ```
   .data/
   ├── users/
   ├── stores/
   ├── transactions/
   ├── transaction_items/
   └── menu_items/
   ```

2. Levantar el sistema:
   ```bash
   docker-compose up --build
   ```

3. Esperar que todos los servicios estén listos (15-30 segundos)

4. Ejecutar cliente:
   ```bash
   docker-compose run --rm client
   ```

### Opción 2: Múltiples Clientes Concurrentes
1. Levantar solo el sistema (sin cliente):
   ```bash
   docker-compose up --build gateway rabbitmq filter_year_0 filter_year_1 filter_year_2 # ... otros workers
   ```

2. Ejecutar múltiples clientes desde el host:
   ```bash
   # Terminal 1
   python client/multi_session_client.py
   
   # Terminal 2 (simultáneamente)
   python client/multi_session_client.py
   
   # Terminal 3 (simultáneamente)
   python client/multi_session_client.py
   ```

### Opción 3: Múltiples Carpetas de Datos
1. Preparar estructura de datos:
   ```
   .data/
   ├── dataset1/
   │   ├── transactions.csv
   │   ├── users.csv
   │   └── ...
   ├── dataset2/
   │   ├── transactions.csv
   │   ├── users.csv
   │   └── ...
   └── dataset3/
       ├── transactions.csv
       ├── users.csv
       └── ...
   ```

2. Levantar el sistema:
   ```bash
   docker-compose up --build gateway rabbitmq # ... workers
   ```

3. Ejecutar clientes con diferentes datasets:
   ```bash
   # Procesar dataset1
   docker-compose run --rm client --data-folder dataset1 --verbose
   
   # Procesar dataset2 (simultáneamente)
   docker-compose run --rm client --data-folder dataset2 --verbose
   
   # Procesar dataset3 (simultáneamente)
   docker-compose run --rm client --data-folder dataset3 --verbose
   ```

4. O usar el script de ejemplo:
   ```bash
   python examples/multiple_datasets.py
   ```

## 🔍 Monitoreo y Debugging

### Logs del Gateway
El gateway ahora proporciona logs detallados:
- ID de sesión único para cada cliente
- Estado del protocolo EOF
- Estadísticas de recursos activos
- Progreso de procesamiento por sesión

### Verificar Estado del Sistema
```bash
# Ver logs del gateway
docker-compose logs -f gateway

# Ver logs de workers específicos
docker-compose logs -f filter_year_0
docker-compose logs -f aggregator_query1

# Ver estado de RabbitMQ
# Acceder a http://localhost:15672 (guest/guest)
```

## 📁 Estructura de Archivos Nuevos

```
├── gateway/
│   ├── eof_protocol.py          # Protocolo EOF robusto (NUEVO)
│   ├── resource_manager.py      # Gestión de recursos (NUEVO)
│   ├── server.py               # Modificado para múltiples clientes
│   └── main.py                 # Modificado con threading
├── client/
│   ├── multi_session_client.py  # Cliente de múltiples sesiones (NUEVO)
│   ├── main.py                 # Modificado para aceptar --data-folder
│   └── common/client.py         # Sin cambios (funcionalidad solo en main.py)
├── tests/
│   └── test_eof_protocol.py     # Test aislado del protocolo (NUEVO)
├── run_eof_test.py             # Script para tests EOF (NUEVO)
└── demo_multiple_clients.py    # Demostración completa (NUEVO)
```

## 🧪 Test del Middleware
Para los tests del middleware: 
```bash
make test-middleware 
```

## 🔬 Protocolo de Finalización EOF

El sistema implementa un protocolo robusto de finalización que:

1. **Rastrea sesiones activas** por cada cliente conectado
2. **Coordina EOF** entre N clientes y M workers downstream
3. **Garantiza que workers** solo reciban EOF cuando TODOS los clientes terminaron
4. **Proporciona logs detallados** para debugging y monitoreo
5. **Maneja desconexiones** inesperadas de clientes

### Ejemplo de Logs del Protocolo EOF
```
[EOF_PROTOCOL] Sesión abc123 registrada con tipos: {'transactions', 'users'}
[EOF_PROTOCOL] EOF recibido: sesión abc123, tipo transactions
[EOF_PROTOCOL] Aún esperando más sesiones para enviar EOF de transactions
[EOF_PROTOCOL] Sesión abc123 completada
[EOF_PROTOCOL] Enviando EOF de transactions a workers: ['filter_year']
```

## 🛠️ Gestión de Recursos

El sistema incluye un gestor de recursos que:

- **Rastrea recursos activos** (conexiones, threads, colas)
- **Limpia automáticamente** recursos al finalizar sesiones
- **Detecta recursos huérfanos** y los limpia periódicamente
- **Proporciona métricas** de uso de recursos
- **Maneja shutdown graceful** con limpieza completa

## 🚨 Troubleshooting

### El gateway no acepta múltiples clientes
- Verificar que se esté usando la versión actualizada del código
- Revisar logs del gateway para errores de threading

### Workers no reciben EOF correctamente
- Verificar logs del protocolo EOF en el gateway
- Asegurar que todos los clientes envíen EOF para todos los tipos de entidad

### Recursos no se limpian correctamente
- Revisar logs del resource manager
- Verificar que las sesiones se desregistren correctamente

### Tests fallan
- Asegurar que no hay otros procesos usando el puerto 9000
- Verificar que RabbitMQ esté disponible para tests que lo requieren
