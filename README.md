# Coffee Shop Analysis - Sistema Distribuido

Sistema distribuido para análisis de datos de coffee shops con soporte para múltiples clientes concurrentes y tolerante a fallos.

## 📋 Ejecutar el Sistema

Pasos para poder usar el sistema 

1. Levantar el sistema completo:
   ```
   docker compose up --build
   ```

2. Previo a poder usar el sistema colocar en la carpeta `.data` los archivos a enviar:
   ```
   .data/
   ├── users/
   ├── stores/
   ├── transactions/
   ├── transaction_items/
   └── menu_items/
   ```

3. Ejecutar cliente y poner en marcha el sistema:
   ```
   docker compose run --rm client
   ```

   En caso de hacer cambios al cliente usar: 
   ```
   docker compose build client
   ```

   Si se desea, usar multiples veces el comando para multiples clientes.


## 🔍 Monitoreo y Debugging

### Logs 
El sistema cuenta con logs para cada nodo donde se ve:
- ID unico para cada sessoin
- Progreso de procesamiento por sesión

Para verlos recomendamos hacer:

```
docker logs <id-conteiner-nodo>
```

### Ver estado de RabbitMQ
Acceder a http://localhost:15672 (guest/guest)

Esta configuracion es la default, cambiarla en el `.env` de ser necesario.  


## 🧪 Test del Middleware
Para los tests del middleware: 
```bash
make test-middleware 
```

## ✅ Verificar resultados
Para verificar que los resultados sean los deseados: 

1. Colocar en `results/results_expected` los resultados deseados para las 4 querys. 

2. Ejecutar
   ```bash
   python scripts/validate_results.py --session-id <id-session> --show-missing-rows
   ```



## 🐒 Chaos Monkey (Tolerancia a Fallas)

Usamos `scripts/chaos_monkey.py` para matar o reiniciar contenedores aleatoriamente y validar la resiliencia del sistema.

```bash
# Ataques básicos cada 20-60s sobre contenedores etiquetados role=node
python scripts/chaos_monkey.py

# Configuración personalizada
python scripts/chaos_monkey.py \
  --min-interval 10 \
  --max-interval 30 \
  --strategy restart \
  --exclude coffee-gateway coffee-monitor-1 coffee-monitor-2 coffee-monitor-3

# Ensayo reproducible con semilla y cantidad finita de acciones
python scripts/chaos_monkey.py --seed 42 --max-actions 5
```

Parámetros útiles:
- `--label`: filtra contenedores por label Docker (default `role=node`)
- `--include/--exclude`: restringen contenedores por nombre
- `--strategy`: `kill`, `stop` o `restart`
- `--min-interval` / `--max-interval`: intervalo aleatorio entre fallos
- `--duration` o `--max-actions`: límites de ejecución
- `--dry-run`: imprime las acciones sin ejecutarlas