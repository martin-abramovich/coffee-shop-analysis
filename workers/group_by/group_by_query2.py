import sys
import os
import signal
import threading
from collections import defaultdict
from datetime import datetime
import time

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
NUM_FILTER_YEAR_WORKERS = int(os.environ.get('NUM_FILTER_YEAR_WORKERS', '3'))

# ID del worker (auto-detectado del hostname o env var)
def get_worker_id():
    worker_id_env = os.environ.get('WORKER_ID')
    if worker_id_env is not None:
        return int(worker_id_env)
    
    import socket, re
    hostname = socket.gethostname()
    match = re.search(r'[-_](\d+)$', hostname)
    if match:
        return int(match.group(1)) - 1
    return 0

WORKER_ID = get_worker_id()

INPUT_EXCHANGE = "transaction_items_year_query2"  # exchange dedicado para query2
INPUT_ROUTING_KEYS = [f"worker_{WORKER_ID}", "eos"]  # routing keys específicas
OUTPUT_EXCHANGE = "transactions_query2"           # exchange de salida para query 2
ROUTING_KEY = "query2"                            # routing para topic

def parse_month(created_at: str) -> str:
    """Extrae el año-mes de created_at. Retorna formato 'YYYY-MM'."""
    if not created_at:
        raise ValueError("created_at vacío")
    
    try:
        # Intentar extraer directamente los primeros 7 caracteres (YYYY-MM)
        return created_at[:7]
    except Exception:
        # Fallback: parsing completo
        try:
            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            return f"{dt.year:04d}-{dt.month:02d}"
        except Exception:
            dt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            return f"{dt.year:04d}-{dt.month:02d}"

def group_by_month_and_item(rows):
    """Agrupa por (mes, item_name) y calcula métricas para Query 2."""
    metrics = defaultdict(lambda: {
        'total_quantity': 0,
        'total_subtotal': 0.0
    })
    
    for r in rows:
        # Validar datos requeridos
        created_at = r.get("created_at")
        item_id = r.get("item_id")  # CAMBIO: usar item_id en lugar de item_name
        
        if not created_at or not item_id or not item_id.strip():
            continue
        
        try:
            month = parse_month(created_at)
            normalized_item_id = item_id.strip()
            
            # Extraer quantity y subtotal de la fila
            quantity = r.get("quantity", 0)
            subtotal = r.get("subtotal", 0.0)
            
            # Convertir a tipos numéricos si vienen como string
            if isinstance(quantity, str):
                quantity = int(float(quantity)) if quantity else 0
            if isinstance(subtotal, str):
                subtotal = float(subtotal) if subtotal else 0.0
            
            # Clave compuesta: (mes, item_id)
            key = (month, normalized_item_id)
            
            # Acumular métricas
            metrics[key]['total_quantity'] += quantity
            metrics[key]['total_subtotal'] += subtotal
            
        except Exception:
            # Ignorar filas con datos inválidos
            continue
    
    return metrics

# Control de EOS por sesión - necesitamos recibir EOS de todos los workers de filter_year por cada sesión
eos_count = {}  # {session_id: count}
eos_lock = threading.Lock()
batch_send = 0 

def on_message(body):
    global eos_count
    header, rows = deserialize_message(body)
    session_id = header.get("session_id", "unknown")
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        with eos_lock:
            # Inicializar contador para esta sesión si no existe
            if session_id not in eos_count:
                eos_count[session_id] = 0
            eos_count[session_id] += 1
            print(f"[GroupByQuery2] EOS recibido para sesión {session_id} ({eos_count[session_id]}/{NUM_FILTER_YEAR_WORKERS})")
            
            # Solo reenviar EOS cuando hayamos recibido de TODOS los workers de filter_year para esta sesión
            if eos_count[session_id] >= NUM_FILTER_YEAR_WORKERS:
                print(f"[GroupByQuery2] EOS recibido de TODOS los workers para sesión {session_id}. Reenviando downstream...")
                eos_msg = serialize_message([], header)  # Mantiene session_id en header
                mq_out.send(eos_msg)
                print(f"[GroupByQuery2] EOS reenviado para sesión {session_id}")
                
                # Limpiar contador EOS para esta sesión después de un delay
                def delayed_cleanup():
                    time.sleep(30)  # Esperar 30 segundos antes de limpiar
                    with eos_lock:
                        if session_id in eos_count:
                            del eos_count[session_id]
                            print(f"[GroupByQuery2] Sesión {session_id} limpiada de contadores EOS")
                
                cleanup_thread = threading.Thread(target=delayed_cleanup, daemon=True)
                cleanup_thread.start()
        return
    
    
    # Agrupar por (mes, item_id) y calcular métricas
    month_item_metrics = group_by_month_and_item(rows)
    
    batch_records = []
    
    for (month, item_id), metrics in month_item_metrics.items():
        if metrics['total_quantity'] > 0 or metrics['total_subtotal'] > 0:
            # Crear un registro único con las métricas de (mes, item_id)
            query2_record = {
                'month': month,
                'item_id': item_id,
                'total_quantity': metrics['total_quantity'],
                'total_subtotal': metrics['total_subtotal']
            }
            batch_records.append(query2_record)

    
    batch_header = header.copy() if header else {}
    batch_header["batch_size"] = str(len(batch_records))
    
    out_msg = serialize_message(batch_records, batch_header)
    mq_out.send(out_msg)
    
    unique_months = len(set(month for month, _ in month_item_metrics.keys()))
    unique_items = len(set(item_id for _, item_id in month_item_metrics.keys()))
    batches_sent += 1
    
    # Log compacto solo si hay datos significativos
    if batches_sent <= 3 or batch_send % 10000 == 0:
        print(f"[GroupByQuery2] in={len(batch_records)} created={len(month_item_metrics)} sent={batches_sent}_batches months={unique_months} items={unique_items}")

if __name__ == "__main__":
    print(f"[GroupByQuery2] Iniciando worker {WORKER_ID}...")
    shutdown_requested = False
    
    def signal_handler(signum, frame):
        global shutdown_requested
        print(f"[GroupByQuery2] Señal {signum} recibida, cerrando...")
        shutdown_requested = True
        mq_in.stop_consuming()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: suscripción al exchange del filtro por año con routing keys específicas
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, INPUT_ROUTING_KEYS)
    
    # Salida: exchange para datos agregados de query 2
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] GroupByQuery2 worker esperando mensajes...")
    print(f"[*] Consumiendo de: {INPUT_EXCHANGE} (transaction_items filtrados por año)")
    print(f"[*] Esperando EOS de {NUM_FILTER_YEAR_WORKERS} workers de filter_year")
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        print("\n[GroupByQuery2] Interrupción recibida")
    finally:
        try:
            mq_in.close()
        except:
            pass
        try:
            mq_out.close()
        except:
            pass
        print("[x] GroupByQuery2 worker detenido")
