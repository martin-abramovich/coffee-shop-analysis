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
NUM_FILTER_HOUR_WORKERS = int(os.environ.get('NUM_FILTER_HOUR_WORKERS', '2'))

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

INPUT_EXCHANGE = "transactions_hour"     # exchange del filtro por hora
INPUT_ROUTING_KEYS = [f"worker_{WORKER_ID}", "eos"]  # routing keys específicas
OUTPUT_EXCHANGE = "transactions_query3"  # exchange de salida para query 3
ROUTING_KEY = "query3"                   # routing para topic

def parse_semester(created_at: str) -> str:
    """Extrae el año-semestre de created_at. Retorna formato 'YYYY-S1' o 'YYYY-S2'."""
    if not created_at:
        raise ValueError("created_at vacío")
    
    try:
        # Intentar extraer directamente el mes de los primeros caracteres (YYYY-MM)
        month_str = created_at[5:7]  # Extraer MM de YYYY-MM-DD
        month = int(month_str)
        year_str = created_at[:4]    # Extraer YYYY
        
        # Determinar semestre basado en el mes
        semester = "S1" if month <= 6 else "S2"
        return f"{year_str}-{semester}"
        
    except Exception:
        # Fallback: parsing completo
        try:
            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            semester = "S1" if dt.month <= 6 else "S2"
            return f"{dt.year}-{semester}"
        except Exception:
            dt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
            semester = "S1" if dt.month <= 6 else "S2"
            return f"{dt.year}-{semester}"

def group_by_semester_and_store(rows):
    """Agrupa por (semestre, store_id) y calcula TPV para Query 3."""
    metrics = defaultdict(lambda: {
        'total_payment_value': 0.0
    })
    
    for r in rows:
        # Validar datos requeridos
        created_at = r.get("created_at")
        store_id = r.get("store_id")
        
        if not created_at or not store_id or not store_id.strip():
            continue
        
        try:
            semester = parse_semester(created_at)
            normalized_store_id = store_id.strip()
            
            # Extraer final_amount de la fila
            final_amount = r.get("final_amount", 0.0)
            
            # Convertir a tipo numérico si viene como string
            if isinstance(final_amount, str):
                final_amount = float(final_amount) if final_amount else 0.0
            
            # Clave compuesta: (semestre, store_id)
            key = (semester, normalized_store_id)
            
            # Acumular TPV (Total Payment Value)
            metrics[key]['total_payment_value'] += final_amount
            
        except Exception:
            # Ignorar filas con datos inválidos
            continue
    
    return metrics

# Control de EOS por sesión - necesitamos recibir EOS de todos los workers de filter_hour por cada sesión
eos_count = {}  # {session_id: count}
eos_lock = threading.Lock()
batches_sent = 0 

def on_message(body):
    global eos_count
    global batches_sent
    header, rows = deserialize_message(body)
    session_id = header.get("session_id", "unknown")
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        with eos_lock:
            # Inicializar contador para esta sesión si no existe
            if session_id not in eos_count:
                eos_count[session_id] = 0
            eos_count[session_id] += 1
            print(f"[GroupByQuery3] EOS recibido para sesión {session_id} ({eos_count[session_id]}/{NUM_FILTER_HOUR_WORKERS})")
            
            # Solo reenviar EOS cuando hayamos recibido de TODOS los workers de filter_hour para esta sesión
            if eos_count[session_id] >= NUM_FILTER_HOUR_WORKERS:
                print(f"[GroupByQuery3] EOS recibido de TODOS los workers para sesión {session_id}. Reenviando downstream...")
                eos_msg = serialize_message([], header)  # Mantiene session_id en header
                mq_out.send(eos_msg)
                print(f"[GroupByQuery3] EOS reenviado para sesión {session_id}")
                
                # Limpiar contador EOS para esta sesión después de un delay
                def delayed_cleanup():
                    time.sleep(30)  # Esperar 30 segundos antes de limpiar
                    with eos_lock:
                        if session_id in eos_count:
                            del eos_count[session_id]
                            print(f"[GroupByQuery3] Sesión {session_id} limpiada de contadores EOS")
                
                cleanup_thread = threading.Thread(target=delayed_cleanup, daemon=True)
                cleanup_thread.start()
        return
    
    total_in = len(rows)
    
    # Agrupar por (semestre, store_id) y calcular TPV
    semester_store_metrics = group_by_semester_and_store(rows)
    
    batch_records = []
    
    for (semester, store_id), metrics in semester_store_metrics.items():
        if metrics['total_payment_value'] > 0:
            # Crear un registro único con las métricas de (semestre, store)
            query3_record = {
                'semester': semester,
                'store_id': store_id,
                'total_payment_value': metrics['total_payment_value']
            }
            batch_records.append(query3_record)
            
        

    batch_header = header.copy() if header else {}
    batch_header["batch_size"] = str(len(batch_records))
    
    out_msg = serialize_message(batch_records, batch_header)
    mq_out.send(out_msg)
    batches_sent += 1

    
    if batches_sent <= 3 or batches_sent % 10000 == 0:
        unique_semesters = len(set(semester for semester, _ in semester_store_metrics.keys()))
        unique_stores = len(set(store_id for _, store_id in semester_store_metrics.keys()))
        
        print(f"[GroupByQuery3] batches_sent={batches_sent} in={total_in} created={len(semester_store_metrics)} semesters={unique_semesters} stores={unique_stores}")

if __name__ == "__main__":
    import threading
    print(f"[GroupByQuery3] Iniciando worker {WORKER_ID}...")
    shutdown_requested = False
    
    def signal_handler(signum, frame):
        global shutdown_requested
        print(f"[GroupByQuery3] Señal {signum} recibida, cerrando...")
        mq_in.stop_consuming()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: suscripción al exchange del filtro por hora con routing keys específicas
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, INPUT_ROUTING_KEYS)
    
    # Salida: exchange para datos agregados de query 3
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] GroupByQuery3 worker esperando mensajes...")
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        print("\n[GroupByQuery3] Interrupción recibida")
    finally:
        try:
            mq_in.close()
        except:
            pass
        try:
            mq_out.close()
        except:
            pass
        print("[x] GroupByQuery3 worker detenido")
