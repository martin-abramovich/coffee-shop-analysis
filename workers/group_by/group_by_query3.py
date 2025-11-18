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
from common.healthcheck import start_healthcheck_server

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
WORKER_ID = os.environ.get('WORKER_ID')

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

batches_sent = 0 

def on_message(body):
    global batches_sent
    header, rows = deserialize_message(body)
    
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
            
            
    out_msg = serialize_message(batch_records, header)
    group_by_queue.send(out_msg)
    batches_sent += 1

    
    if batches_sent <= 3 or batches_sent % 10000 == 0:
        total_in = len(rows)
        unique_semesters = len(set(semester for semester, _ in semester_store_metrics.keys()))
        unique_stores = len(set(store_id for _, store_id in semester_store_metrics.keys()))
        
        print(f"[GroupByQuery3] batches_sent={batches_sent} in={total_in} created={len(semester_store_metrics)} semesters={unique_semesters} stores={unique_stores}")

if __name__ == "__main__":
    print(f"[GroupByQuery3] Iniciando worker {WORKER_ID}...")
    shutdown_event = threading.Event()
    
    # Iniciar servidor de healthcheck UDP
    healthcheck_port = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
    start_healthcheck_server(port=healthcheck_port, node_name=f"group_by_query3_{WORKER_ID}", shutdown_event=shutdown_event)
    print(f"[GroupByQuery3 Worker {WORKER_ID}] Healthcheck server iniciado en puerto UDP {healthcheck_port}")
    
    def signal_handler(signum, frame):
        print(f"[GroupByQuery3] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
        try:
            hour_trans_queue.stop_consuming()
        except Exception as e: 
            print(f"Error al parar el consumo: {e}")
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: suscripción al exchange del filtro por hora con routing keys específicas
    hour_trans_exchange = MessageMiddlewareExchange(RABBIT_HOST, "transactions_hour", ["transactions_hour"])
    hour_trans_queue = MessageMiddlewareQueue(RABBIT_HOST, "transactions_hour_q3")
    hour_trans_queue.bind("transactions_hour", "transactions_hour")
    
    # Salida: exchange para datos agregados de query 3
    group_by_queue = MessageMiddlewareQueue(RABBIT_HOST, "group_by_q3")
    
    print("[*] GroupByQuery3 worker esperando mensajes...")
    try:
        hour_trans_queue.start_consuming(on_message)
    except KeyboardInterrupt:
        print("\n[GroupByQuery3] Interrupción recibida")
    finally:
        # Cerrar conexiones
        for mq in [group_by_queue, hour_trans_exchange, hour_trans_queue]:
            try:
                mq.close()
            except Exception as e:
                print(f"Error al cerrar conexión: {e}")
                
        print("[x] GroupByQuery3 worker detenido")
