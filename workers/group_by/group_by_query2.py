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


batches_sent = 0
 
def on_message(body):
    global batches_sent
    header, rows = deserialize_message(body)
        
     
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
    batches_sent += 1
   
    # Log compacto solo si hay datos significativos
    if batches_sent <= 3 or batches_sent % 10000 == 0:
        total_in = len(rows)
        unique_months = len(set(month for month, _ in month_item_metrics.keys()))
        unique_items = len(set(item_id for _, item_id in month_item_metrics.keys()))
        
        print(f"[GroupByQuery2] batches_sent={batches_sent} in={total_in} created={len(month_item_metrics)} months={unique_months} items={unique_items}")

if __name__ == "__main__":
    print(f"[GroupByQuery2] Iniciando worker {WORKER_ID}...")
    shutdown_event = threading.Event()
    
    # Iniciar servidor de healthcheck UDP
    healthcheck_port = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
    start_healthcheck_server(port=healthcheck_port, node_name=f"group_by_query2_{WORKER_ID}", shutdown_event=shutdown_event)
    print(f"[GroupByQuery2 Worker {WORKER_ID}] Healthcheck server iniciado en puerto UDP {healthcheck_port}")
    
    def signal_handler(signum, frame):
        print(f"[GroupByQuery2] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
        try:
            filter_trans_item_queue = MessageMiddlewareQueue(RABBIT_HOST, "transactions_year").stop_consuming()
        except Exception as e: 
            print(f"Error al parar el consumo: {e}")
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: suscripción al exchange del filtro por año con routing keys específicas
    year_trans_item_queue = MessageMiddlewareQueue(RABBIT_HOST, "transaction_items_year")
    
    # Salida: exchange para datos agregados de query 2
    mq_out = MessageMiddlewareQueue(RABBIT_HOST, "group_by_q2")
    
    print("[*] GroupByQuery2 worker esperando mensajes...")
    print(f"[*] Consumiendo de: {INPUT_EXCHANGE} (transaction_items filtrados por año)")
    try:
        year_trans_item_queue.start_consuming(on_message)
    except KeyboardInterrupt:
        print("\n[GroupByQuery2] Interrupción recibida")
    finally:
        try:
            filter_trans_item_queue = MessageMiddlewareQueue(RABBIT_HOST, "transactions_year").close()
        except:
            pass
        try:
            mq_out.close()
        except:
            pass
        print("[x] GroupByQuery2 worker detenido")
