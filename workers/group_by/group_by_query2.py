import sys
import os
import signal
from collections import defaultdict
from datetime import datetime

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
INPUT_EXCHANGE = "transaction_items_year"     # exchange del filtro por año (CORREGIDO: transaction_items)
INPUT_ROUTING_KEY = "year"                    # routing key del filtro por año
OUTPUT_EXCHANGE = "transactions_query2"       # exchange de salida para query 2
ROUTING_KEY = "query2"                        # routing para topic

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

def on_message(body):
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print("[GroupByQuery2] End of Stream recibido. Reenviando...")
        # Reenviar EOS a workers downstream
        eos_msg = serialize_message([], header)
        mq_out.send(eos_msg)
        print("[GroupByQuery2] EOS reenviado a workers downstream")
        return
    
    # Procesamiento normal
    total_in = len(rows)
    
    # Agrupar por (mes, item_id) y calcular métricas
    month_item_metrics = group_by_month_and_item(rows)
    
    # OPTIMIZACIÓN: Enviar de a BATCHES de 1000 registros para evitar mensajes individuales
    BATCH_SIZE = 1000
    batch_records = []
    batches_sent = 0
    total_quantity = 0
    total_subtotal = 0.0
    
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
            total_quantity += metrics['total_quantity']
            total_subtotal += metrics['total_subtotal']
            
            # Enviar cuando alcanzamos el tamaño del batch
            if len(batch_records) >= BATCH_SIZE:
                batch_header = header.copy() if header else {}
                batch_header["group_by"] = "month_item"
                batch_header["batch_size"] = str(len(batch_records))
                batch_header["metrics_type"] = "query2_aggregated"
                batch_header["batch_type"] = "grouped_metrics"
                
                out_msg = serialize_message(batch_records, batch_header)
                mq_out.send(out_msg)
                batches_sent += 1
                batch_records = []  # Limpiar para el siguiente batch
    
    # Enviar batch residual si queda algo
    if batch_records:
        batch_header = header.copy() if header else {}
        batch_header["group_by"] = "month_item"
        batch_header["batch_size"] = str(len(batch_records))
        batch_header["metrics_type"] = "query2_aggregated"
        batch_header["batch_type"] = "grouped_metrics"
        batch_header["is_last_chunk"] = "true"
        
        out_msg = serialize_message(batch_records, batch_header)
        mq_out.send(out_msg)
        batches_sent += 1
    
    unique_months = len(set(month for month, _ in month_item_metrics.keys()))
    unique_items = len(set(item_id for _, item_id in month_item_metrics.keys()))
    
    # Log más compacto
    if batches_sent > 0:
        print(f"[GroupByQuery2] in={total_in} created={len(month_item_metrics)} sent={batches_sent}_batches months={unique_months} items={unique_items} qty={total_quantity} subtotal={total_subtotal:.2f}")

if __name__ == "__main__":
    shutdown_requested = False
    
    def signal_handler(signum, frame):
        global shutdown_requested
        print(f"[GroupByQuery2] Señal {signum} recibida, cerrando...")
        shutdown_requested = True
        mq_in.stop_consuming()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: suscripción al exchange del filtro por año (transaction_items)
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Salida: exchange para datos agregados de query 2
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] GroupByQuery2 worker esperando mensajes...")
    print(f"[*] Consumiendo de: {INPUT_EXCHANGE} (transaction_items filtrados por año)")
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
