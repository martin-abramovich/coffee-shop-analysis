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
INPUT_EXCHANGE = "transactions_hour"     # exchange del filtro por hora
INPUT_ROUTING_KEY = "hour"               # routing key del filtro por hora
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

def on_message(body):
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print("[GroupByQuery3] End of Stream recibido. Reenviando...")
        # Reenviar EOS a workers downstream
        eos_msg = serialize_message([], header)
        mq_out.send(eos_msg)
        print("[GroupByQuery3] EOS reenviado a workers downstream")
        return
    
    # Procesamiento normal
    total_in = len(rows)
    
    # Agrupar por (semestre, store_id) y calcular TPV
    semester_store_metrics = group_by_semester_and_store(rows)
    
    # OPTIMIZACIÓN: Enviar de a BATCHES de 1000 registros para evitar mensajes individuales
    BATCH_SIZE = 1000
    batch_records = []
    batches_sent = 0
    total_tpv = 0.0
    
    for (semester, store_id), metrics in semester_store_metrics.items():
        if metrics['total_payment_value'] > 0:
            # Crear un registro único con las métricas de (semestre, store)
            query3_record = {
                'semester': semester,
                'store_id': store_id,
                'total_payment_value': metrics['total_payment_value']
            }
            batch_records.append(query3_record)
            total_tpv += metrics['total_payment_value']
            
            # Enviar cuando alcanzamos el tamaño del batch
            if len(batch_records) >= BATCH_SIZE:
                batch_header = header.copy() if header else {}
                batch_header["group_by"] = "semester_store"
                batch_header["batch_size"] = str(len(batch_records))
                batch_header["metrics_type"] = "query3_aggregated"
                batch_header["batch_type"] = "grouped_metrics"
                
                out_msg = serialize_message(batch_records, batch_header)
                mq_out.send(out_msg)
                batches_sent += 1
                batch_records = []  # Limpiar para el siguiente batch
    
    # Enviar batch residual si queda algo
    if batch_records:
        batch_header = header.copy() if header else {}
        batch_header["group_by"] = "semester_store"
        batch_header["batch_size"] = str(len(batch_records))
        batch_header["metrics_type"] = "query3_aggregated"
        batch_header["batch_type"] = "grouped_metrics"
        batch_header["is_last_chunk"] = "true"
        
        out_msg = serialize_message(batch_records, batch_header)
        mq_out.send(out_msg)
        batches_sent += 1
    
    unique_semesters = len(set(semester for semester, _ in semester_store_metrics.keys()))
    unique_stores = len(set(store_id for _, store_id in semester_store_metrics.keys()))
    
    # Log más compacto
    if batches_sent > 0:
        print(f"[GroupByQuery3] in={total_in} created={len(semester_store_metrics)} sent={batches_sent}_batches semesters={unique_semesters} stores={unique_stores} tpv={total_tpv:.2f}")

if __name__ == "__main__":
    shutdown_requested = False
    
    def signal_handler(signum, frame):
        global shutdown_requested
        print(f"[GroupByQuery3] Señal {signum} recibida, cerrando...")
        shutdown_requested = True
        mq_in.stop_consuming()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: suscripción al exchange del filtro por hora
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
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
