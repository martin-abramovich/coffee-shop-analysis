from collections import defaultdict
from datetime import datetime
from middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = "localhost"
INPUT_EXCHANGE = "transactions_year"     # exchange del filtro por año
INPUT_ROUTING_KEY = "year"               # routing key del filtro por año
OUTPUT_EXCHANGE = "transactions_query2"  # exchange de salida para query 2
ROUTING_KEY = "query2"                   # routing para topic

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
        item_name = r.get("item_name")
        
        if not created_at or not item_name or not item_name.strip():
            continue
        
        try:
            month = parse_month(created_at)
            normalized_item_name = item_name.strip()
            
            # Extraer quantity y subtotal de la fila
            quantity = r.get("quantity", 0)
            subtotal = r.get("subtotal", 0.0)
            
            # Convertir a tipos numéricos si vienen como string
            if isinstance(quantity, str):
                quantity = int(float(quantity)) if quantity else 0
            if isinstance(subtotal, str):
                subtotal = float(subtotal) if subtotal else 0.0
            
            # Clave compuesta: (mes, item_name)
            key = (month, normalized_item_name)
            
            # Acumular métricas
            metrics[key]['total_quantity'] += quantity
            metrics[key]['total_subtotal'] += subtotal
            
        except Exception:
            # Ignorar filas con datos inválidos
            continue
    
    return metrics

def on_message(body):
    header, rows = deserialize_message(body)
    total_in = len(rows)
    
    # Agrupar por (mes, item_name) y calcular métricas
    month_item_metrics = group_by_month_and_item(rows)
    
    # Enviar cada combinación (mes, item) por separado
    groups_sent = 0
    total_quantity = 0
    total_subtotal = 0.0
    
    for (month, item_name), metrics in month_item_metrics.items():
        if metrics['total_quantity'] > 0 or metrics['total_subtotal'] > 0:
            # Crear un registro único con las métricas de (mes, item)
            query2_record = {
                'month': month,
                'item_name': item_name,
                'total_quantity': metrics['total_quantity'],
                'total_subtotal': metrics['total_subtotal']
            }
            
            # Agregar información del grupo al header
            group_header = header.copy() if header else {}
            group_header["group_by"] = "month_item"
            group_header["group_key"] = f"{month}|{item_name}"
            group_header["month"] = month
            group_header["item_name"] = item_name
            group_header["group_size"] = 1
            group_header["metrics_type"] = "query2_aggregated"
            
            # Enviar como lista con un solo elemento
            out_msg = serialize_message([query2_record], group_header)
            mq_out.send(out_msg)
            groups_sent += 1
            
            # Acumular totales para logging
            total_quantity += metrics['total_quantity']
            total_subtotal += metrics['total_subtotal']
    
    unique_months = len(set(month for month, _ in month_item_metrics.keys()))
    unique_items = len(set(item_name for _, item_name in month_item_metrics.keys()))
    
    print(f"[GroupByQuery2] in={total_in} combinations_created={len(month_item_metrics)} groups_sent={groups_sent}")
    print(f"[GroupByQuery2] unique_months={unique_months} unique_items={unique_items} total_qty={total_quantity} total_subtotal={total_subtotal:.2f}")

if __name__ == "__main__":
    # Entrada: suscripción al exchange del filtro por año
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Salida: exchange para datos agregados de query 2
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] GroupByQuery2 worker esperando mensajes...")
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        mq_in.stop_consuming()
        mq_in.close()
        mq_out.close()
        print("[x] GroupByQuery2 worker detenido")
