from collections import defaultdict
from datetime import datetime
from middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = "localhost"
INPUT_EXCHANGE = "transactions_year"     # exchange del filtro por año
INPUT_ROUTING_KEY = "year"               # routing key del filtro por año
OUTPUT_EXCHANGE = "transactions_month"   # exchange de salida para month grouping
ROUTING_KEY = "month"                    # routing para topic

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

def group_by_month(rows):
    """Agrupa las transacciones por mes (año-mes)."""
    month_groups = defaultdict(list)
    
    for r in rows:
        created_at = r.get("created_at")
        if not created_at:
            continue
        
        try:
            month_key = parse_month(created_at)
            month_groups[month_key].append(r)
        except Exception:
            # Ignorar filas con fechas inválidas
            continue
    
    return month_groups

def on_message(body):
    header, rows = deserialize_message(body)
    total_in = len(rows)
    
    # Agrupar por mes
    month_groups = group_by_month(rows)
    
    # Enviar cada grupo por separado
    groups_sent = 0
    for month_key, month_rows in month_groups.items():
        if month_rows:
            # Agregar información del grupo al header
            group_header = header.copy() if header else {}
            group_header["group_by"] = "month"
            group_header["group_key"] = month_key
            group_header["group_size"] = len(month_rows)
            
            out_msg = serialize_message(month_rows, group_header)
            mq_out.send(out_msg)
            groups_sent += 1
    
    print(f"[GroupByMonth] in={total_in} groups_created={len(month_groups)} groups_sent={groups_sent}")

if __name__ == "__main__":
    # Entrada: suscripción al exchange del filtro por año
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Salida: exchange para datos agrupados por mes
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] GroupByMonth worker esperando mensajes...")
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        mq_in.stop_consuming()
        mq_in.close()
        mq_out.close()
        print("[x] GroupByMonth worker detenido")
