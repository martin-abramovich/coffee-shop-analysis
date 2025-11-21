import sys
import os
import signal
import threading
import time
from collections import defaultdict

from workers.session_tracker import SessionTracker

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from workers.utils import deserialize_message, serialize_message
from common.healthcheck import start_healthcheck_server

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')

INPUT_EXCHANGE = "transactions_query2"    # exchange del group_by query 2
INPUT_ROUTING_KEY = "query2"              # routing key del group_by
OUTPUT_EXCHANGE = "results_query2"        # exchange de salida para resultados finales
ROUTING_KEY = "query2_results"            # routing para resultados

# Exchanges adicionales para JOIN
MENU_ITEMS_EXCHANGE = "menu_items_raw"
MENU_ITEMS_ROUTING_KEY = "menu_items"
MENU_ITEMS_QUEUE = "menu_items_raw"

class AggregatorQuery2:
    def __init__(self):
        # Datos por sesión: {session_id: session_data}
        self.session_data = {}
        
        # Los menu_items ahora se manejan por sesión (no globalmente)
    
    def initialize_session(self, session_id):
        """Inicializa datos para una nueva sesión"""
        if session_id not in self.session_data:
            self.session_data[session_id] = {
                'month_item_metrics': defaultdict(lambda: {
                    'total_quantity': 0,
                    'total_subtotal': 0.0
                }),
                'batches_received': 0,
                'eos_received': False,
                'results_sent': False,
                'eos_metrics_done': False,
                # Diccionario de JOIN específico de esta sesión
                'item_id_to_name': {},
                # Control de flujo específico de esta sesión
                'menu_items_loaded': False,
                'eos_menu_done': False
            }
    
    def get_session_data(self, session_id):
        """Obtiene los datos de una sesión específica"""
        self.initialize_session(session_id)
        return self.session_data[session_id]
        
    def load_menu_items(self, rows, session_id):
        """Carga menu_items para construir el diccionario item_id -> item_name para una sesión específica."""
        session_data = self.get_session_data(session_id)
        
        for row in rows:
            item_id = row.get('item_id')
            item_name = row.get('item_name')
            
            if item_id and item_name:
                session_data['item_id_to_name'][item_id] = item_name.strip()
        
        print(f"[AggregatorQuery2] Sesión {session_id}: Cargados {len(session_data['item_id_to_name'])} menu items para JOIN")
    
    def accumulate_metrics(self, rows, session_id):
        """Acumula métricas parciales de group_by_query2 para una sesión específica."""
        session_data = self.get_session_data(session_id)
        
        for row in rows:
            month = row.get('month')
            item_id = row.get('item_id')  # CAMBIO: usar item_id en lugar de item_name
            total_quantity = row.get('total_quantity', 0)
            total_subtotal = row.get('total_subtotal', 0.0)
            
            # Validar campos requeridos
            if not month or not item_id:
                continue
                
            # Convertir a tipos correctos
            try:
                if isinstance(total_quantity, str):
                    total_quantity = int(float(total_quantity))
                if isinstance(total_subtotal, str):
                    total_subtotal = float(total_subtotal)
            except (ValueError, TypeError):
                continue
            
            # Clave compuesta: (mes, item_id)
            key = (month, item_id if not isinstance(item_id, str) else item_id.strip())
            
            # Acumular métricas para esta sesión
            session_data['month_item_metrics'][key]['total_quantity'] += total_quantity
            session_data['month_item_metrics'][key]['total_subtotal'] += total_subtotal
        
        session_data['batches_received'] += 1
        # Log solo cada 100 batches para reducir verbosidad
        if session_data['batches_received'] % 10000 == 0:
            print(f"[AggregatorQuery2] Sesión {session_id}: Procesados {session_data['batches_received']} batches, combinaciones: {len(session_data['month_item_metrics'])}")
    
    def generate_final_results(self, session_id):
        """Genera los resultados finales para Query 2 con JOIN para una sesión específica."""
        session_data = self.get_session_data(session_id)
        
        print(f"[AggregatorQuery2] Generando resultados finales para sesión {session_id}...")
        print(f"[AggregatorQuery2] Total combinaciones procesadas: {len(session_data['month_item_metrics'])}")
        print(f"[AggregatorQuery2] Menu items disponibles para JOIN: {len(session_data['item_id_to_name'])}")
        
        if not session_data['month_item_metrics']:
            print(f"[AggregatorQuery2] No hay datos para procesar en sesión {session_id}")
            return []
        
        # Si no hay menu_items para esta sesión, haremos un fallback usando item_id como nombre
        if not session_data['item_id_to_name']:
            print(f"[AggregatorQuery2] WARNING: No hay menu_items cargados para sesión {session_id}. Se usará item_id como item_name (fallback)")
        
        # Preparar resultados por mes con JOIN
        results_by_month = defaultdict(list)
        
        # Agrupar por mes y hacer JOIN con menu_items
        for (month, item_id), metrics in session_data['month_item_metrics'].items():
            # JOIN: buscar item_name para el item_id de esta sesión (o fallback al propio id)
            item_name = session_data['item_id_to_name'].get(item_id) or str(item_id)
            
            results_by_month[month].append({
                'item_id': item_id,
                'item_name': item_name,
                'total_quantity': metrics['total_quantity'],
                'total_subtotal': metrics['total_subtotal']
            })
        
        final_results = []
        
        # Para cada mes, encontrar el producto más vendido y el de mayor ganancia
        for month, products in results_by_month.items():
            # Producto más vendido (mayor quantity)
            most_sold = max(products, key=lambda x: x['total_quantity'])
            
            # Producto con mayor ganancia (mayor subtotal)
            most_profitable = max(products, key=lambda x: x['total_subtotal'])
            
            final_results.append({
                'year_month_created_at': month,
                'item_name': most_sold['item_name'],
                'sellings_qty': most_sold['total_quantity'],
                'profit_sum': '',
                'metric_type': 'most_sold'
            })
            final_results.append({
                'year_month_created_at': month,
                'item_name': most_profitable['item_name'],
                'sellings_qty': '',
                'profit_sum': most_profitable['total_subtotal'],
                'metric_type': 'most_profitable'
            })
        
        # Ordenar por mes para consistencia
        final_results.sort(key=lambda x: x['year_month_created_at'])
        
        print(f"[AggregatorQuery2] Resultados generados para {len(results_by_month)} meses")
        print(f"[AggregatorQuery2] Total registros de resultados: {len(final_results)}")
        
        # Mostrar ejemplos
        # Solo mostrar ejemplos si hay pocos resultados
        if len(final_results) <= 10:
            print(f"[AggregatorQuery2] Ejemplos de resultados:")
            for i, result in enumerate(final_results[:3]):
                if 'sellings_qty' in result:
                    print(f"  {i+1}. {result['year_month_created_at']} - {result['item_name']}: {result['sellings_qty']} unidades")
            
        return final_results
    
    def generate_detailed_results(self):
        """Genera resultados detallados con TODOS los productos por mes (opcional)."""
        detailed_results = []
        
        for (month, item_id), metrics in self.month_item_metrics.items():
            item_name = self.item_id_to_name.get(item_id) or str(item_id)
            # Registro para cantidad vendida
            detailed_results.append({
                'year_month_created_at': month,
                'item_id': item_id,
                'item_name': item_name,
                'sellings_qty': metrics['total_quantity']
            })
            
            # Registro para ganancia
            detailed_results.append({
                'year_month_created_at': month,
                'item_id': item_id,
                'item_name': item_name,
                'profit_sum': metrics['total_subtotal']
            })
        
        # Ordenar por mes y producto
        detailed_results.sort(key=lambda x: (x['year_month_created_at'], x['item_name']))
        
        return detailed_results

# Instancia global del agregador
aggregator = AggregatorQuery2()
session_tracker = SessionTracker(["metricas", "menu_items"])

def on_metrics_message(body):
    """Maneja mensajes de métricas de group_by_query2."""
    global eos_count
    try:
        header, rows = deserialize_message(body)
    except Exception as e:
        print(f"[AggregatorQuery2] Error deserializando mensaje: {e}")
        return
    
    session_id = header.get("session_id", "unknown")
    batch_id = int(header.get("batch_id"))
    is_eos = header.get("is_eos") == "true"
    
    if is_eos:
        print(f"Se recibió EOS en metricas para sesión {session_id}, batch_id: {batch_id}. Marcando como listo...")
          
    if rows:
        aggregator.accumulate_metrics(rows, session_id)
    
    if session_tracker.update(session_id, "metricas", batch_id, is_eos):              
        generate_and_send_results(session_id)

def on_menu_items_message(body):
    """Maneja mensajes de menu_items para el JOIN."""
    print(f"[AggregatorQuery2] Mensaje recibido en menu_items: {len(body)} bytes")
    
    try:
        header, rows = deserialize_message(body)
        print(f"[AggregatorQuery2] Menu_items deserializado: {len(rows)} registros")
    except Exception as e:
        print(f"[AggregatorQuery2] Error deserializando mensaje de menu_items: {e}")
        return
    
    session_id = header.get("session_id", "unknown")
    batch_id = int(header.get("batch_id"))
    is_eos = header.get("is_eos") == "true"
    
    if is_eos:
        print(f"Se recibió EOS en metricas para sesión {session_id}, batch_id: {batch_id}. Marcando como listo...")
     
    if rows:
        aggregator.load_menu_items(rows, session_id)
    
    if session_tracker.update(session_id, "menu_items", batch_id, is_eos):
        generate_and_send_results(session_id)

def generate_and_send_results(session_id):
    """Genera y envía los resultados finales cuando ambos flujos terminaron para una sesión específica."""
    global shutdown_event, mq_out
        
    # Generar resultados finales (TOP productos por mes) para esta sesión
    final_results = aggregator.generate_final_results(session_id)
    print(f"[AggregatorQuery2] Resultados generados: {len(final_results)} registros")
    
    if final_results:
        # Enviar resultados finales con headers completos
        results_header = {
            "type": "result",
            "stream_id": "query2_results",
            "batch_id": "final",
            "is_batch_end": "true",
            "is_eos": "false",
            "query": "query2",
            "session_id": session_id,
            "total_results": str(len(final_results)),
            "description": "Productos_mas_vendidos_y_mayor_ganancia_por_mes_2024-2025",
            "is_final_result": "true"
        }
        
        # Enviar en batches el conjunto combinado (dos filas por mes)
        batch_size = 50
        total_batches = (len(final_results) + batch_size - 1) // batch_size
        for i in range(0, len(final_results), batch_size):
            batch = final_results[i:i + batch_size]
            batch_header = results_header.copy()
            batch_header["batch_number"] = str((i // batch_size) + 1)
            batch_header["total_batches"] = str(total_batches)
            result_msg = serialize_message(batch, batch_header)
            
            # Intentar enviar con reconexión automática si falla
            max_retries = 3
            mq_out.send(result_msg)
    else:
        print("[AggregatorQuery2] No hay resultados para enviar")
    
    if session_id in aggregator.session_data:
        del aggregator.session_data[session_id]
        
    print(f"[AggregatorQuery2] Resultados finales enviados para sesión {session_id}. Worker continúa activo.")

def consume_metrics():
    try:
        mq_metrics.start_consuming(on_metrics_message)
    except Exception as e:
        if not shutdown_event.is_set():
            print(f"[AggregatorQuery2] Error en consumo de métricas: {e}")

def consume_menu_items():
    try:
        mq_menu_items.start_consuming(on_menu_items_message)
    except Exception as e:
        if not shutdown_event.is_set():
            print(f"[AggregatorQuery2] Error en consumo de menu_items: {e}")

if __name__ == "__main__":
    shutdown_event = threading.Event()
    
    # Iniciar servidor de healthcheck UDP
    healthcheck_port = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
    start_healthcheck_server(port=healthcheck_port, node_name="aggregator_query2", shutdown_event=shutdown_event)
    print(f"[AggregatorQuery2] Healthcheck server iniciado en puerto UDP {healthcheck_port}")
    
    def signal_handler(signum, frame):
        print(f"[AggregatorQuery2] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada 1: métricas del group_by_query2
    mq_metrics = MessageMiddlewareQueue(RABBIT_HOST, "group_by_q2")
    
    # Entrada 2: menu_items para JOIN
    mq_menu_items = MessageMiddlewareQueue(RABBIT_HOST, MENU_ITEMS_QUEUE)
    
    # Salida: exchange para resultados finales
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] AggregatorQuery2 esperando mensajes...")
    print("[*] Query 2: Productos más vendidos y mayor ganancia por mes 2024-2025")
    print("[*] Consumiendo de 2 fuentes: métricas + menu_items para JOIN")
    print(f"[*] Escuchando métricas de: {INPUT_EXCHANGE} con routing key '{INPUT_ROUTING_KEY}'")
    print(f"[*] Escuchando menu_items de: {MENU_ITEMS_QUEUE}")
    print("[*] Esperará hasta recibir EOS de ambas fuentes para generar reporte")
    
    try:
        # Ejecutar ambos consumidores en paralelo como daemon threads
        metrics_thread = threading.Thread(target=consume_metrics, daemon=True)
        menu_items_thread = threading.Thread(target=consume_menu_items, daemon=True)
        
        metrics_thread.start()
        menu_items_thread.start()
        
        # Esperar indefinidamente - el worker NO termina después de EOS
        # Solo termina por señal externa (SIGTERM, SIGINT)
        print("[AggregatorQuery2] Worker iniciado, esperando mensajes de múltiples sesiones...")
        print("[AggregatorQuery2] El worker continuará procesando múltiples clientes")
        
        # Loop principal - solo termina por señal
        while not shutdown_event.is_set():
            metrics_thread.join(timeout=1)
            menu_items_thread.join(timeout=1)
            if not metrics_thread.is_alive() and not menu_items_thread.is_alive():
                break
        
        print("[AggregatorQuery2] Terminando por señal externa")
        
    except KeyboardInterrupt:
        print("\n[AggregatorQuery2] Interrupción recibida")
        shutdown_event.set()
    finally:
        for mq in [mq_metrics, mq_menu_items]:
            try:
                mq.stop_consuming()
            except Exception as e:
                print(f"Error al parar el consumo: {e}")
        
        
        # Cerrar conexiones
        for mq in [mq_metrics, mq_menu_items, mq_out]:
            try:
                mq.close()
            except Exception as e:
                print(f"Error al cerrar conexión: {e}")
      
        print("[x] AggregatorQuery2 detenido")
