import sys
import os
import signal
import threading
import time
from collections import defaultdict

from workers.session_tracker import SessionTracker

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from workers.utils import deserialize_message, serialize_message
from common.healthcheck import start_healthcheck_server

RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')

INPUT_QUEUE = "group_by_q3"    

OUTPUT_EXCHANGE = "results_query3"        
ROUTING_KEY = "query3_results"            

STORES_EXCHANGE = "stores_raw"
STORES_ROUTING_KEY = "q3"


class AggregatorQuery3:
    def __init__(self):
        # Datos por sesión: {session_id: session_data}
        self.session_data = {}
    
    def initialize_session(self, session_id):
        """Inicializa datos para una nueva sesión"""
        if session_id not in self.session_data:
            self.session_data[session_id] = {
                'semester_store_tpv': defaultdict(float),
                'batches_received': 0,
                # Diccionario de JOIN específico de esta sesión
                'store_id_to_name': {},
            }
    
    def get_session_data(self, session_id):
        """Obtiene los datos de una sesión específica"""
        self.initialize_session(session_id)
        return self.session_data[session_id]
        
    def load_stores(self, rows, session_id):
        """Carga stores para construir el diccionario store_id -> store_name para una sesión específica."""
        session_data = self.get_session_data(session_id)
        
        for row in rows:
            store_id = row.get('store_id')
            store_name = row.get('store_name')
            
            if store_id and store_name:
                session_data['store_id_to_name'][store_id] = store_name.strip()
        
        print(f"[AggregatorQuery3] Sesión {session_id}: Cargadas {len(session_data['store_id_to_name'])} stores para JOIN")
    
    def accumulate_tpv(self, rows, session_id):
        """Acumula TPV parciales de group_by_query3 para una sesión específica."""
        session_data = self.get_session_data(session_id)
        
        for row in rows:
            semester = row.get('semester')
            store_id = row.get('store_id')
            total_payment_value = row.get('total_payment_value', 0.0)
            
            # Validar campos requeridos
            if not semester or not store_id:
                continue
                
            # Convertir a tipo correcto
            try:
                if isinstance(total_payment_value, str):
                    total_payment_value = float(total_payment_value)
            except (ValueError, TypeError):
                continue
            
            # Clave compuesta: (semestre, store_id)
            key = (semester, store_id)
            
            # Acumular TPV para esta sesión
            session_data['semester_store_tpv'][key] += total_payment_value
        
        session_data['batches_received'] += 1
        
        if session_data['batches_received'] % 10000 == 0 or session_data['batches_received'] == 1:
            print(f"[AggregatorQuery3] Sesión {session_id}: Procesado batch {session_data['batches_received']} con {len(rows)} registros. Total combinaciones: {len(session_data['semester_store_tpv'])}")
    
    def generate_final_results(self, session_id):
        """Genera los resultados finales para Query 3 con JOIN para una sesión específica."""
        session_data = self.get_session_data(session_id)
        
        print(f"[AggregatorQuery3] Generando resultados finales para sesión {session_id}...")
        print(f"[AggregatorQuery3] Total combinaciones procesadas: {len(session_data['semester_store_tpv'])}")
        print(f"[AggregatorQuery3] Stores disponibles para JOIN: {len(session_data['store_id_to_name'])}")
        
        if not session_data['semester_store_tpv']:
            print(f"[AggregatorQuery3] No hay datos para procesar en sesión {session_id}")
            return []
        
        if not session_data['store_id_to_name']:
            print(f"[AggregatorQuery3] WARNING: No hay stores cargadas para sesión {session_id}. No se puede hacer JOIN.")
            return []
        
        final_results = []
        
        # Procesar cada combinación (semestre, store_id) con JOIN
        for (semester, store_id), total_tpv in session_data['semester_store_tpv'].items():
            # JOIN: buscar store_name para el store_id de esta sesión
            store_name = session_data['store_id_to_name'].get(store_id)
            
            if not store_name:
                print(f"[AggregatorQuery3] WARNING: store_id {store_id} no encontrado en stores")
                continue
            
            # Convertir formato de semestre: "2024-S1" -> "2024-H1", "2024-S2" -> "2024-H2"
            year_half = semester.replace('-S', '-H')
            
            final_results.append({
                'year_half_created_at': year_half,
                'store_name': store_name,
                'tpv': round(total_tpv, 2)  # Redondear TPV a 2 decimales
            })
        
        # Ordenar por semestre y luego por store_name para consistencia
        final_results.sort(key=lambda x: (x['year_half_created_at'], x['store_name']))
        
        print(f"[AggregatorQuery3] Resultados generados: {len(final_results)} combinaciones")
        
        # Mostrar ejemplos
        print(f"[AggregatorQuery3] Ejemplos de resultados:")
        for i, result in enumerate(final_results[:5]):
            print(f"  {i+1}. {result['year_half_created_at']} - {result['store_name']}: TPV ${result['tpv']:,.2f}")
        
        if len(final_results) > 5:
            print(f"  ... y {len(final_results) - 5} más")
        
        # Estadísticas adicionales
        total_tpv_all = sum(r['tpv'] for r in final_results)
        unique_stores = len(set(r['store_name'] for r in final_results))
        unique_semesters = len(set(r['year_half_created_at'] for r in final_results))
        
        print(f"[AggregatorQuery3] Estadísticas: {unique_semesters} semestres, {unique_stores} sucursales, TPV total: ${total_tpv_all:,.2f}")
            
        return final_results

# Instancia global del agregador
aggregator = AggregatorQuery3()
sesssion_traker = SessionTracker(tracked_types=["tpv", "stores"])
 

def on_tpv_message(body):
    """Maneja mensajes de TPV de group_by_query3."""
    try:
        header, rows = deserialize_message(body)
    except Exception as e:
        print(f"[AggregatorQuery3] Error deserializando mensaje: {e}")
        return
        
    session_id = header.get("session_id", "unknown")
    bach_id = int(header.get("batch_id"))
    is_eos = header.get("is_eos") == "true"

    
    if is_eos:
        print(f"[AggregatorQuery3] Recibido mensaje EOS en TPV para sesión {session_id}, batch_id {bach_id}")
        
    if rows:
        aggregator.accumulate_tpv(rows, session_id)
        
    if sesssion_traker.update(session_id, "stores", bach_id, is_eos):
        generate_and_send_results(session_id)

def on_stores_message(body):
    """Maneja mensajes de stores para el JOIN."""
    try:
        header, rows = deserialize_message(body)
    except Exception as e:
        print(f"[AggregatorQuery3] Error deserializando mensaje: {e}")
        return
    
    session_id = header.get("session_id", "unknown")
    bach_id = int(header.get("batch_id", -1))
    if bach_id == -1:
        print(f"[AggregatorQuery3] WARNING: batch_id inválido en sesión {session_id}")
        return

    is_eos = header.get("is_eos") == "true"
    
    if is_eos:
        print(f"[AggregatorQuery3] Recibido mensaje EOS en Stores para sesión {session_id}, batch_id {bach_id}")
    
    if rows:
        aggregator.load_stores(rows, session_id)
        
    if sesssion_traker.update(session_id, "tpv", bach_id, is_eos):
        generate_and_send_results(session_id)

def generate_and_send_results(session_id):
    """Genera y envía los resultados finales cuando ambos flujos terminaron para una sesión específica."""
    
    print(f"[AggregatorQuery3] Ambos flujos completados para sesión {session_id}. Generando resultados finales...")
    
    # Generar resultados finales para esta sesión
    final_results = aggregator.generate_final_results(session_id)
    
    if final_results:
        # Enviar resultados finales con headers completos
        results_header = {
            "type": "result",
            "stream_id": "query3_results",
            "batch_id": "final",
            "is_batch_end": "true",
            "is_eos": "false",
            "query": "query3",
            "session_id": session_id,
            "total_results": str(len(final_results)),
            "description": "TPV_por_semestre_y_sucursal_2024-2025_06:00-23:00",
            "is_final_result": "true"
        }
        
        # Enviar en batches si hay muchos resultados
        batch_size = 100
        total_batches = (len(final_results) + batch_size - 1) // batch_size
        
        for i in range(0, len(final_results), batch_size):
            batch = final_results[i:i + batch_size]
            batch_header = results_header.copy()
            batch_header["batch_number"] = str((i // batch_size) + 1)
            batch_header["total_batches"] = str(total_batches)
            
            result_msg = serialize_message(batch, batch_header)
            results_exchange.send(result_msg)
    
    
    del aggregator.session_data[session_id]
    print(f"[AggregatorQuery3] Resultados finales enviados para sesión {session_id}. Worker continúa activo esperando nuevos clientes...")

def consume_tpv():
        try:
            tpv_queue.start_consuming(on_tpv_message)
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"[AggregatorQuery3] Error en consumo de TPV: {e}")
    
def consume_stores():
    try:
        stores_exchange.start_consuming(on_stores_message)
    except Exception as e:
        if not shutdown_event.is_set():
            print(f"[AggregatorQuery3] Error en consumo de stores: {e}")
                
if __name__ == "__main__":
    shutdown_event = threading.Event()
    
    # Iniciar servidor de healthcheck UDP
    healthcheck_port = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
    start_healthcheck_server(port=healthcheck_port, node_name="aggregator_query3", shutdown_event=shutdown_event)
    print(f"[AggregatorQuery3] Healthcheck server iniciado en puerto UDP {healthcheck_port}")
    
    def signal_handler(signum, frame):
        print(f"[AggregatorQuery3] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada 1: TPV del group_by_query3
    tpv_queue = MessageMiddlewareQueue(RABBIT_HOST, INPUT_QUEUE)
    
    # Entrada 2: stores para JOIN
    stores_exchange = MessageMiddlewareExchange(RABBIT_HOST, STORES_EXCHANGE, [STORES_ROUTING_KEY])
    
    # Salida: exchange para resultados finales
    results_exchange = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] AggregatorQuery3 esperando mensajes...")
    print("[*] Query 3: TPV por semestre y sucursal 2024-2025 (06:00-23:00)")
    print("[*] Consumiendo de 2 fuentes: TPV + stores para JOIN")
    
    try:
        tpv_thread = threading.Thread(target=consume_tpv, daemon=True)
        stores_thread = threading.Thread(target=consume_stores, daemon=True)
        
        tpv_thread.start()
        stores_thread.start()
        
        print("[AggregatorQuery3] Worker iniciado, esperando mensajes de múltiples sesiones...")
        
        # Loop principal - solo termina por señal
        while not shutdown_event.is_set():
            tpv_thread.join(timeout=1)
            stores_thread.join(timeout=1)
            if not tpv_thread.is_alive() and not stores_thread.is_alive():
                break
            
        print("[AggregatorQuery3] Terminando por señal externa")
        
    except KeyboardInterrupt:
        print("\n[AggregatorQuery3] Interrupción recibida")
    finally:
        for mq in [tpv_queue, stores_exchange]:
            try:
                mq.stop_consuming()
            except Exception as e:
                print(f"Error al parar el consumo: {e}")
        
        for mq in [tpv_queue, stores_exchange, results_exchange]:
            try:
                mq.delete()
            except Exception as e:
                print(f"Error al eliminar conexión: {e}")
    
        # Cerrar conexiones
        for mq in [tpv_queue, stores_exchange, results_exchange]:
            try:
                mq.close()
            except Exception as e:
                print(f"Error al cerrar conexión: {e}")
                
        print("[x] AggregatorQuery3 detenido")
