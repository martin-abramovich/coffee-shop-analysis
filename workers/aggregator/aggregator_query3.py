import sys
import os
import signal
import threading
import time
from collections import defaultdict

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
NUM_GROUP_BY_QUERY3_WORKERS = int(os.environ.get('NUM_GROUP_BY_QUERY3_WORKERS', '2'))

INPUT_EXCHANGE = "transactions_query3"    # exchange del group_by query 3
INPUT_ROUTING_KEY = "query3"              # routing key del group_by
OUTPUT_EXCHANGE = "results_query3"        # exchange de salida para resultados finales
ROUTING_KEY = "query3_results"            # routing para resultados

# Control de EOS
eos_count = {}  # {session_id: count}
eos_lock = threading.Lock()

# Exchanges adicionales para JOIN
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
                'eos_received': False,
                'results_sent': False,
                'eos_tpv_done': False,
                # Diccionario de JOIN específico de esta sesión
                'store_id_to_name': {},
                # Control de flujo específico de esta sesión
                'stores_loaded': False,
                'eos_stores_done': False
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
        # Log solo cada 10 batches para no saturar
        if session_data['batches_received'] % 10 == 0 or session_data['batches_received'] == 1:
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

# Variable global para control de shutdown
shutdown_event = None

def on_tpv_message(body):
    """Maneja mensajes de TPV de group_by_query3."""
    try:
        header, rows = deserialize_message(body)
    except Exception as e:
        print(f"[AggregatorQuery3] Error deserializando mensaje: {e}")
        return
    
    session_id = header.get("session_id", "unknown")
    
    # Inicializar contadores por sesión si no existen
    if session_id not in eos_count:
        eos_count[session_id] = 0
    
    # Si ya enviamos resultados para esta sesión, ignorar mensajes adicionales
    session_data = aggregator.get_session_data(session_id)
    if session_data['results_sent']:
        return
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        with eos_lock:
            eos_count[session_id] += 1
            print(f"[AggregatorQuery3] EOS recibido en TPV para sesión {session_id} ({eos_count[session_id]}/{NUM_GROUP_BY_QUERY3_WORKERS})")
            
            # Solo procesar cuando recibimos de TODOS los workers para esta sesión
            if eos_count[session_id] < NUM_GROUP_BY_QUERY3_WORKERS:
                print(f"[AggregatorQuery3] Esperando más EOS para sesión {session_id}...")
                return
            
            print(f"[AggregatorQuery3] EOS recibido de TODOS los workers para sesión {session_id}. Marcando como listo...")
            session_data['eos_received'] = True
            session_data['eos_tpv_done'] = True
            
            # Si ya tenemos stores cargadas para esta sesión y no hemos enviado, generar resultados
            if session_data['stores_loaded'] and not session_data['results_sent']:
                generate_and_send_results(session_id)
        return
    
    # Procesamiento normal: acumular TPV parciales para esta sesión
    if rows:
        aggregator.accumulate_tpv(rows, session_id)

def on_stores_message(body):
    """Maneja mensajes de stores para el JOIN."""
    try:
        header, rows = deserialize_message(body)
    except Exception as e:
        print(f"[AggregatorQuery3] Error deserializando mensaje: {e}")
        return
    
    session_id = header.get("session_id", "unknown")
    session_data = aggregator.get_session_data(session_id)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print(f"[AggregatorQuery3] EOS recibido en stores para sesión {session_id}. Marcando como listo...")
        session_data['stores_loaded'] = True
        session_data['eos_stores_done'] = True
        
        # Generar resultados para esta sesión si ya tiene todo listo
        if session_data['eos_received'] and not session_data['results_sent']:
            print(f"[AggregatorQuery3] Generando resultados para sesión {session_id}...")
            generate_and_send_results(session_id)
        return
    
    # Cargar stores para JOIN en esta sesión
    if rows:
        aggregator.load_stores(rows, session_id)

def generate_and_send_results(session_id):
    """Genera y envía los resultados finales cuando ambos flujos terminaron para una sesión específica."""
    global shutdown_event, mq_out
    
    session_data = aggregator.get_session_data(session_id)
    
    # Evitar procesamiento duplicado para esta sesión
    if session_data['results_sent']:
        print(f"[AggregatorQuery3] Resultados ya enviados para sesión {session_id}, ignorando llamada duplicada")
        return
    
    print(f"[AggregatorQuery3] Ambos flujos completados para sesión {session_id}. Generando resultados finales...")
    
    # Marcar como enviado ANTES de generar para evitar race conditions
    session_data['results_sent'] = True
    
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
            sent = False
            for retry in range(max_retries):
                try:
                    mq_out.send(result_msg)
                    sent = True
                    print(f"[AggregatorQuery3] Enviado batch {batch_header['batch_number']}/{batch_header['total_batches']}")
                    break
                except Exception as e:
                    print(f"[AggregatorQuery3] Error enviando batch {batch_header['batch_number']} (intento {retry+1}/{max_retries}): {e}")
                    if retry < max_retries - 1:
                        try:
                            mq_out.close()
                        except:
                            pass
                        print(f"[AggregatorQuery3] Reconectando exchange de salida...")
                        from middleware.middleware import MessageMiddlewareExchange
                        mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
            
            if not sent:
                print(f"[AggregatorQuery3] CRÍTICO: No se pudo enviar batch {batch_header['batch_number']}")
    
    print(f"[AggregatorQuery3] Resultados finales enviados para sesión {session_id}. Worker continúa activo esperando nuevos clientes...")

if __name__ == "__main__":
    import threading
    
    # Control de EOS - esperamos EOS de ambas fuentes
    shutdown_event = threading.Event()
    
    def signal_handler(signum, frame):
        print(f"[AggregatorQuery3] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada 1: TPV del group_by_query3
    mq_tpv = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Entrada 2: stores para JOIN
    mq_stores = MessageMiddlewareExchange(RABBIT_HOST, STORES_EXCHANGE, [STORES_ROUTING_KEY])
    
    # Salida: exchange para resultados finales
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] AggregatorQuery3 esperando mensajes...")
    print("[*] Query 3: TPV por semestre y sucursal 2024-2025 (06:00-23:00)")
    print("[*] Consumiendo de 2 fuentes: TPV + stores para JOIN")
    
    def consume_tpv():
        try:
            mq_tpv.start_consuming(on_tpv_message)
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"[AggregatorQuery3] Error en consumo de TPV: {e}")
    
    def consume_stores():
        try:
            mq_stores.start_consuming(on_stores_message)
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"[AggregatorQuery3] Error en consumo de stores: {e}")
    
    try:
        # Ejecutar ambos consumidores en paralelo como daemon threads
        tpv_thread = threading.Thread(target=consume_tpv, daemon=True)
        stores_thread = threading.Thread(target=consume_stores, daemon=True)
        
        tpv_thread.start()
        stores_thread.start()
        
        # Esperar indefinidamente - el worker NO termina después de EOS
        # Solo termina por señal externa (SIGTERM, SIGINT)
        print("[AggregatorQuery3] Worker iniciado, esperando mensajes de múltiples sesiones...")
        print("[AggregatorQuery3] El worker continuará procesando múltiples clientes")
        
        # Loop principal - solo termina por señal
        while not shutdown_event.is_set():
            time.sleep(1)
        
        print("[AggregatorQuery3] Terminando por señal externa")
        
    except KeyboardInterrupt:
        print("\n[AggregatorQuery3] Interrupción recibida")
    finally:
        # Detener consumo
        try:
            mq_tpv.stop_consuming()
        except:
            pass
        try:
            mq_stores.stop_consuming()
        except:
            pass
        
        # Cerrar conexiones
        try:
            mq_tpv.close()
        except:
            pass
        try:
            mq_stores.close()
        except:
            pass
        try:
            mq_out.close()
        except:
            pass
        print("[x] AggregatorQuery3 detenido")
