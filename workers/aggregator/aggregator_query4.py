import sys
import os
import signal
import threading
import time
import re
from collections import defaultdict
from datetime import datetime

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

def log_with_timestamp(message):
    """Función para logging con timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] {message}")

from middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
NUM_GROUP_BY_QUERY4_WORKERS = int(os.environ.get('NUM_GROUP_BY_QUERY4_WORKERS', '2'))

INPUT_EXCHANGE = "transactions_query4"    # exchange del group_by query 4
INPUT_ROUTING_KEY = "query4"              # routing key del group_by
OUTPUT_EXCHANGE = "results_query4"        # exchange de salida para resultados finales
ROUTING_KEY = "query4_results"            # routing para resultados

# Control de EOS
eos_count = {}  # {session_id: count}
eos_lock = threading.Lock()

# Exchanges adicionales para JOIN
STORES_EXCHANGE = "stores_raw"
STORES_ROUTING_KEY = "q4"
USERS_QUEUE = "users_raw"

def canonicalize_id(value):
    """Normaliza IDs: si vienen como "123.0" convertir a "123"; sino, devolver strip()."""
    if value is None:
        return ''
    if not isinstance(value, str):
        value = str(value)
    s = value.strip()
    if '.' in s:
        left, right = s.split('.', 1)
        if right.strip('0') == '':
            return left
    return s

class AggregatorQuery4:
    def __init__(self):
        # Datos por sesión: {session_id: session_data}
        self.session_data = {}
        
        # Diccionarios globales para JOIN (compartidos entre sesiones)
        self.store_id_to_name = {}  # store_id -> store_name
        self.user_id_to_birthdate = {}  # user_id -> birthdate
        
        # Control de flujo global
        self.stores_loaded = False
        self.users_loaded = False
        self.eos_stores_done = False
        self.eos_users_done = False
    
    def initialize_session(self, session_id):
        """Inicializa datos para una nueva sesión"""
        if session_id not in self.session_data:
            self.session_data[session_id] = {
                'store_user_transactions': defaultdict(int),
                'batches_received': 0,
                'eos_received': False,
                'results_sent': False,
                'eos_transactions_done': False
            }
    
    def get_session_data(self, session_id):
        """Obtiene los datos de una sesión específica"""
        self.initialize_session(session_id)
        return self.session_data[session_id]
        
    def load_stores(self, rows):
        """Carga stores para construir el diccionario store_id -> store_name."""
        for row in rows:
            store_id = row.get('store_id')
            store_name = row.get('store_name')
            
            if store_id and store_name:
                normalized_store_id = canonicalize_id(store_id)
                normalized_store_name = store_name.strip()
                if normalized_store_id:
                    self.store_id_to_name[normalized_store_id] = normalized_store_name
        
        print(f"[AggregatorQuery4] Stores cargadas para JOIN: {len(self.store_id_to_name)}")
    
    def load_users(self, rows):
        """Carga users para construir el diccionario user_id -> birthdate."""
        for row in rows:
            user_id = row.get('user_id')
            birthdate = row.get('birthdate')
            
            # Aceptar birthdate vacío como "sin fecha" y no indexarlo
            if not user_id:
                continue
            if birthdate is None:
                continue
            normalized_user_id = canonicalize_id(user_id)
            normalized_birthdate = birthdate.strip()
            if normalized_birthdate == "":
                # sin fecha
                continue
            # Validar formato simple YYYY-MM-DD
            is_valid_format = bool(re.match(r"^\d{4}-\d{2}-\d{2}$", normalized_birthdate))
            if not is_valid_format:
                continue
            if normalized_user_id:
                self.user_id_to_birthdate[normalized_user_id] = normalized_birthdate
        
        print(f"[AggregatorQuery4] Users cargados para JOIN: {len(self.user_id_to_birthdate)}")
    
    def accumulate_transactions(self, rows, session_id):
        """Acumula conteos de transacciones de group_by_query4 para una sesión específica."""
        session_data = self.get_session_data(session_id)
        processed_count = 0
        
        for row in rows:
            store_id = row.get('store_id')
            user_id = row.get('user_id')
            transaction_count = row.get('transaction_count', 0)
            
            # Validar campos requeridos
            if not store_id or not user_id:
                continue
                
            # Convertir a tipo correcto
            try:
                if isinstance(transaction_count, str):
                    transaction_count = int(transaction_count)
            except (ValueError, TypeError):
                continue
            
            # Clave compuesta: (store_id, user_id)
            normalized_store_id = canonicalize_id(store_id)
            normalized_user_id = canonicalize_id(user_id)
            if not normalized_store_id or not normalized_user_id:
                continue
            key = (normalized_store_id, normalized_user_id)
            
            # Acumular conteo de transacciones para esta sesión
            session_data['store_user_transactions'][key] += transaction_count
            processed_count += 1
        
        session_data['batches_received'] += 1
        
        # OPTIMIZACIÓN: Logs menos frecuentes para mejor performance
        if session_data['batches_received'] % 100 == 0 or session_data['batches_received'] <= 5:
            print(f"[AggregatorQuery4] Sesión {session_id}: Batch {session_data['batches_received']}: {processed_count}/{len(rows)} procesados; total combinaciones={len(session_data['store_user_transactions'])}")
    
    def generate_final_results(self, session_id):
        """Genera los resultados finales para Query 4 con doble JOIN y TOP 3 para una sesión específica."""
        session_data = self.get_session_data(session_id)
        
        print(f"[AggregatorQuery4] Generando resultados para sesión {session_id}... combinaciones={len(session_data['store_user_transactions'])}, stores={len(self.store_id_to_name)}, users={len(self.user_id_to_birthdate)}")
        
        if not session_data['store_user_transactions']:
            print(f"[AggregatorQuery4] No hay datos para procesar en sesión {session_id}")
            return []
        
        if not self.store_id_to_name:
            print(f"[AggregatorQuery4] WARNING: No hay stores cargadas. No se puede hacer JOIN.")
            return []
            
        if not self.user_id_to_birthdate:
            print(f"[AggregatorQuery4] WARNING: No hay users cargados. No se puede hacer JOIN.")
            return []
        
        # OPTIMIZACIÓN: Agrupar por store_id para encontrar TOP 3 por sucursal
        transactions_by_store = defaultdict(list)
        missing_stores = set()
        missing_users = set()
        
        for (store_id, user_id), transaction_count in session_data['store_user_transactions'].items():
            # JOIN con stores: obtener store_name
            store_name = self.store_id_to_name.get(store_id)
            if not store_name:
                missing_stores.add(store_id)
                continue
            
            # JOIN con users: obtener birthdate
            birthdate = self.user_id_to_birthdate.get(user_id)
            if not birthdate:
                missing_users.add(user_id)
                continue
            
            transactions_by_store[store_id].append({
                'store_name': store_name,
                'user_id': user_id,
                'birthdate': birthdate,
                'transaction_count': transaction_count
            })
        
        # Log de warnings solo si hay muchos faltantes
        if len(missing_stores) > 0:
            print(f"[AggregatorQuery4] WARNING: {len(missing_stores)} store_ids no encontrados en stores")
        if len(missing_users) > 0:
            print(f"[AggregatorQuery4] WARNING: {len(missing_users)} user_ids no encontrados en users")
        
        final_results = []
        
        # Para cada sucursal, seleccionar TOP 3 clientes
        for store_id, customers in transactions_by_store.items():
            # Ordenar por transaction_count descendente
            customers_sorted = sorted(customers, key=lambda x: x['transaction_count'], reverse=True)
            
            # Seleccionar TOP 3
            top3_customers = customers_sorted[:3]
            
            store_name = top3_customers[0]['store_name'] if top3_customers else "Unknown"
            
            # OPTIMIZACIÓN: Log más compacto
            print(f"[AggregatorQuery4] {store_name}: TOP 3 de {len(customers)} clientes")
            
            # Agregar resultados del TOP 3
            for customer in top3_customers:
                final_results.append({
                    'store_name': customer['store_name'],
                    'birthdate': customer['birthdate']
                })
        
        # Ordenar resultados por store_name
        final_results.sort(key=lambda x: x['store_name'])
        
        print(f"[AggregatorQuery4] Resultados generados: {len(final_results)} registros de TOP 3")
        
        # Estadísticas
        unique_stores = len(set(r['store_name'] for r in final_results))
        print(f"[AggregatorQuery4] Estadísticas: {unique_stores} sucursales procesadas")
            
        return final_results

# Instancia global del agregador
aggregator = AggregatorQuery4()

# Variable global para control de shutdown
shutdown_event = None

def on_transactions_message(body):
    """Maneja mensajes de conteos de transacciones de group_by_query4."""
    try:
        header, rows = deserialize_message(body)
    except Exception as e:
        print(f"[AggregatorQuery4] Error deserializando mensaje: {e}")
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
            log_with_timestamp(f"[AggregatorQuery4] EOS recibido en transacciones para sesión {session_id} ({eos_count[session_id]}/{NUM_GROUP_BY_QUERY4_WORKERS})")
            
            # Solo procesar cuando recibimos de TODOS los workers para esta sesión
            if eos_count[session_id] < NUM_GROUP_BY_QUERY4_WORKERS:
                print(f"[AggregatorQuery4] Esperando más EOS para sesión {session_id}...")
                return
            
            log_with_timestamp(f"[AggregatorQuery4] ✅ EOS recibido de TODOS los workers para sesión {session_id}. Marcando como listo...")
            session_data['eos_received'] = True
            session_data['eos_transactions_done'] = True
            
            # Si ya tenemos stores y users cargados, generar resultados para esta sesión
            if aggregator.stores_loaded and aggregator.users_loaded and not session_data['results_sent']:
                generate_and_send_results(session_id)
        return
    
    # Procesamiento normal: acumular conteos de transacciones para esta sesión
    if rows:
        aggregator.accumulate_transactions(rows, session_id)

def on_stores_message(body):
    """Maneja mensajes de stores para el JOIN."""
    try:
        header, rows = deserialize_message(body)
    except Exception as e:
        print(f"[AggregatorQuery4] Error deserializando mensaje: {e}")
        return
    
    session_id = header.get("session_id", "unknown")
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print(f"[AggregatorQuery4] EOS recibido en stores para sesión {session_id}. Marcando como listo...")
        aggregator.stores_loaded = True
        aggregator.eos_stores_done = True
        
        # Generar resultados para todas las sesiones que estén esperando
        for sid, sdata in aggregator.session_data.items():
            if sdata['eos_received'] and aggregator.users_loaded and not sdata['results_sent']:
                print(f"[AggregatorQuery4] Generando resultados para sesión {sid}...")
                generate_and_send_results(sid)
        return
    
    # Cargar stores para JOIN
    if rows:
        # OPTIMIZACIÓN: Eliminar logs de debug excesivos
        aggregator.load_stores(rows)

def on_users_message(body):
    """Maneja mensajes de users para el JOIN."""
    try:
        header, rows = deserialize_message(body)
    except Exception as e:
        print(f"[AggregatorQuery4] Error deserializando mensaje: {e}")
        return
    
    session_id = header.get("session_id", "unknown")
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print(f"[AggregatorQuery4] EOS recibido en users para sesión {session_id}. Marcando como listo...")
        aggregator.users_loaded = True
        aggregator.eos_users_done = True
        
        # Generar resultados para todas las sesiones que estén esperando
        for sid, sdata in aggregator.session_data.items():
            if sdata['eos_received'] and aggregator.stores_loaded and not sdata['results_sent']:
                print(f"[AggregatorQuery4] Generando resultados para sesión {sid}...")
                generate_and_send_results(sid)
        return
    
    # Cargar users para JOIN
    if rows:
        # OPTIMIZACIÓN: Eliminar logs de debug excesivos
        aggregator.load_users(rows)

def generate_and_send_results(session_id):
    """Genera y envía los resultados finales cuando todos los flujos terminaron para una sesión específica."""
    global shutdown_event, mq_out
    
    session_data = aggregator.get_session_data(session_id)
    
    # Evitar procesamiento duplicado para esta sesión
    if session_data['results_sent']:
        print(f"[AggregatorQuery4] ⚠️ Resultados ya enviados para sesión {session_id}, ignorando llamada duplicada")
        return
    
    print(f"[AggregatorQuery4] 🔚 Todos los flujos completados para sesión {session_id}. Generando resultados finales...")
    
    # Marcar como enviado ANTES de generar para evitar race conditions
    session_data['results_sent'] = True
    
    # Generar resultados finales para esta sesión
    final_results = aggregator.generate_final_results(session_id)
    
    if final_results:
        # Enviar resultados finales con headers completos
        results_header = {
            "type": "result",
            "stream_id": "query4_results",
            "batch_id": "final",
            "is_batch_end": "true",
            "is_eos": "false",
            "query": "query4",
            "total_results": str(len(final_results)),
            "description": "TOP_3_clientes_por_sucursal_mas_compras_2024-2025",
            "is_final_result": "true"
        }
        
        # Enviar en batches si hay muchos resultados
        batch_size = 30  # Batches más pequeños para TOP 3
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
                    break
                except Exception as e:
                    print(f"[AggregatorQuery4] Error enviando batch {batch_header['batch_number']} (intento {retry+1}/{max_retries}): {e}")
                    if retry < max_retries - 1:
                        # Reconectar
                        try:
                            mq_out.close()
                        except:
                            pass
                        print(f"[AggregatorQuery4] Reconectando exchange de salida...")
                        mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
            
            if not sent:
                print(f"[AggregatorQuery4] CRÍTICO: No se pudo enviar batch {batch_header['batch_number']}")
                return
            
            # Log solo si hay pocos batches o cada 10
            if total_batches <= 5 or (i // batch_size + 1) % 10 == 0:
                print(f"[AggregatorQuery4] Enviado batch {batch_header['batch_number']}/{batch_header['total_batches']}")
    
    print("[AggregatorQuery4] Resultados finales enviados. Deteniendo consumidores...")
    
    # Detener consumidores inmediatamente para evitar loops
    try:
        mq_transactions.stop_consuming()
        print("[AggregatorQuery4] ✓ Consumidor de transacciones detenido")
    except Exception as e:
        print(f"[AggregatorQuery4] Error deteniendo transacciones: {e}")
    
    try:
        mq_stores.stop_consuming()
        print("[AggregatorQuery4] ✓ Consumidor de stores detenido")
    except Exception as e:
        print(f"[AggregatorQuery4] Error deteniendo stores: {e}")
    
    try:
        mq_users.stop_consuming()
        print("[AggregatorQuery4] ✓ Consumidor de users detenido")
    except Exception as e:
        print(f"[AggregatorQuery4] Error deteniendo users: {e}")
    
    print(f"[AggregatorQuery4] Resultados finales enviados para sesión {session_id}. Worker continúa activo.")

if __name__ == "__main__":
    import threading
    
    # Control de EOS - esperamos EOS de las 3 fuentes
    shutdown_event = threading.Event()
    
    def signal_handler(signum, frame):
        print(f"[AggregatorQuery4] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada 1: conteos de transacciones del group_by_query4
    mq_transactions = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Entrada 2: stores para JOIN
    mq_stores = MessageMiddlewareExchange(RABBIT_HOST, STORES_EXCHANGE, [STORES_ROUTING_KEY])
    
    # Entrada 3: users para JOIN
    mq_users = MessageMiddlewareQueue(RABBIT_HOST, USERS_QUEUE)
    
    # Salida: exchange para resultados finales
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] AggregatorQuery4 esperando mensajes...")
    print("[*] Query 4: TOP 3 clientes por sucursal (más compras 2024-2025)")
    print("[*] Consumiendo de 3 fuentes: transacciones + stores + users para doble JOIN")
    
    def consume_transactions():
        try:
            mq_transactions.start_consuming(on_transactions_message)
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"[AggregatorQuery4] Error en consumo de transacciones: {e}")
    
    def consume_stores():
        try:
            mq_stores.start_consuming(on_stores_message)
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"[AggregatorQuery4] Error en consumo de stores: {e}")
            
    def consume_users():
        try:
            mq_users.start_consuming(on_users_message)
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"[AggregatorQuery4] Error en consumo de users: {e}")
    
    try:
        # Ejecutar los 3 consumidores en paralelo como daemon threads
        transactions_thread = threading.Thread(target=consume_transactions, daemon=True)
        stores_thread = threading.Thread(target=consume_stores, daemon=True)
        users_thread = threading.Thread(target=consume_users, daemon=True)
        
        transactions_thread.start()
        stores_thread.start()
        users_thread.start()
        
        # Esperar indefinidamente - el worker NO termina después de EOS
        # Solo termina por señal externa (SIGTERM, SIGINT)
        print("[AggregatorQuery4] ✅ Worker iniciado, esperando mensajes de múltiples sesiones...")
        print("[AggregatorQuery4] 💡 El worker continuará procesando múltiples clientes")
        
        # Loop principal - solo termina por señal
        while not shutdown_event.is_set():
            time.sleep(1)
        
        print("[AggregatorQuery4] ✅ Terminando por señal externa")
        
    except KeyboardInterrupt:
        print("\n[AggregatorQuery4] Interrupción recibida")
    finally:
        # Detener consumo
        try:
            mq_transactions.stop_consuming()
        except:
            pass
        try:
            mq_stores.stop_consuming()
        except:
            pass
        try:
            mq_users.stop_consuming()
        except:
            pass
        
        # Cerrar conexiones
        try:
            mq_transactions.close()
        except:
            pass
        try:
            mq_stores.close()
        except:
            pass
        try:
            mq_users.close()
        except:
            pass
        try:
            mq_out.close()
        except:
            pass
        print("[x] AggregatorQuery4 detenido")
