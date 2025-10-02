import sys
import os
import signal
from collections import defaultdict

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
INPUT_EXCHANGE = "transactions_query4"    # exchange del group_by_query4
INPUT_ROUTING_KEY = "query4"              # routing key del group_by_query4
STORES_EXCHANGE = "stores_raw"            # exchange de stores para JOIN
STORES_ROUTING_KEY = "q4"                 # routing key para query 4
USERS_QUEUE = "users_raw"                 # cola de users para JOIN
OUTPUT_EXCHANGE = "results_query4"        # exchange de salida para resultados finales
ROUTING_KEY = "query4_results"            # routing para resultados

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
        # Acumulador de transaction_count por (store_id, user_id)
        # Estructura: {(store_id, user_id): transaction_count}
        self.store_user_transactions = defaultdict(int)
        
        # Diccionarios para JOIN
        self.store_id_to_name = {}  # store_id -> store_name
        self.user_id_to_birthdate = {}  # user_id -> birthdate
        
        # Control de flujo
        self.batches_received = 0
        self.stores_loaded = False
        self.users_loaded = False
        self.eos_received = False
        self.results_sent = False
        
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
            
            if user_id and birthdate:
                normalized_user_id = canonicalize_id(user_id)
                normalized_birthdate = birthdate.strip()
                if normalized_user_id:
                    self.user_id_to_birthdate[normalized_user_id] = normalized_birthdate
        
        print(f"[AggregatorQuery4] Users cargados para JOIN: {len(self.user_id_to_birthdate)}")
    
    def accumulate_transactions(self, rows):
        """Acumula conteos de transacciones de group_by_query4."""
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
            
            # Acumular conteo de transacciones
            self.store_user_transactions[key] += transaction_count
        
        self.batches_received += 1
        # Logs compactos
        print(f"[AggregatorQuery4] Batch {self.batches_received}: {len(rows)} registros; total combinaciones={len(self.store_user_transactions)}")
    
    def generate_final_results(self):
        """Genera los resultados finales para Query 4 con doble JOIN y TOP 3."""
        print(f"[AggregatorQuery4] Generando resultados... combinaciones={len(self.store_user_transactions)}, stores={len(self.store_id_to_name)}, users={len(self.user_id_to_birthdate)}")
        
        if not self.store_user_transactions:
            print(f"[AggregatorQuery4] No hay datos para procesar")
            return []
        
        if not self.store_id_to_name:
            print(f"[AggregatorQuery4] WARNING: No hay stores cargadas. No se puede hacer JOIN.")
            return []
            
        if not self.user_id_to_birthdate:
            print(f"[AggregatorQuery4] WARNING: No hay users cargados. No se puede hacer JOIN.")
            return []
        
        # Agrupar por store_id para encontrar TOP 3 por sucursal
        transactions_by_store = defaultdict(list)
        
        for (store_id, user_id), transaction_count in self.store_user_transactions.items():
            # JOIN con stores: obtener store_name
            store_name = self.store_id_to_name.get(store_id)
            if not store_name:
                # Mantener advertencia importante
                print(f"[AggregatorQuery4] WARNING: store_id {store_id} no encontrado en stores")
                continue
            
            # JOIN con users: obtener birthdate
            birthdate = self.user_id_to_birthdate.get(user_id)
            if not birthdate:
                print(f"[AggregatorQuery4] WARNING: user_id {user_id} no encontrado en users")
                continue
            
            transactions_by_store[store_id].append({
                'store_name': store_name,
                'user_id': user_id,
                'birthdate': birthdate,
                'transaction_count': transaction_count
            })
        
        final_results = []
        
        # Para cada sucursal, seleccionar TOP 3 clientes
        for store_id, customers in transactions_by_store.items():
            # Ordenar por transaction_count descendente
            customers_sorted = sorted(customers, key=lambda x: x['transaction_count'], reverse=True)
            
            # Seleccionar TOP 3
            top3_customers = customers_sorted[:3]
            
            store_name = top3_customers[0]['store_name'] if top3_customers else "Unknown"
            
            # Reducir detalle por sucursal para no saturar
            print(f"[AggregatorQuery4] {store_name}: TOP 3 de {len(customers)} clientes")
            
            # Agregar resultados del TOP 3
            for i, customer in enumerate(top3_customers):
                final_results.append({
                    'store_name': customer['store_name'],
                    'birthdate': customer['birthdate']
                })
                
                # Detalle por usuario opcionalmente podría quitarse si aún hay ruido
                print(f"  {i+1}. User {customer['user_id']}: {customer['transaction_count']} transacciones, nacido {customer['birthdate']}")
        
        # Ordenar resultados por store_name y luego por transaction_count (implícito en el orden)
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
    # Si ya enviamos resultados, ignorar mensajes adicionales
    if aggregator.results_sent:
        return
    
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print("[AggregatorQuery4] EOS recibido en transacciones. Marcando como listo...")
        aggregator.eos_received = True
        
        # Si ya tenemos stores y users cargados, generar resultados (una sola vez)
        if aggregator.stores_loaded and aggregator.users_loaded and not aggregator.results_sent:
            generate_and_send_results()
        return
    
    # Procesamiento normal: acumular conteos de transacciones
    if rows:
        try:
            sample_keys = list(rows[0].keys()) if isinstance(rows[0], dict) else []
            print(f"[AggregatorQuery4] DEBUG tx batch size={len(rows)} keys={sample_keys}")
            if rows and isinstance(rows[0], dict):
                print(f"[AggregatorQuery4] DEBUG tx sample={rows[0]}")
        except Exception as _:
            pass
        aggregator.accumulate_transactions(rows)

def on_stores_message(body):
    """Maneja mensajes de stores para el JOIN."""
    # Si ya enviamos resultados, ignorar mensajes adicionales
    if aggregator.results_sent:
        return
    
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print("[AggregatorQuery4] EOS recibido en stores. Marcando como listo...")
        aggregator.stores_loaded = True
        
        # Si ya recibimos EOS de transacciones y users cargados, generar resultados (una sola vez)
        if aggregator.eos_received and aggregator.users_loaded and not aggregator.results_sent:
            generate_and_send_results()
        return
    
    # Cargar stores para JOIN
    if rows:
        try:
            sample_keys = list(rows[0].keys()) if isinstance(rows[0], dict) else []
            print(f"[AggregatorQuery4] DEBUG stores batch size={len(rows)} keys={sample_keys}")
            if rows and isinstance(rows[0], dict):
                print(f"[AggregatorQuery4] DEBUG stores sample={rows[0]}")
        except Exception as _:
            pass
        aggregator.load_stores(rows)

def on_users_message(body):
    """Maneja mensajes de users para el JOIN."""
    # Si ya enviamos resultados, ignorar mensajes adicionales
    if aggregator.results_sent:
        return
    
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print("[AggregatorQuery4] EOS recibido en users. Marcando como listo...")
        aggregator.users_loaded = True
        
        # Si ya recibimos EOS de transacciones y stores cargadas, generar resultados (una sola vez)
        if aggregator.eos_received and aggregator.stores_loaded and not aggregator.results_sent:
            generate_and_send_results()
        return
    
    # Cargar users para JOIN
    if rows:
        try:
            sample_keys = list(rows[0].keys()) if isinstance(rows[0], dict) else []
            print(f"[AggregatorQuery4] DEBUG users batch size={len(rows)} keys={sample_keys}")
            if rows and isinstance(rows[0], dict):
                print(f"[AggregatorQuery4] DEBUG users sample={rows[0]}")
        except Exception as _:
            pass
        aggregator.load_users(rows)

def generate_and_send_results():
    """Genera y envía los resultados finales cuando todos los flujos terminaron."""
    global shutdown_event
    
    # Evitar procesamiento duplicado
    if aggregator.results_sent:
        print("[AggregatorQuery4] ⚠️ Resultados ya enviados, ignorando llamada duplicada")
        return
    
    print("[AggregatorQuery4] 🔚 Todos los flujos completados. Generando resultados finales...")
    
    # Marcar como enviado ANTES de generar para evitar race conditions
    aggregator.results_sent = True
    
    # Generar resultados finales
    final_results = aggregator.generate_final_results()
    
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
            try:
                mq_out.send(result_msg)
            except Exception as e:
                print(f"[AggregatorQuery4] ❌ Error enviando resultados: {e}")
                return
            print(f"[AggregatorQuery4] ✅ Enviado batch {batch_header['batch_number']}/{batch_header['total_batches']} con {len(batch)} registros")
    
    print("[AggregatorQuery4] 🎉 Resultados finales enviados. Deteniendo consumidores...")
    
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
    
    # Señalar shutdown
    if shutdown_event:
        shutdown_event.set()

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
    print("[*] 🎯 Esperará hasta recibir EOS de las 3 fuentes para generar reporte")
    
    def consume_transactions():
        try:
            mq_transactions.start_consuming(on_transactions_message)
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"[AggregatorQuery4] ❌ Error en consumo de transacciones: {e}")
    
    def consume_stores():
        try:
            mq_stores.start_consuming(on_stores_message)
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"[AggregatorQuery4] ❌ Error en consumo de stores: {e}")
            
    def consume_users():
        try:
            mq_users.start_consuming(on_users_message)
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"[AggregatorQuery4] ❌ Error en consumo de users: {e}")
    
    try:
        # Ejecutar los 3 consumidores en paralelo como daemon threads
        transactions_thread = threading.Thread(target=consume_transactions, daemon=True)
        stores_thread = threading.Thread(target=consume_stores, daemon=True)
        users_thread = threading.Thread(target=consume_users, daemon=True)
        
        transactions_thread.start()
        stores_thread.start()
        users_thread.start()
        
        # Esperar hasta recibir EOS o timeout
        timeout = 300  # 5 minutos timeout
        if shutdown_event.wait(timeout):
            print("[AggregatorQuery4] ✅ Terminando por EOS completo o señal")
        else:
            print(f"[AggregatorQuery4] ⏰ Timeout después de {timeout}s, terminando...")
        
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
