import logging
import sys
import os
import signal
import threading
import time
import re
from collections import defaultdict
from datetime import datetime
import traceback

from common.logger import init_log
from workers.aggregator.sesion_state_manager import SessionStateManager
from workers.session_tracker import SessionTracker

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))


from middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from common.healthcheck import start_healthcheck_server
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')

INPUT_EXCHANGE = "transactions_query4"    # exchange del group_by query 4
INPUT_ROUTING_KEY = "query4"              # routing key del group_by
OUTPUT_EXCHANGE = "results_query4"        # exchange de salida para resultados finales
ROUTING_KEY = "query4_results"            # routing para resultados


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
        
        self.shutdown_event = threading.Event()
        self.session_tracker = SessionTracker(["transactions", "stores", "users"])
        self.state_manager = SessionStateManager()
        
        #in 
        self.transactions_queue = None
        self.stores_exchange = None
        self.users_queue = None        

    
    def __initialize_session(self, session_id):
        """Inicializa datos para una nueva sesión"""
        if session_id not in self.session_data:
            self.session_data[session_id] = {
                "transactions":{
                    'store_user_transactions': defaultdict(int),
                    'batches_received': 0,
                },
                # Diccionarios de JOIN
                'stores': {},  # store_id -> store_name
                'users': {},  # user_id -> birthdate
            }
    
    def __get_session_data(self, session_id):
        """Obtiene los datos de una sesión específica"""
        self.__initialize_session(session_id)
        return self.session_data[session_id]
        
    def __load_stores(self, rows, session_id):
        """Carga stores para construir el diccionario store_id -> store_name para una sesión específica."""
        session_data = self.__get_session_data(session_id)
        
        for row in rows:
            store_id = row.get('store_id')
            store_name = row.get('store_name')
            
            if store_id and store_name:
                normalized_store_id = canonicalize_id(store_id)
                normalized_store_name = store_name.strip()
                if normalized_store_id:
                    session_data['stores'][int(normalized_store_id)] = normalized_store_name
        
        logger.info(f"[AggregatorQuery4] Sesión {session_id}: Stores cargadas para JOIN: {len(session_data['stores'])}")
    
    def __load_users(self, rows, session_id):
        """Carga users para construir el diccionario user_id -> birthdate para una sesión específica."""
        session_data = self.__get_session_data(session_id)
        new_users = {}
        
        for row in rows:
            user_id = row.get('user_id')
            birthdate = row.get('birthdate')
            
            # Aceptar birthdate vacío como "sin fecha" y no indexarlo
            if not user_id:
                continue
            if birthdate is None:
                continue
            normalized_user_id = int(canonicalize_id(user_id))
            normalized_birthdate = birthdate.strip()
            if normalized_birthdate == "":
                # sin fecha
                continue
            # Validar formato simple YYYY-MM-DD
            is_valid_format = bool(re.match(r"^\d{4}-\d{2}-\d{2}$", normalized_birthdate))
            if not is_valid_format:
                continue
            if normalized_user_id:
                session_data['users'][normalized_user_id] = normalized_birthdate
                new_users[normalized_user_id] = normalized_birthdate
        
        return new_users
    
    def __accumulate_transactions(self, rows, session_id):
        """Acumula conteos de transacciones de group_by_query4 para una sesión específica."""
        session_data = self.__get_session_data(session_id)
        
        store_user_transactions = session_data["transactions"]["store_user_transactions"]
        session_data["transactions"]['batches_received'] += 1
        batches_received = session_data["transactions"]['batches_received']
        
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
            key = (int(normalized_store_id), int(normalized_user_id))
            
            
            # Acumular conteo de transacciones para esta sesión
            store_user_transactions[key] += transaction_count
            processed_count += 1
        
                
        if batches_received % 10000 == 0 or batches_received <= 5:
            logger.info(f"[AggregatorQuery4] Sesión {session_id}: Batch {batches_received}: {processed_count}/{len(rows)} procesados; total combinaciones={len(store_user_transactions)}")
        
          
    def __generate_final_results(self, session_id):
        """Genera los resultados finales para Query 4 con doble JOIN y TOP 3 para una sesión específica."""
        session_data = self.__get_session_data(session_id)
        
        store_user_transactions = session_data["transactions"]["store_user_transactions"]
           
        logger.info(f"[AggregatorQuery4] Generando resultados para sesión {session_id}... combinaciones={len(store_user_transactions)}, stores={len(session_data['stores'])}, users={len(session_data['users'])}")
        
        if not store_user_transactions:
            logger.warning(f"[AggregatorQuery4] No hay datos para procesar en sesión {session_id}")
            return []
        
        if not session_data['stores']:
            logger.warning(f"[AggregatorQuery4] WARNING: No hay stores cargadas para sesión {session_id}. No se puede hacer JOIN.")
            return []
            
        if not session_data['users']:
            logger.warning(f"[AggregatorQuery4] WARNING: No hay users cargados para sesión {session_id}. No se puede hacer JOIN.")
            return []
        
        # OPTIMIZACIÓN: Agrupar por store_id para encontrar TOP 3 por sucursal
        transactions_by_store = defaultdict(list)
        missing_stores = set()
        missing_users = set()
        
        for (store_id, user_id), transaction_count in store_user_transactions.items():
            # JOIN con stores: obtener store_name de esta sesión
            store_name = session_data['stores'].get(store_id)
            if not store_name:
                missing_stores.add(store_id)
                continue
            
            # JOIN con users: obtener birthdate de esta sesión
            birthdate = session_data['users'].get(user_id)
            if not birthdate:
                missing_users.add(user_id)
                continue
            
            transactions_by_store[store_id].append({
                'store_name': store_name,
                'user_id': user_id,
                'birthdate': birthdate,
                'transaction_count': transaction_count
            })
        
        if len(missing_stores) > 0:
            logger.warning(f"[AggregatorQuery4] WARNING: {len(missing_stores)} store_ids no encontrados en stores")
        if len(missing_users) > 0:
            logger.warning(f"[AggregatorQuery4] WARNING: {len(missing_users)} user_ids no encontrados en users")
        
        final_results = []
        
        # Para cada sucursal, seleccionar TOP 3 clientes
        for store_id, customers in transactions_by_store.items():
            # Ordenar por transaction_count descendente
            customers_sorted = sorted(customers, key=lambda x: x['transaction_count'], reverse=True)
            
            # Seleccionar TOP 3
            top3_customers = customers_sorted[:3]
            
            store_name = top3_customers[0]['store_name'] if top3_customers else "Unknown"
            
            # OPTIMIZACIÓN: Log más compacto
            logger.info(f"[AggregatorQuery4] {store_name}: TOP 3 de {len(customers)} clientes")
            
            # Agregar resultados del TOP 3
            for customer in top3_customers:
                final_results.append({
                    'store_name': customer['store_name'],
                    'birthdate': customer['birthdate']
                })
        
        # Ordenar resultados por store_name
        final_results.sort(key=lambda x: x['store_name'])
        
        logger.info(f"[AggregatorQuery4] Resultados generados: {len(final_results)} registros de TOP 3")
            
        return final_results
    
   
    def __generate_and_send_results(self, session_id):
        """Genera y envía los resultados finales cuando todos los flujos terminaron para una sesión específica."""
            
        logger.info(f"[AggregatorQuery4] Todos los flujos completados para sesión {session_id}. Generando resultados finales...")
        
        # Generar resultados finales para esta sesión
        final_results = self.__generate_final_results(session_id)
        
        if final_results:
            # Enviar resultados finales con headers completos incluyendo session_id
            results_header = {
                "type": "result",
                "stream_id": "query4_results",
                "batch_id": "final",
                "is_batch_end": "true",
                "is_eos": "false",
                "query": "query4",
                "session_id": session_id,
                "total_results": str(len(final_results)),
                "description": "TOP_3_clientes_por_sucursal_mas_compras_2024-2025",
                "is_final_result": "true"
            }
            
            # Enviar en batches si hay muchos resultados
            batch_size = 1000  # Batches más pequeños para TOP 3
            total_batches = (len(final_results) + batch_size - 1) // batch_size
            
            results_exchange = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
            
            for i in range(0, len(final_results), batch_size):
                batch = final_results[i:i + batch_size]
                batch_header = results_header.copy()
                batch_header["batch_number"] = str((i // batch_size) + 1)
                batch_header["total_batches"] = str(total_batches)
                
                result_msg = serialize_message(batch, batch_header)
                results_exchange.send(result_msg)
            
            results_exchange.close()
             
        logger.info(f"[AggregatorQuery4] Resultados finales enviados para sesión {session_id}. Worker continúa activo esperando nuevos clientes...")
        
        
    def __del_session(self, session_id):
        #data en memoria
        if session_id in self.session_data:
            del self.session_data[session_id]
        
        #data en disco
        self.state_manager.delete_session(session_id)
          
    def __save_session_type(self, session_id, type: str):
        tracker_snap = self.session_tracker.get_single_session_type_snapshot(session_id, type)
        session_snap = self.__get_session_data(session_id)[type]
                    
        self.state_manager.save_type_state(session_id, type, session_snap, tracker_snap)
    
    def __save_session_type_add(self, session_id, type: str, new_data):
        tracker_snap = self.session_tracker.get_single_session_type_snapshot(session_id, type)
                    
        self.state_manager.save_type_state_add(session_id, type, new_data, tracker_snap)
        
    def __on_transactions_message(self, body):
        """Maneja mensajes de conteos de transacciones de group_by_query4."""        
        
        header, rows = deserialize_message(body)
        session_id = header.get("session_id", "unknown")
        batch_id = int(header.get("batch_id"))
        is_eos = header.get("is_eos") == "true"
        
        if is_eos:
            logger.info(f"Se recibió EOS en transactions para sesión {session_id}, batch_id: {batch_id}. Marcando como listo...")
        
        if rows:
            self.__accumulate_transactions(rows, session_id)
        
        if self.session_tracker.update(session_id, "transactions", batch_id, is_eos):
            self.__generate_and_send_results(session_id)
            
            self.__del_session(session_id)
        else: 
            self.__save_session_type(session_id, "transactions")

    def __on_stores_message(self, body):
        """Maneja mensajes de stores para el JOIN."""
        
        header, rows = deserialize_message(body)        
        session_id = header.get("session_id", "unknown")
        batch_id = int(header.get("batch_id"))
        is_eos = header.get("is_eos") == "true"
        
        if is_eos:
            logger.info(f"Se recibió EOS en stores para sesión {session_id}, batch_id: {batch_id}. Marcando como listo...")
            
        if rows:
            self.__load_stores(rows, session_id)
        
        if self.session_tracker.update(session_id, "stores", batch_id, is_eos):
            self.__generate_and_send_results(session_id)
            
            self.__del_session(session_id)
        else:
            self.__save_session_type(session_id, "stores")
    
    def __on_users_message(self, body):
        """Maneja mensajes de users para el JOIN."""
        
        try:
            header, rows = deserialize_message(body)       
            session_id = header.get("session_id", "unknown")
            batch_id = int(header.get("batch_id"))
            is_eos = header.get("is_eos") == "true"
            new_users = {}
            
            if is_eos: logger.info(f"Se recibió EOS en users para sesión {session_id}, batch_id: {batch_id}. Marcando como listo...")

            if rows:
                new_users = self.__load_users(rows, session_id)
            
            if self.session_tracker.update(session_id, "users", batch_id, is_eos):
                self.__generate_and_send_results(session_id)
                
                self.__del_session(session_id)
                
            else:
                self.__save_session_type_add(session_id, "users", new_users)
                pass 
            
        except Exception as e: 
            logger.error(f"[AggregatorQuery4] Error procesando el mensaje de users: {e}")
            logger.error(traceback.format_exc())
            
    def __consume_transactions(self):
        try:
            self.transactions_queue = MessageMiddlewareQueue(RABBIT_HOST, "group_by_q4")
            self.transactions_queue.start_consuming(self.__on_transactions_message)
        except Exception as e:
            if not self.shutdown_event.is_set():
                logger.error(f"[AggregatorQuery4] Error en consumo de transacciones: {e}")
            else: 
                self.transactions_queue.close()

    def __consume_stores(self):
        try:
            self.stores_exchange = MessageMiddlewareExchange(RABBIT_HOST, STORES_EXCHANGE, [STORES_ROUTING_KEY])
            self.stores_exchange.start_consuming(self.__on_stores_message)
        except Exception as e:
            if not self.shutdown_event.is_set():
                logger.error(f"[AggregatorQuery4] Error en consumo de stores: {e}")
            else:
                self.stores_exchange.close()
            
    def __consume_users(self):
        try:
            self.users_queue = MessageMiddlewareQueue(RABBIT_HOST, USERS_QUEUE)
            self.users_queue.start_consuming(self.__on_users_message)
        except Exception as e:
            if not self.shutdown_event.is_set():
                logger.error(f"[AggregatorQuery4] Error en consumo de users: {e}")
            else: 
                self.users_queue.close()

    def __init_healthcheck(self):
        "Iniciar servidor de healthcheck UDP"
        
        healthcheck_port = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
        start_healthcheck_server(port=healthcheck_port, node_name="aggregator_query4", shutdown_event=self.shutdown_event)
        
        logger.info(f"[AggregatorQuery4] Healthcheck server iniciado en puerto UDP {healthcheck_port}")
        
    def __signal_handler(self,signum, frame):
        logger.warning(f"[AggregatorQuery4] Señal {signum} recibida, cerrando...")
        self.shutdown_event.set()
        
        for mq in [self.users_queue, self.stores_exchange, self.transactions_queue]:
            try:
                mq.stop_consuming()
            except Exception as e: 
                logger.error(f"Error al parar el consumo: {e}")
    
    def __init_signal_handler(self):
        signal.signal(signal.SIGTERM, self.__signal_handler)
        signal.signal(signal.SIGINT, self.__signal_handler)
    
    def __load_sessions_data(self, data: dict): 
        for session_id, types_data in data.items():
            self.__initialize_session(session_id)
            
            if 'stores' in types_data:
                self.session_data[session_id]['stores'] = types_data['stores']
                        
            if 'transactions' in types_data:
                self.session_data[session_id]['transactions'] = types_data['transactions'] 
            
            if 'users' in types_data:
                self.session_data[session_id]['users'] = types_data['users'] 
            
     
     
    def __load_sessions(self):
        logger.info("[*] Intentando recuperar estado previo...")
        saved_data, saved_tracker = self.state_manager.load_all_sessions()

        if saved_data and saved_tracker:
            self.__load_sessions_data(saved_data)
            self.session_tracker.load_state_snapshot(saved_tracker)
            
            logger.info(f"[*] Estado recuperado. Sesiones activas: {len(self.session_data)}")
        else:
            logger.info("[*] No se encontró estado previo o estaba corrupto. Iniciando desde cero.")
    
    
    def start(self):
    
        self.__init_healthcheck()
        self.__init_signal_handler()        
        
        self.__load_sessions()
        
        logger.info("[*] AggregatorQuery4 esperando mensajes...")
        logger.info("[*] Query 4: TOP 3 clientes por sucursal (más compras 2024-2025)")
        logger.info("[*] Consumiendo de 3 fuentes: transacciones + stores + users para doble JOIN")
         
        self.__run()
        
        logger.info("[x] AggregatorQuery4 detenido")

    def __run(self):
        transactions_thread = threading.Thread(target=self.__consume_transactions)
        stores_thread = threading.Thread(target=self.__consume_stores)
        users_thread = threading.Thread(target=self.__consume_users)
            
        transactions_thread.start()
        stores_thread.start()
        users_thread.start()
            
        logger.info("[AggregatorQuery4] Worker iniciado, esperando mensajes de múltiples sesiones...")
        logger.info("[AggregatorQuery4] El worker continuará procesando múltiples clientes")
            
        transactions_thread.join()
        stores_thread.join()
        users_thread.join()
        
if __name__ == "__main__":    
    logger = init_log("AggregatorQuery4")
    
    aggregator = AggregatorQuery4()
    aggregator.start()
