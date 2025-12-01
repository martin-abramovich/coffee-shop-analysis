"""
Results Handler - Maneja la recepción de resultados finales
Este módulo se ejecuta en el gateway para recibir resultados de los aggregators
"""

import sys
import os
import threading
from collections import defaultdict
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from middleware.middleware import MessageMiddlewareExchange
from workers.utils import deserialize_message
from gateway.result_dispatcher import result_dispatcher
from workers.session_tracker import SessionTracker
from workers.aggregator.sesion_state_manager import SessionStateManager, merge_list

logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
OUTPUT_DIR = './results'

# Exchanges de resultados
RESULT_EXCHANGES = {
    'query1': ('results_query1', 'query1_results'),
    'query2': ('results_query2', 'query2_results'),
    'query3': ('results_query3', 'query3_results'),
    'query4': ('results_query4', 'query4_results')
}

# Configuración del State Manager para persistencia de resultados
DEFAULT_DATA_CONFIGS_STATE_MANAGER = {
    "query1": (list, merge_list),
    "query2": (list, merge_list),
    "query3": (list, merge_list),
    "query4": (list, merge_list),
}

class ResultsHandler:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        # Ahora results es un diccionario anidado: {session_id: {query_name: [rows]}}
        self.results = defaultdict(lambda: defaultdict(list))
        # Lock para proteger acceso concurrente a self.results
        self.lock = threading.Lock()
        
        # Session Tracker separado para cada query (cada query es independiente)
        # Usamos un tracker por query-session en lugar de un tracker global
        self.session_trackers = {}  # {(session_id, query_name): SessionTracker}
        self.trackers_lock = threading.Lock()
        
        self.state_manager = SessionStateManager(DEFAULT_DATA_CONFIGS_STATE_MANAGER, base_dir="./state_results")
        
        print(f"[ResultsHandler] Directorio (deprecated): {self.output_dir}")
        
        # Cargar sesiones previas
        self.__load_sessions()
    
    def __get_tracker(self, session_id, query_name):
        """Obtiene o crea un tracker para una combinación session_id + query_name"""
        key = (session_id, query_name)
        with self.trackers_lock:
            if key not in self.session_trackers:
                # Cada query tiene su propio tracker independiente
                self.session_trackers[key] = SessionTracker([query_name])
            return self.session_trackers[key]
    
    def collect_result(self, query_name, rows, header):
        session_id = header.get('session_id', 'default')
        batch_num = int(header.get('batch_number', 1))
        total_batches = int(header.get('total_batches', 1))
        
        # Convertir batch_number (base-1) a batch_id (base-0) para el tracker
        batch_id = batch_num - 1
        
        # Reducir verbosidad: mostrar solo conteo y batch
        print(f"[ResultsHandler] {query_name} (sesión {session_id}): filas={len(rows)} batch={batch_num}/{total_batches}")
        
        # Obtener el tracker específico para esta query en esta sesión
        tracker = self.__get_tracker(session_id, query_name)
        
        # Verificar si ya procesamos este batch
        if tracker.previus_update(session_id, query_name, batch_id):
            print(f"[ResultsHandler] Batch duplicado ignorado: {query_name} (sesión {session_id}) batch={batch_num}")
            return
        
        if header.get('is_final_result') == 'true':
            # Proteger acceso concurrente a self.results
            with self.lock:
                # Inicializar sesión si no existe
                if session_id not in self.results:
                    self.results[session_id] = defaultdict(list)
                
                # Acumular resultados por sesión y query
                self.results[session_id][query_name].extend(rows)
                current_count = len(self.results[session_id][query_name])
            
            print(f"[ResultsHandler] {query_name} (sesión {session_id}): Batch {batch_num}/{total_batches} acumulado={current_count}")
            
            # Determinar si es el último batch (EOS para este query)
            is_eos = (batch_num == total_batches)
            
            # Actualizar el tracker y verificar si esta query está completa
            if tracker.update(session_id, query_name, batch_id, is_eos):
                # Esta query específica está completa - despachar y limpiar
                print(f"[ResultsHandler] Último batch recibido para {query_name} (sesión {session_id}), despachando resultados al cliente...")
                self.dispatch_results(query_name, header, session_id)
                self.__del_session_query(session_id, query_name)
            else:
                # Guardar estado incremental
                self.__save_session_add(session_id, query_name, rows, tracker)
        else:
            print(f"[ResultsHandler] Mensaje ignorado - is_final_result={header.get('is_final_result')}")
    
    def dispatch_results(self, query_name, header, session_id):
        # Proteger lectura de results con lock
        with self.lock:
            session_bucket = self.results.get(session_id, {})
            query_rows = session_bucket.get(query_name, [])
            results_list = list(query_rows)
        
        if not results_list:
            return
        
        columns = self._resolve_columns(header, results_list)
        metadata = {
            'batch_number': header.get('batch_number'),
            'total_batches': header.get('total_batches'),
            'description': header.get('description'),
            'total_results': header.get('total_results'),
            'stream_id': header.get('stream_id'),
        }

        payload = {
            'query': query_name,
            'columns': columns,
            'rows': results_list,
            'metadata': metadata,
        }

        result_dispatcher.submit_result(session_id, query_name, payload)

        print(f"\n{'='*60}")
        print(f"[ResultsHandler] {query_name} COMPLETADO (sesión {session_id})")
        print(f"[ResultsHandler] Resultados enviados al cliente (total filas: {len(results_list)})")
        print(f"{'='*60}\n")

    def __save_session_add(self, session_id, query_name, rows, tracker):
        """Guarda estado incremental de resultados parciales"""
        tracker_snap = tracker.get_single_session_type_snapshot(session_id, query_name)
        self.state_manager.save_type_state_add(session_id, query_name, rows, tracker_snap)
    
    def __del_session_query(self, session_id, query_name):
        """Elimina una query específica de una sesión de memoria y disco"""
        # Limpiar de memoria
        with self.lock:
            if session_id in self.results and query_name in self.results[session_id]:
                del self.results[session_id][query_name]
                
                # Si no quedan queries en esta sesión, eliminar la sesión completa
                if not self.results[session_id]:
                    del self.results[session_id]
        
        # Limpiar tracker
        key = (session_id, query_name)
        with self.trackers_lock:
            if key in self.session_trackers:
                del self.session_trackers[key]
        
        # Verificar si debemos borrar toda la sesión del disco
        # Solo borrar si no hay más queries activas para esta sesión
        with self.lock:
            session_has_data = session_id in self.results and len(self.results[session_id]) > 0
        
        if not session_has_data:
            # No hay más queries activas, borrar del disco
            self.state_manager.delete_session(session_id)
    
    def __load_sessions(self):
        """Carga sesiones previas desde disco al iniciar"""
        logger.info("[ResultsHandler] Intentando recuperar estado previo...")
        saved_data, saved_tracker = self.state_manager.load_all_sessions()
        
        if saved_data and saved_tracker:
            # Reconstruir self.results y trackers desde el estado guardado
            with self.lock:
                for session_id, queries_data in saved_data.items():
                    if session_id not in self.results:
                        self.results[session_id] = defaultdict(list)
                    
                    for query_name, rows in queries_data.items():
                        self.results[session_id][query_name] = rows
                        
                        # Reconstruir tracker para esta query
                        if session_id in saved_tracker and query_name in saved_tracker[session_id]:
                            key = (session_id, query_name)
                            with self.trackers_lock:
                                tracker = SessionTracker([query_name])
                                # Cargar solo el snapshot de esta query específica
                                query_tracker_data = {session_id: saved_tracker[session_id]}
                                tracker.load_state_snapshot(query_tracker_data)
                                self.session_trackers[key] = tracker
            
            logger.info(f"[ResultsHandler] Estado recuperado. Sesiones activas: {len(self.results)}")
        else:
            logger.info("[ResultsHandler] No se encontró estado previo o estaba corrupto. Iniciando desde cero.")
    
    def _resolve_columns(self, header, results_list):
        if not results_list:
            return []

        header_columns = header.get('columns')
        if header_columns:
            separator = ',' if ',' in header_columns else ':'
            cols = [col.strip() for col in header_columns.split(separator) if col.strip()]
            if cols:
                return cols

        # Fallback: usar las claves del primer registro preservando orden de inserción
        return list(results_list[0].keys())

def start_results_handler():
    """Inicia el handler de resultados en threads separados"""
    handler = ResultsHandler(OUTPUT_DIR)
    mq_connections = {}
    
    try:
        for query_name, (exchange, routing_key) in RESULT_EXCHANGES.items():
            print(f"[ResultsHandler] Conectando a exchange: {exchange} con routing_key: {routing_key} para {query_name}")
            mq = MessageMiddlewareExchange(RABBITMQ_HOST, exchange, [routing_key])
            mq_connections[query_name] = mq
        
        print("[ResultsHandler] Esperando resultados de:", list(RESULT_EXCHANGES.keys()))
        
        def create_consumer(query_name, mq):
            def on_message(body):
                try:
                    header, rows = deserialize_message(body)
                    handler.collect_result(query_name, rows, header)
                except Exception as e:
                    print(f"[ResultsHandler] Error en {query_name}: {e}")
            
            def consume():
                try:
                    mq.start_consuming(on_message)
                except Exception as e:
                    print(f"[ResultsHandler] Error consumiendo {query_name}: {e}")
            
            return consume
        
        threads = []
        for query_name, mq in mq_connections.items():
            consumer = create_consumer(query_name, mq)
            thread = threading.Thread(target=consumer, name=f"Results-{query_name}", daemon=True)
            thread.start()
            threads.append(thread)
        
        # Los threads corren como daemon, no bloqueamos aquí
        print("[ResultsHandler] Threads de resultados iniciados")
        
    except Exception as e:
        print(f"[ResultsHandler] Error: {e}")
        for mq in mq_connections.values():
            try:
                mq.close()
            except:
                pass
