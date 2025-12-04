"""
Results Handler - Maneja la recepción de resultados finales
Este módulo se ejecuta en el gateway para recibir resultados de los aggregators
"""

import sys
import os
import threading
from collections import defaultdict
import traceback

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from middleware.middleware import MessageMiddlewareExchange
from workers.utils import deserialize_message
from gateway.result_dispatcher import result_dispatcher

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
OUTPUT_DIR = './results'

# Exchanges de resultados
RESULT_EXCHANGES = {
    'query1': ('results_query1', 'query1_results'),
    'query2': ('results_query2', 'query2_results'),
    'query3': ('results_query3', 'query3_results'),
    'query4': ('results_query4', 'query4_results')
}

class ResultsHandler:
    def __init__(self):
        
        # Ahora results es un diccionario anidado: {session_id: {query_name: [rows]}}
        self.results = defaultdict(lambda: defaultdict(list))
        # Lock para proteger acceso concurrente a self.results
        self.lock = threading.Lock()
        
    
    def collect_result(self, query_name, rows, header):
        try:
            session_id = header.get('session_id', 'default')
            
            # Reducir verbosidad: mostrar solo conteo y batch
            print(f"[ResultsHandler] {query_name} (sesión {session_id}): filas={len(rows)}")
            
            is_eos = header.get('is_eos')
            
            # Proteger acceso concurrente a self.results
            with self.lock:
                # Acumular resultados por sesión y query
                self.results[session_id][query_name].extend(rows)
                current_count = len(self.results[session_id][query_name])
            
            
            if is_eos:
                print(f"[ResultsHandler] Último batch recibido para {query_name} (sesión {session_id})")
                self.dispatch_results(query_name, header, session_id)
        except Exception as e:
            print(f"[ResultsHandler] Error al recolectar resultado para {query_name}: {e}")
            print(traceback.format_exc())
    
    def dispatch_results(self, query_name, header, session_id):
        # Proteger lectura de results con lock
        with self.lock:
            session_bucket = self.results.get(session_id, {})
            query_rows = session_bucket.get(query_name, [])
            results_list = list(query_rows)
            session_bucket.pop(query_name, None)
            if not session_bucket and session_id in self.results:
                del self.results[session_id]
        
        if not results_list:
            return
        
        columns = self._resolve_columns(header, results_list)
       

        payload = {
            'columns': columns,
            'rows': results_list,
        }

        result_dispatcher.submit_result(session_id, query_name, payload)

        print(f"\n{'='*60}")
        print(f"[ResultsHandler] {query_name} COMPLETADO (sesión {session_id})")
        print(f"[ResultsHandler] Resultados enviados al cliente (total filas: {len(results_list)})")
        print(f"{'='*60}\n")

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

def start_results_handler(shutdown_event):
    """Inicia el handler de resultados en threads separados"""
    handler = ResultsHandler()
    mq_connections = {}
    
    try:
        for query_name, (exchange, routing_key) in RESULT_EXCHANGES.items():
            mq = MessageMiddlewareExchange(RABBITMQ_HOST, exchange, [routing_key])
            mq_connections[query_name] = mq
        
        
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
                    if not shutdown_event.is_set():
                        print(f"[ResultsHandler] Error consumiendo {query_name}: {e}")
            
            return consume
        
        threads = []
        for query_name, mq in mq_connections.items():
            consumer = create_consumer(query_name, mq)
            thread = threading.Thread(target=consumer, name=f"Results-{query_name}", daemon=False)
            thread.start()
            threads.append(thread)
        
        print("[ResultsHandler] Threads de resultados iniciados")
        
        # Retornar las conexiones para que main.py pueda cerrarlas
        return mq_connections
        
    except Exception as e:
        print(f"[ResultsHandler] Error: {e}")
        for mq in mq_connections.values():
            try:
                mq.close()
            except:
                pass
        return None
