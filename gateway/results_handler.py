"""
Results Handler - Maneja la recepción de resultados finales
Este módulo se ejecuta en el gateway para recibir resultados de los aggregators
"""

import sys
import os
import threading
from collections import defaultdict

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from middleware.middleware import MessageMiddlewareExchange
from workers.utils import deserialize_message

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
    def __init__(self, output_dir):
        self.output_dir = output_dir
        # Ahora results es un diccionario anidado: {session_id: {query_name: [rows]}}
        self.results = defaultdict(lambda: defaultdict(list))
        # Lock para proteger acceso concurrente a self.results
        self.lock = threading.Lock()
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"[ResultsHandler] Directorio: {self.output_dir}")
    
    def collect_result(self, query_name, rows, header):
        session_id = header.get('session_id', 'default')
        
        # Reducir verbosidad: mostrar solo conteo y batch
        print(f"[ResultsHandler] {query_name} (sesión {session_id}): filas={len(rows)} batch={header.get('batch_number', '?')}/{header.get('total_batches', '?')}")
        
        if header.get('is_final_result') == 'true':
            batch_num = header.get('batch_number', '?')
            total_batches = header.get('total_batches', '?')
            
            # Proteger acceso concurrente a self.results
            with self.lock:
                # Acumular resultados por sesión y query
                self.results[session_id][query_name].extend(rows)
                current_count = len(self.results[session_id][query_name])
            
            print(f"[ResultsHandler] {query_name} (sesión {session_id}): Batch {batch_num}/{total_batches} acumulado={current_count}")
            
            if batch_num == total_batches:
                print(f"[ResultsHandler] Último batch recibido para {query_name} (sesión {session_id}), guardando resultados...")
                self.save_results(query_name, header, session_id)
        else:
            print(f"[ResultsHandler] Mensaje ignorado - is_final_result={header.get('is_final_result')}")
    
    def save_results(self, query_name, header, session_id):
        # Proteger lectura de results con lock
        with self.lock:
            results_list = self.results[session_id][query_name].copy()
        
        if not results_list:
            return
        
        # Crear directorio por sesión para evitar sobrescritura en ejecuciones concurrentes
        session_dir = os.path.join(self.output_dir, f"session_{session_id}")
        os.makedirs(session_dir, exist_ok=True)
        
        # Archivo en directorio de la sesión
        output_file = os.path.join(session_dir, f"{query_name}.csv")
        
        columns = list(results_list[0].keys())
        
        # Guardar SOLO en directorio de sesión (no sobrescribir archivos generales)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(','.join(columns) + '\n')
            for row in results_list:
                values = [str(row.get(col, '')) for col in columns]
                f.write(','.join(values) + '\n')
        
        print(f"\n{'='*60}")
        print(f"[ResultsHandler] {query_name} COMPLETADO (sesión {session_id})")
        print(f"[ResultsHandler] Resultados de {query_name} guardados en {output_file}")
        print(f"{'='*60}\n")

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
