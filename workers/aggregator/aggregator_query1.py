import sys
import os
import signal
import threading
from datetime import datetime

from workers.session_tracker import SessionTracker

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from common.healthcheck import start_healthcheck_server

def log_with_timestamp(message):
    """Función para logging con timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{timestamp}] {message}")

from middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')

INPUT_EXCHANGE = "transactions_amount"    # exchange del filtro por amount
INPUT_ROUTING_KEY = "amount"              # routing key del filtro por amount
OUTPUT_EXCHANGE = "results_query1"        # exchange de salida para resultados finales
ROUTING_KEY = "query1_results"            # routing para resultados


class AggregatorQuery1:
    def __init__(self):
        # Acumulador de transacciones válidas por sesión
        self.session_data = {}  # {session_id: {'transactions': [], 'total_received': 0, 'results_sent': False}}
        self.total_received = 0
        
    def accumulate_transactions(self, rows, session_id):
        """Acumula transacciones que pasaron todos los filtros para una sesión específica."""
        if session_id not in self.session_data:
            self.session_data[session_id] = {'transactions': [], 'total_received': 0, 'results_sent': False}
        
        session_info = self.session_data[session_id]
        
        for row in rows:
            # Extraer los campos requeridos para Query 1: transaction_id y final_amount
            transaction_record = {
                'transaction_id': row.get('transaction_id'),
                'final_amount': row.get('final_amount')
            }
            
            if transaction_record.get('transaction_id') and transaction_record.get('final_amount') is not None:
                session_info['transactions'].append(transaction_record)
        
        session_info['total_received'] += len(rows)
        self.total_received += len(rows)
        
        # Log solo cada 1000 transacciones recibidas
        if self.total_received % 10000 < len(rows):
            total_accumulated = sum(len(data['transactions']) for data in self.session_data.values())
            print(f"[AggregatorQuery1] Total acumulado: {total_accumulated}/{self.total_received} (sesiones: {len(self.session_data)})")
    
    def generate_final_results(self, session_id):
        """Genera los resultados finales para Query 1 de una sesión específica."""
        print(f"[AggregatorQuery1] Generando resultados finales para sesión {session_id}...")
        
        if session_id not in self.session_data:
            print(f"[AggregatorQuery1] No hay datos para sesión {session_id}")
            return []
        
        session_info = self.session_data[session_id]
        accumulated_transactions = session_info['transactions']
        
        if not accumulated_transactions:
            print(f"[AggregatorQuery1] No hay transacciones válidas para reportar en sesión {session_id}")
            return []
        
        # Para Query 1, retornamos todas las transacciones con transaction_id y final_amount
        results = []
        for txn in accumulated_transactions:
            # Asegurar tipos de datos correctos
            try:
                result = {
                    'transaction_id': str(txn['transaction_id']),
                    'final_amount': float(txn['final_amount']) if isinstance(txn['final_amount'], str) else txn['final_amount']
                }
                results.append(result)
            except (ValueError, TypeError) as e:
                print(f"[AggregatorQuery1] Error procesando transacción {txn}: {e}")
                continue
        
        # Ordenar por transaction_id para consistencia
        results.sort(key=lambda x: x['transaction_id'])
        
        print(f"[AggregatorQuery1] Resultados generados para sesión {session_id}: {len(results)} transacciones")

        # Estadísticas
        total_amount = sum(r['final_amount'] for r in results)
        avg_amount = total_amount / len(results) if results else 0
        print(f"[AggregatorQuery1] Monto total: ${total_amount:,.2f}, Promedio: ${avg_amount:.2f}")
            
        return results

aggregator = AggregatorQuery1()
session_tracker = SessionTracker(["transactions"])
count = 0

def on_message(body):
    global aggregator
    global session_tracker
    global count
    header, rows = deserialize_message(body)
    session_id = header.get("session_id", "unknown")
    batch_id = int(header.get("batch_id"))
    is_eos = str(header.get("is_eos", "")).lower() == "true"
    
    count += 1
    if is_eos: 
        print(f"SE RECIBIO EOS: {batch_id} && {count}")
    
    if batch_id is None:
        print(f"[AggregatorQuery1] batch_id faltante en sesión {session_id}, mensaje ignorado")
        return

    if rows:
        aggregator.accumulate_transactions(rows, session_id)
    
        
    if session_tracker.update(session_id, "transactions",batch_id, is_eos): 
        
        final_results = aggregator.generate_final_results(session_id)
    
        if final_results:
            # Enviar resultados finales
            results_header = {
                "type": "result",
                "stream_id": f"query1_results_{session_id}",
                "batch_id": f"final_{session_id}",
                "is_batch_end": "true",
                "is_eos": "false",
                "query": "query1",
                "total_results": str(len(final_results)),
                "description": "Transacciones_2024-2025_06:00-23:00_monto>=75",
                "columns": "transaction_id:final_amount",
                "is_final_result": "true",
                "session_id": session_id
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
                
                results_queue.send(result_msg)
        
        print(f"[AggregatorQuery1] Resultados finales enviados para sesión {session_id}.")
        
        if session_id in aggregator.session_data:
            del aggregator.session_data[session_id]
            
if __name__ == "__main__":
    import threading
    
    shutdown_event = threading.Event()
    
    # Iniciar servidor de healthcheck UDP
    healthcheck_port = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
    start_healthcheck_server(port=healthcheck_port, node_name="aggregator_query1", shutdown_event=shutdown_event)
    print(f"[AggregatorQuery1] Healthcheck server iniciado en puerto UDP {healthcheck_port}")
    
    def signal_handler(signum, frame):
        print(f"[AggregatorQuery1] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
        try:
            amount_trans_queue.stop_consuming()
        except Exception as e: 
            print(f"Error al parar el consumo: {e}")
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    amount_trans_queue = MessageMiddlewareQueue(RABBIT_HOST, "transactions_amount")
    
    results_queue = MessageMiddlewareExchange(RABBIT_HOST, 'results_query1', ['query1_results'])
    
    print("[*] AggregatorQuery1 esperando mensajes...")
    print("[*] Query 1: Transacciones 2024-2025, 06:00-23:00, monto >= 75")
    print("[*] Columnas output: transaction_id, final_amount")
    print("[*] Esperará hasta recibir EOS para generar reporte")
    
    try:
        amount_trans_queue.start_consuming(on_message)    
    except KeyboardInterrupt:
        print("\n[AggregatorQuery1] Interrupción recibida")
        shutdown_event.set()
    finally:
        
        # Cerrar conexiones
        for mq in [amount_trans_queue, results_queue]:
            try:
                mq.close()
            except Exception as e:
                print(f"Error al cerrar conexión: {e}")
                
        print("[x] AggregatorQuery1 detenido")