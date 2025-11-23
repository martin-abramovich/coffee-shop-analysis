import logging
import sys
import os
import signal
import threading
from datetime import datetime

from workers.session_tracker import SessionTracker
from workers.aggregator.sesion_state_manager import SessionStateManager
# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from common.healthcheck import start_healthcheck_server


from middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

INPUT_EXCHANGE = "transactions_amount"    # exchange del filtro por amount
INPUT_ROUTING_KEY = "amount"              # routing key del filtro por amount
OUTPUT_EXCHANGE = "results_query1"        # exchange de salida para resultados finales
ROUTING_KEY = "query1_results"            # routing para resultados


class AggregatorQuery1:
    def __init__(self):
        # Acumulador de transacciones válidas por sesión
        self.session_data = {}  # {session_id: {'transactions': [], 'total_received': 0, 'results_sent': False}}
        self.total_received = 0
        self.session_tracker = SessionTracker(["transactions"])
        self.state_manager = SessionStateManager()
        
        self.shutdown_event = threading.Event()
        self.amount_trans_queue = None
        self.results_queue = None
     
        
    def __accumulate_transactions(self, rows, session_id):
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
            logger.debug(f"[AggregatorQuery1] Total acumulado: {total_accumulated}/{self.total_received} (sesiones: {len(self.session_data)})")
    
    
    def __generate_final_results(self, session_id):
        """Genera los resultados finales para Query 1 de una sesión específica."""
        
        logger.info(f"[AggregatorQuery1] Generando resultados finales para sesión {session_id}...")
        
        if session_id not in self.session_data:
            logger.warning(f"[AggregatorQuery1] No hay datos para sesión {session_id}")
            return []
        
        session_info = self.session_data[session_id]
        results = session_info['transactions']
        
        if not results:
            logger.warning(f"[AggregatorQuery1] No hay transacciones válidas para reportar en sesión {session_id}")
            return []
        
        # Ordenar por transaction_id para consistencia
        results.sort(key=lambda x: x['transaction_id'])
        
        logger.info(f"[AggregatorQuery1] Resultados generados para sesión {session_id}: {len(results)} transacciones")
            
        return results
    
    def __send_results(self, session_id):
        final_results = self.__generate_final_results(session_id)

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
            batch_size = 1000
            total_batches = (len(final_results) + batch_size - 1) // batch_size
                
            for i in range(0, len(final_results), batch_size):
                batch = final_results[i:i + batch_size]
                batch_header = results_header.copy()
                batch_header["batch_number"] = str((i // batch_size) + 1)
                batch_header["total_batches"] = str(total_batches)
                    
                result_msg = serialize_message(batch, batch_header)
                    
                self.results_queue.send(result_msg)
            
        logger.info(f"[AggregatorQuery1] Resultados finales enviados para sesión {session_id}.")
    
    def __signal_handler(self, signum, frame):
        logger.info(f"[AggregatorQuery1] Señal {signum} recibida, cerrando...")
        self.shutdown_event.set()
        try:
            self.amount_trans_queue.stop_consuming()
        except Exception as e: 
            logger.error(f"Error al parar el consumo: {e}")
    
    
    def __init_signal_handler(self):
        signal.signal(signal.SIGTERM, self.__signal_handler)
        signal.signal(signal.SIGINT, self.__signal_handler)
    
        
    def __init_middleware(self):
        self.amount_trans_queue = MessageMiddlewareQueue(RABBIT_HOST, "transactions_amount")        
        self.results_queue = MessageMiddlewareExchange(RABBIT_HOST, 'results_query1', ['query1_results'])
    
        
    def __init_healthcheck(self):
        "Inicia servidor de healthcheck UDP"
        
        healthcheck_port = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
        start_healthcheck_server(port=healthcheck_port, node_name="aggregator_query1", shutdown_event=self.shutdown_event)
        logger.info(f"[AggregatorQuery1] Healthcheck server iniciado en puerto UDP {healthcheck_port}")
    
    def __del_session(self, session_id):
        #data en memoria
        if session_id in self.session_data:
            del self.session_data[session_id]
        
        #data en disco
        self.state_manager.delete_session(session_id)
    
    def __save_session(self, session_id):
        tracker_snap = self.session_tracker.get_single_session_type_snapshot(session_id, "transactions")
        session_snap = self.session_data.get(session_id, {})
        
        # Escribimos session_{id}.pkl
        self.state_manager.save_type_state(session_id, "transactions", session_snap, tracker_snap)
        
        
        
    def __on_message(self, body):
        header, rows = deserialize_message(body)

        session_id = header.get("session_id", "unknown")
        batch_id = int(header.get("batch_id"))
        is_eos = str(header.get("is_eos", "")).lower() == "true"

        if self.session_tracker.previus_update(session_id, "transactions", batch_id):
            return

        if rows:
            self.__accumulate_transactions(rows, session_id)
       
        if self.session_tracker.update(session_id, "transactions",batch_id, is_eos): 
                self.__send_results(session_id)
                
                self.__del_session(session_id)
          
        else: 
            # Obtenemos snapshot SOLO de esta sesión
            self.__save_session(session_id)

    def __load_sessions_data(self, data):
        for session_id, data in data.items():
            self.session_data[session_id] = data["transactions"]
    
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
        self.__init_middleware()
        
        self.__load_sessions()
            
        logger.info("[*] AggregatorQuery1 esperando mensajes...")
        logger.info("[*] Query 1: Transacciones 2024-2025, 06:00-23:00, monto >= 75")
        logger.info("[*] Columnas output: transaction_id, final_amount")
        
        try:
            self.amount_trans_queue.start_consuming(self.__on_message)    
        except KeyboardInterrupt:
            logger.info("\n[AggregatorQuery1] Interrupción recibida")
            self.shutdown_event.set()
        finally: 
            self.__close_middleware()        
            logger.info("[x] AggregatorQuery1 detenido")



    def __close_middleware(self):
        "Cerrar conexiones"
        
        for mq in [self.amount_trans_queue, self.results_queue]:
            try:
                mq.close()
            except Exception as e:
                logger.error(f"Error al cerrar conexión: {e}")


def init_log():
    logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

if __name__ == "__main__":
    init_log()
    logger = logging.getLogger(__name__)
    
    aggregator = AggregatorQuery1()
    aggregator.start()
    
    