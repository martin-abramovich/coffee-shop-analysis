import sys
import os
import signal
import threading
import time
from collections import defaultdict
import traceback

from common.logger import init_log
from common.utils import yyyymm_int_to_str
from workers.aggregator.delete_thread import delete_sessions_thread
from workers.aggregator.sesion_state_manager import SessionStateManager
from workers.session_tracker import SessionTracker

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from workers.utils import deserialize_message, serialize_message
from common.healthcheck import start_healthcheck_server

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')

OUTPUT_QUEUE = "results_query2"        # exchange de salida para resultados finales
GROUP_BY_Q2_QUEUE = "group_by_q2"

# Exchanges adicionales para JOIN
MENU_ITEMS_QUEUE = "menu_items_raw"

INDEX_METRICS = {
    "total_quantity": 0 ,
    "total_subtotal": 1,   
}

class AggregatorQuery2:
    def __init__(self):
        # Datos por sesión: {session_id: session_data}
        self.session_data = {}
        self.session_data_lock = threading.Lock()
        
        self.shutdown_event = threading.Event()
        self.session_tracker = SessionTracker(["metrics", "menu_items"])
        self.state_manager = SessionStateManager(logger=logger)
        self.finish_sessions = set()
        
        #in
        self.mq_metrics = None         
        self.mq_menu_items = None 
        
    
    def __initialize_session(self, session_id):
        """Inicializa datos para una nueva sesión"""
        with self.session_data_lock:
            if session_id not in self.session_data:
                self.session_data[session_id] = {
                    'metrics': {},
                    'menu_items': {},
                }
    
    def __get_session_data(self, session_id):
        """Obtiene los datos de una sesión específica"""
        self.__initialize_session(session_id)
        return self.session_data[session_id]
        
    def load_menu_items(self, rows, session_id):
        """Carga menu_items para construir el diccionario item_id -> item_name para una sesión específica."""
        session_data = self.__get_session_data(session_id)
        
        for row in rows:
            item_id = int(row.get('item_id'))
            item_name = row.get('item_name')
            
            if item_id and item_name:
                session_data['menu_items'][item_id] = item_name.strip()
        
        logger.info(f"[AggregatorQuery2] Sesión {session_id}: Cargados {len(session_data['menu_items'])} menu items para JOIN")
    
    def accumulate_metrics(self, rows, session_id):
        """Acumula métricas parciales de group_by_query2 para una sesión específica."""
        session_data = self.__get_session_data(session_id)
        
        for row in rows:
            month = int(row.get('month'))
            item_id = int(row.get('item_id'))
            total_quantity = row.get('total_quantity', 0)
            total_subtotal = row.get('total_subtotal', 0.0)
            
            # Validar campos requeridos
            if not month or not item_id:
                continue
                
            # Convertir a tipos correctos
            try:
                if isinstance(total_quantity, str):
                    total_quantity = int(total_quantity)
                if isinstance(total_subtotal, str):
                    total_subtotal = float(total_subtotal)
            except (ValueError, TypeError):
                continue
            
            # Clave compuesta: (mes, item_id)
            key = (month, item_id)
            
            if key not in session_data['metrics']:
                session_data['metrics'][key] = [0, 0.0]
                
            # Acumular métricas para esta sesión
            idx = INDEX_METRICS
            session_data['metrics'][key][idx["total_quantity"]] += total_quantity
            session_data['metrics'][key][idx["total_subtotal"]] += total_subtotal
            
        
    def generate_final_results(self, session_id):
        """Genera los resultados finales para Query 2 con JOIN para una sesión específica."""
        session_data = self.__get_session_data(session_id)
        
        logger.info(f"[AggregatorQuery2] Generando resultados finales para sesión {session_id}...")
        logger.info(f"[AggregatorQuery2] Total combinaciones procesadas: {len(session_data['metrics'])}")
        logger.info(f"[AggregatorQuery2] Menu items disponibles para JOIN: {len(session_data['menu_items'])}")
        
        if not session_data['metrics']:
            logger.warning(f"[AggregatorQuery2] No hay datos para procesar en sesión {session_id}")
            return []
        
        # Si no hay menu_items para esta sesión, haremos un fallback usando item_id como nombre
        if not session_data['menu_items']:
            logger.warning(f"[AggregatorQuery2] WARNING: No hay menu_items cargados para sesión {session_id}. Se usará item_id como item_name (fallback)")
        
        # Preparar resultados por mes con JOIN
        results_by_month = defaultdict(list)
        idx = INDEX_METRICS
        # Agrupar por mes y hacer JOIN con menu_items
        for (month, item_id), metrics in session_data['metrics'].items():
            # JOIN: buscar item_name para el item_id de esta sesión (o fallback al propio id)
            item_name = session_data['menu_items'].get(item_id)
            
            results_by_month[month].append({
                'item_id': item_id,
                'item_name': item_name,
                'total_quantity': metrics[idx["total_quantity"]],
                'total_subtotal': metrics[idx["total_subtotal"]]
            })
        
        final_results = []
        
        # Para cada mes, encontrar el producto más vendido y el de mayor ganancia
        for month, products in results_by_month.items():
            # Producto más vendido (mayor quantity)
            most_sold = max(products, key=lambda x: x['total_quantity'])
            
            # Producto con mayor ganancia (mayor subtotal)
            most_profitable = max(products, key=lambda x: x['total_subtotal'])
            
            final_results.append({
                'year_month_created_at': yyyymm_int_to_str(month),
                'item_name': most_sold['item_name'],
                'sellings_qty': most_sold['total_quantity'],
                'profit_sum': '',
                'metric_type': 'most_sold'
            })
            final_results.append({
                'year_month_created_at': yyyymm_int_to_str(month),
                'item_name': most_profitable['item_name'],
                'sellings_qty': '',
                'profit_sum': most_profitable['total_subtotal'],
                'metric_type': 'most_profitable'
            })
        
        # Ordenar por mes para consistencia
        final_results.sort(key=lambda x: x['year_month_created_at'])
        
        logger.info(f"[AggregatorQuery2] Resultados generados para {len(results_by_month)} meses")
        logger.info(f"[AggregatorQuery2] Total registros de resultados: {len(final_results)}")
     
        return final_results
    
    def generate_and_send_results(self, session_id):
        """Genera y envía los resultados finales cuando ambos flujos terminaron para una sesión específica."""
            
        # Generar resultados finales (TOP productos por mes) para esta sesión
        final_results = self.generate_final_results(session_id)
        logger.info(f"[AggregatorQuery2] Resultados generados: {len(final_results)} registros")
        
            
        if final_results:
            batch_size = 1000
            total_batches = (len(final_results) + batch_size - 1) // batch_size
            mq_out = MessageMiddlewareQueue(RABBIT_HOST, OUTPUT_QUEUE)
            
            
            for i in range(0, len(final_results), batch_size):
                batch_id = i // batch_size + 1
                batch = final_results[i:i + batch_size]

                batch_header = {
                    "batch_id": batch_id,
                    "is_eos": batch_id == total_batches,
                    "session_id": session_id,
                }

                result_msg = serialize_message(batch, batch_header)

                mq_out.send(result_msg)
            
            mq_out.close()
        else:
            logger.warning("[AggregatorQuery2] No hay resultados para enviar")
            
        logger.info(f"[AggregatorQuery2] Resultados finales enviados para sesión {session_id}. Worker continúa activo.")
    
    def __finish_session(self, session_id):
        """Marca una sesión como finalizada"""
        self.state_manager.finish_session(session_id)
        
        self.finish_sessions.add(session_id)
        
        #data en memoria
        with self.session_data_lock:
            if session_id in self.session_data:
                del self.session_data[session_id]
                
    def __save_session_type(self, session_id, type: str):
        tracker_snap = self.session_tracker.get_single_session_type_snapshot(session_id, type)
        session_snap = self.__get_session_data(session_id)[type]
                    
        self.state_manager.save_type_state(session_id, type, session_snap, tracker_snap)
    
    def __save_session_type_add(self, session_id, type: str, new_data):
        tracker_snap = self.session_tracker.get_single_session_type_snapshot(session_id, type)
                    
        self.state_manager.save_type_state_add(session_id, type, new_data, tracker_snap)

    def on_metrics_message(self, body):
        """Maneja mensajes de métricas de group_by_query2."""
    
        try:
            
            header, rows = deserialize_message(body)
        
            session_id = header.get("session_id", "unknown")
            batch_id = int(header.get("batch_id"))
            is_eos = header.get("is_eos")
            
            if session_id in self.finish_sessions or \
                self.session_tracker.previus_update(session_id, "metrics", batch_id):
                return
            
            if is_eos:
                logger.info(f"Se recibió EOS en metrics para sesión {session_id}, batch_id: {batch_id}. Marcando como listo...")
            
            if rows:
                self.accumulate_metrics(rows, session_id)
            
            if self.session_tracker.update(session_id, "metrics", batch_id, is_eos):              
                self.generate_and_send_results(session_id)
                self.__finish_session(session_id)
            else:
                self.__save_session_type(session_id, "metrics")
                
        except Exception as e: 
            logger.error(f"[AggregatorQuery2] Error procesando el mensaje de metrics: {e}")
            logger.error(traceback.format_exc())
            
    def on_menu_items_message(self, body):
        """Maneja mensajes de menu_items para el JOIN."""
        try:
            header, rows = deserialize_message(body)
                
            session_id = header.get("session_id", "unknown")
            batch_id = int(header.get("batch_id"))
            is_eos = header.get("is_eos")
            
            if batch_id == 0 or batch_id % 10000: 
                logger.info(f"Se recibio batch {batch_id} para sesion {session_id}")
                
            if is_eos:
                logger.info(f"Se recibió EOS en metrics para sesión {session_id}, batch_id: {batch_id}. Marcando como listo...")
            
            if session_id in self.finish_sessions or \
                self.session_tracker.previus_update(session_id, "menu_items", batch_id):
                return
            
            if rows:
                self.load_menu_items(rows, session_id)
            
            if self.session_tracker.update(session_id, "menu_items", batch_id, is_eos):
                self.generate_and_send_results(session_id)
                self.__finish_session(session_id)
            else:
                self.__save_session_type(session_id, "menu_items")
        
        except Exception as e: 
            logger.error(f"[AggregatorQuery2] Error procesando el mensaje de menu items: {e}")
            logger.error(traceback.format_exc())
            
    
    def consume_metrics(self):
        try:
            self.mq_metrics = MessageMiddlewareQueue(RABBIT_HOST, GROUP_BY_Q2_QUEUE)
            self.mq_metrics.start_consuming(self.on_metrics_message)
        except Exception as e:
            if not self.shutdown_event.is_set():
                logger.error(f"[AggregatorQuery2] Error en consumo de métricas: {e}")
            else:
                self.mq_metrics.close()

    def consume_menu_items(self):
        try:
            self.mq_menu_items = MessageMiddlewareQueue(RABBIT_HOST, MENU_ITEMS_QUEUE)
            self.mq_menu_items.start_consuming(self.on_menu_items_message)
        except Exception as e:
            if not self.shutdown_event.is_set():
                logger.error(f"[AggregatorQuery2] Error en consumo de menu_items: {e}")
            else:
                self.mq_menu_items.close()
                
    def __init_healthcheck(self):
        "Iniciar servidor de healthcheck UDP"
        
        healthcheck_port = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
        start_healthcheck_server(port=healthcheck_port, node_name="aggregator_query2", 
                                 shutdown_event=self.shutdown_event, logger=logger)
        logger.info(f"[AggregatorQuery2] Healthcheck server iniciado en puerto UDP {healthcheck_port}")
    
    def signal_handler(self,signum, frame):
        logger.info(f"[AggregatorQuery2] Señal {signum} recibida, cerrando...")
        self.shutdown_event.set()
        
        for mq in [self.mq_metrics, self.mq_menu_items]:
                try:
                    mq.stop_consuming()
                except Exception as e:
                    logger.error(f"Error al parar el consumo: {e}")
    
    def __init_signal_handler(self):
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
    
    def __init_delete_sessions(self):
        """
        Inicializa y comienza el hilo de fondo para la limpieza periódica 
        de sesiones expiradas.
        """
        logger.info(f"[*] Inicializando limpieza periódica de sesiones...")
        
        self.delete_thread = threading.Thread(
            target=delete_sessions_thread,
            args=(self.state_manager, self.shutdown_event, logger, self.session_data, self.session_data_lock),
            name="SessionCleanupThread"
        )
        
        self.delete_thread.start()
        
    def __load_sessions_data(self, data: dict): 
        for session_id, types_data in data.items():
            self.__initialize_session(session_id)
            
            if 'metrics' in types_data:
                self.session_data[session_id]['metrics'] =  types_data['metrics']
               
            if 'menu_items' in types_data:
                self.session_data[session_id]['menu_items'] = types_data['menu_items'] 
            
      
    def __load_sessions(self):
        logger.info("[*] Intentando recuperar estado previo...")
        saved_data, saved_tracker, finish_sessions = self.state_manager.load_all_sessions()

        if saved_data and saved_tracker:
            self.__load_sessions_data(saved_data)
            self.session_tracker.load_state_snapshot(saved_tracker)
            
            logger.info(f"[*] Estado recuperado. Sesiones activas: {len(self.session_data)}")
        else:
            logger.info("[*] No se encontró estado previo o estaba corrupto. Iniciando desde cero.")
        
        self.finish_sessions = finish_sessions
    
    
    def start(self):
        self.__init_healthcheck()
        self.__init_signal_handler()
        self.__init_delete_sessions()
        
        self.__load_sessions()
        
        logger.info("[*] AggregatorQuery2 esperando mensajes...")
        logger.info("[*] Query 2: Productos más vendidos y mayor ganancia por mes 2024-2025")
        logger.info("[*] Consumiendo de 2 fuentes: métricas + menu_items para JOIN")
        
        self.__run()

        self.__close_delete_session()
        logger.info("[x] AggregatorQuery2 detenido")

    def __run(self):
        metrics_thread = threading.Thread(target=self.consume_metrics, daemon=True)
        menu_items_thread = threading.Thread(target=self.consume_menu_items, daemon=True)
        
        metrics_thread.start()
        menu_items_thread.start()
        
        logger.info("[AggregatorQuery2] Worker iniciado, esperando mensajes de múltiples sesiones...")
        
        metrics_thread.join()
        menu_items_thread.join()
    
    def __close_delete_session(self):
        if self.delete_thread and self.delete_thread.is_alive():
            self.delete_thread.join()
        
        self.state_manager.finish_all_active_sessions()

    
if __name__ == "__main__":
    logger = init_log("AgregatorQuery2")
    aggregator = AggregatorQuery2()
    aggregator.start()
    
