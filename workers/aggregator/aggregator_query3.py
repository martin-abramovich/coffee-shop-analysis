import logging
import sys
import os
import signal
import threading
import time
from collections import defaultdict

from common.utils import yyyys_int_to_string
from workers.aggregator.delete_thread import delete_sessions_thread
from workers.aggregator.sesion_state_manager import SessionStateManager
from common.logger import init_log
from workers.session_tracker import SessionTracker

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from workers.utils import deserialize_message, serialize_message
from common.healthcheck import start_healthcheck_server

RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')

INPUT_QUEUE = "group_by_q3"    

OUTPUT_EXCHANGE = "results_query3"        
ROUTING_KEY = "query3_results"            

STORES_EXCHANGE = "stores_raw"
STORES_ROUTING_KEY = "q3"


class AggregatorQuery3:
    def __init__(self):
        # Datos por sesión: {session_id: session_data}
        self.session_data = {}
        self.session_data_lock = threading.Lock()
        
        self.shutdown_event = threading.Event()
        self.session_tracker = SessionTracker(tracked_types=["tpv", "stores"])
        self.state_manager = SessionStateManager(logger=logger)
        self.finish_sessions = set()
        
        #in
        self.tpv_queue = None
        self.stores_exchange = None 
        
    def __initialize_session(self, session_id):
        """Inicializa datos para una nueva sesión"""
        with self.session_data_lock:
            if session_id not in self.session_data:
                self.session_data[session_id] = {
                    'tpv':{
                        'semester_store_tpv': defaultdict(float),
                        'batches_received': 0,
                        },
                    'stores': {},
                }
    
    def __get_session_data(self, session_id):
        """Obtiene los datos de una sesión específica"""
        self.__initialize_session(session_id)
        return self.session_data[session_id]
        
    def __load_stores(self, rows, session_id):
        """Carga stores para construir el diccionario store_id -> store_name para una sesión específica."""
        session_data = self.__get_session_data(session_id)
        stores = session_data['stores']
        
        for row in rows:
            store_id = int(row.get('store_id'))
            store_name = row.get('store_name')
            
            if store_id and store_name:
                stores[store_id] = store_name.strip()
        
        logger.info(f"[AggregatorQuery3] Sesión {session_id}: Cargadas {len(stores)} stores para JOIN")
    
    def __accumulate_tpv(self, rows, session_id):
        """Acumula TPV parciales de group_by_query3 para una sesión específica."""
        session_data = self.__get_session_data(session_id)
        semester_store_tpv = session_data["tpv"]['semester_store_tpv']
        
        session_data["tpv"]['batches_received'] += 1
        batches_received = session_data["tpv"]['batches_received']
        
        for row in rows:
            semester = int(row.get('semester', 0))
            store_id = int(row.get('store_id', 0))
            total_payment_value = float(row.get('total_payment_value', 0.0))
            
            # Validar campos requeridos
            if semester == 0 or store_id == 0:
                continue
                
            
            # Clave compuesta: (semestre, store_id)
            key = (semester, store_id)
            
            # Acumular TPV para esta sesión
            semester_store_tpv[key] +=  total_payment_value
            
        
        if batches_received % 10000 == 0 or batches_received == 1:
            logger.info(f"[AggregatorQuery3] Sesión {session_id}: Procesado batch {batches_received} con {len(rows)} registros. Total combinaciones: {len(semester_store_tpv)}")
    
            
    def __generate_final_results(self, session_id):
        """Genera los resultados finales para Query 3 con JOIN para una sesión específica."""
        session_data = self.__get_session_data(session_id)
        
        semester_store_tpv = session_data['tpv']['semester_store_tpv']
        stores = session_data['stores']
        
        logger.info(f"[AggregatorQuery3] Generando resultados finales para sesión {session_id}...")
        logger.info(f"[AggregatorQuery3] Total combinaciones procesadas: {len(semester_store_tpv)}")
        logger.info(f"[AggregatorQuery3] Stores disponibles para JOIN: {len(stores)}")
        
        if not semester_store_tpv:
            logger.warning(f"[AggregatorQuery3] No hay datos para procesar en sesión {session_id}")
            return []
        
        if not stores:
            logger.warning(f"[AggregatorQuery3] WARNING: No hay stores cargadas para sesión {session_id}. No se puede hacer JOIN.")
            return []
        
        final_results = []
        
        # Procesar cada combinación (semestre, store_id) con JOIN
        for (semester, store_id), total_tpv in semester_store_tpv.items():
            # JOIN: buscar store_name para el store_id de esta sesión
            store_name = stores.get(store_id)
            
            if not store_name:
                logger.warning(f"[AggregatorQuery3] WARNING: store_id {store_id} no encontrado en stores")
                continue
                        
            final_results.append({
                'year_half_created_at': yyyys_int_to_string(semester),
                'store_name': store_name,
                'tpv': round(total_tpv, 2)  # Redondear TPV a 2 decimales
            })
        
        # Ordenar por semestre y luego por store_name para consistencia
        final_results.sort(key=lambda x: (x['year_half_created_at'], x['store_name']))
        
        logger.info(f"[AggregatorQuery3] Resultados generados: {len(final_results)} combinaciones")
              
        return final_results

    def __generate_and_send_results(self, session_id):
        """Genera y envía los resultados finales cuando ambos flujos terminaron para una sesión específica."""

        logger.info(f"[AggregatorQuery3] Ambos flujos completados para sesión {session_id}. Generando resultados finales...")

        # Generar resultados finales para esta sesión
        final_results = self.__generate_final_results(session_id)

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
            batch_size = 1000
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
            
        logger.info(f"[AggregatorQuery3] Resultados finales enviados para sesión {session_id}. Worker continúa activo esperando nuevos clientes...")
    
    
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
        
            
    def __on_tpv_message(self, body):
        """Maneja mensajes de TPV de group_by_query3."""
        header, rows = deserialize_message(body)

        session_id = header.get("session_id", "unknown")
        batch_id = int(header.get("batch_id"))
        is_eos = header.get("is_eos")

        if session_id in self.finish_sessions or \
            self.session_tracker.previus_update(session_id, "tpv", batch_id):
            return
        
        if is_eos:
            logger.info(f"[AggregatorQuery3] Recibido mensaje EOS en TPV para sesión {session_id}, batch_id {batch_id}")
            
        if rows:
            self.__accumulate_tpv(rows, session_id)
            
        if self.session_tracker.update(session_id, "tpv", batch_id, is_eos):
            self.__generate_and_send_results(session_id)
            self.__finish_session(session_id)
            
        else: 
            self.__save_session_type(session_id, "tpv")

    def __on_stores_message(self, body):
        """Maneja mensajes de stores para el JOIN."""
        try:
            header, rows = deserialize_message(body)
        except Exception as e:
            logger.error(f"[AggregatorQuery3] Error deserializando mensaje: {e}")
            return
        
        session_id = header.get("session_id", "unknown")
        is_eos = header.get("is_eos")
        batch_id = int(header.get("batch_id", -1))

        if session_id in self.finish_sessions or \
            self.session_tracker.previus_update(session_id, "stores", batch_id):
            return
        
        if is_eos:
            logger.info(f"[AggregatorQuery3] Recibido mensaje EOS en Stores para sesión {session_id}, batch_id {batch_id}")
        
        if rows:
            self.__load_stores(rows, session_id)
            
        if self.session_tracker.update(session_id, "stores", batch_id, is_eos):
            self.__generate_and_send_results(session_id)
            self.__finish_session(session_id)
        else: 
            self.__save_session_type(session_id, "stores")
        
    
    def __consume_tpv(self):
        try:
            self.tpv_queue = MessageMiddlewareQueue(RABBIT_HOST, INPUT_QUEUE)
            self.tpv_queue.start_consuming(self.__on_tpv_message)
        except Exception as e:
            if not self.shutdown_event.is_set():
                logger.error(f"[AggregatorQuery3] Error en consumo de TPV: {e}")
            else: 
                self.tpv_queue.close()
                
                                 
    def __consume_stores(self):
        try:
            self.stores_exchange = MessageMiddlewareExchange(RABBIT_HOST, STORES_EXCHANGE, [STORES_ROUTING_KEY])
            self.stores_exchange.start_consuming(self.__on_stores_message)
        except Exception as e:
            if not self.shutdown_event.is_set():
                logger.error(f"[AggregatorQuery3] Error en consumo de stores: {e}")
            else: 
                self.stores_exchange.close()
    

    def __signal_handler(self, signum, frame):
        logger.info(f"[AggregatorQuery3] Señal {signum} recibida, cerrando...")
        self.shutdown_event.set()
        
        for mq in [self.stores_exchange, self.tpv_queue]:
            try:
                mq.stop_consuming()
            except Exception as e: 
                logger.error(f"Error al parar el consumo: {e}")


    def __init_signal_handler(self):
        signal.signal(signal.SIGTERM, self.__signal_handler)
        signal.signal(signal.SIGINT, self.__signal_handler)

    def __init_healthcheck(self):
        "Iniciar servidor de healthcheck UDP"
        
        healthcheck_port = int(os.environ.get('HEALTHCHECK_PORT', '8888'))
        start_healthcheck_server(port=healthcheck_port, node_name="aggregator_query3",
                                 shutdown_event=self.shutdown_event, logger=logger)
        logger.info(f"[AggregatorQuery3] Healthcheck server iniciado en puerto UDP {healthcheck_port}")
    
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
            
            if 'stores' in types_data:
                self.session_data[session_id]['stores'] = types_data['stores']
                        
            if 'tpv' in types_data:
                loaded_tpv = types_data['tpv']
                
                raw_dict = loaded_tpv.get('semester_store_tpv', {})
                if not isinstance(raw_dict, defaultdict):
                    loaded_tpv['semester_store_tpv'] = defaultdict(float, raw_dict)
                
                self.session_data[session_id]['tpv'] = loaded_tpv  
                    
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
        
        logger.info("[*] AggregatorQuery3 esperando mensajes...")
        logger.info("[*] Query 3: TPV por semestre y sucursal 2024-2025 (06:00-23:00)")
        logger.info("[*] Consumiendo de 2 fuentes: TPV + stores para JOIN")
        
        self.__run()
                
        self.__close_delete_session()    
        
        logger.info("[x] AggregatorQuery3 detenido")

    def __run(self):
        tpv_thread = threading.Thread(target=self.__consume_tpv)
        stores_thread = threading.Thread(target=self.__consume_stores)
            
        tpv_thread.start()
        stores_thread.start()
            
        logger.info("[AggregatorQuery3] Worker iniciado, esperando mensajes de múltiples sesiones...")
            
        tpv_thread.join()
        stores_thread.join()

    def __close_delete_session(self):
        if self.delete_thread and self.delete_thread.is_alive():
            self.delete_thread.join()
        
        self.state_manager.finish_all_active_sessions()


                
if __name__ == "__main__":
    logger = init_log("AggregatorQuery3")
    
    aggregator = AggregatorQuery3()
    aggregator.start()
    
    
