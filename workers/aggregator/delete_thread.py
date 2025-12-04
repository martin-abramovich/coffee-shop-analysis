import os
import threading
import traceback

from workers.aggregator.sesion_state_manager import SessionStateManager

CLEANUP_INTERVAL_SECONDS = int(os.getenv("CLEANUP_INTERVAL_SECONDS", 600))  # Intervalo de limpieza en segundos
SESSION_EXPIRATION_SECONDS = int(os.getenv("SESSION_EXPIRATION_SECONDS", 60))  # Tiempo de expiración de sesión en segundos

def delete_sessions_thread(state_manager: SessionStateManager, shutdown_event: threading.Event, logger, 
                           session_data: dict, session_data_lock: threading.Lock):
        """
        Función ejecutada en un hilo de fondo. Se despierta periódicamente
        para llamar a SessionStateManager.delete_finish_session.
        """
        logger.info("Hilo de depuración iniciado.")
        
        while not shutdown_event.is_set():
            
            shutdown_event.wait(CLEANUP_INTERVAL_SECONDS)
            
            if shutdown_event.is_set():
                break
                
            try:
                inactive_deleted_ids = state_manager.delete_finish_session(SESSION_EXPIRATION_SECONDS)
                if inactive_deleted_ids:
                    with session_data_lock:
                        for session_id in inactive_deleted_ids:
                            if session_id in session_data:
                                del session_data[session_id]
            except Exception as e:
                logger.error(f"Error en el hilo de depuración: {e}")
                logger.error(traceback.format_exc())
                
        logger.info("Hilo de depuración finalizado.")