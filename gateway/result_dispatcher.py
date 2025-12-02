import threading
import time
from typing import Dict, Any, List, Optional, Set, Tuple


class ResultDispatcher:
    """
    Gestiona la coordinación de resultados finales entre el handler de resultados
    (que recibe datos desde RabbitMQ) y los threads del gateway que atienden a cada cliente.
    """

    def __init__(self) -> None:
        self._results: Dict[str, Dict[str, Any]] = {}
        self._conditions: Dict[str, threading.Condition] = {}
        self._cancelled: Dict[str, bool] = {}
        self._lock = threading.Lock()

    def register_session(self, session_id: str) -> None:
        """Asegura que exista el estado interno para una sesión."""
        self._ensure_session_structures(session_id)

    def submit_result(self, session_id: str, query_name: str, payload: Dict[str, Any]) -> None:
        """
        Guarda el resultado final de una query para la sesión indicada
        y notifica a los threads que estén esperando esos datos.
        Si la sesión fue cancelada, los resultados se descartan.
        """
        condition = self._ensure_session_structures(session_id)
        with condition:
            if self._cancelled.get(session_id, False):
                return
            
            session_results = self._results.setdefault(session_id, {})
            session_results[query_name] = payload
            condition.notify_all()

    def wait_for_results(
        self,
        session_id: str,
        expected_queries: Optional[Set[str]] = None,
        timeout: Optional[float] = None,
    ) -> Tuple[Dict[str, Any], List[str]]:
        """
        Bloquea hasta que se completen las queries esperadas o se agote el timeout.
        Retorna (resultados, queries_pendientes).
        Si la sesión fue cancelada, retorna inmediatamente con los resultados disponibles.
        """
        condition = self._ensure_session_structures(session_id)
        end_time = time.time() + timeout if timeout else None

        with condition:
            while True:
                if self._cancelled.get(session_id, False):
                    break
                
                current_results = self._results.get(session_id, {})
                if not expected_queries or expected_queries.issubset(current_results.keys()):
                    break

                if timeout is None:
                    condition.wait(timeout=1.0)  # Usar timeout para verificar cancelación periódicamente
                    continue

                remaining = end_time - time.time()
                if remaining <= 0:
                    break
                # Usar el menor entre el timeout restante y 1 segundo para verificar cancelación
                wait_time = min(remaining, 1.0)
                condition.wait(timeout=wait_time)

            final_results = dict(self._results.get(session_id, {}))

        missing = []
        if expected_queries:
            missing = [q for q in expected_queries if q not in final_results]

        return final_results, missing

    def cancel_wait(self, session_id: str) -> None:
        """Cancela la espera de resultados para una sesión (útil cuando el cliente se desconecta)."""
        condition = self._ensure_session_structures(session_id)
        with self._lock:
            self._cancelled[session_id] = True
        with condition:
            condition.notify_all()
    
    def is_session_cancelled(self, session_id: str) -> bool:
        """Verifica si una sesión ha sido cancelada (cliente desconectado)."""
        with self._lock:
            return self._cancelled.get(session_id, False)
    
    def get_results_immediate(
        self,
        session_id: str,
        expected_queries: Optional[Set[str]] = None,
    ) -> Tuple[Dict[str, Any], List[str]]:
        """
        Obtiene los resultados disponibles inmediatamente sin esperar.
        Útil cuando el cliente se desconecta y no queremos bloquear.
        """
        condition = self._ensure_session_structures(session_id)
        # Leer resultados dentro del lock de la condición para thread-safety
        with condition:
            current_results = dict(self._results.get(session_id, {}))
        
        missing = []
        if expected_queries:
            missing = [q for q in expected_queries if q not in current_results]
        
        return current_results, missing
    
    def cleanup_session(self, session_id: str) -> None:
        """Elimina el estado en memoria para una sesión ya finalizada."""
        with self._lock:
            self._results.pop(session_id, None)
            self._conditions.pop(session_id, None)
            self._cancelled.pop(session_id, None)

    def _ensure_session_structures(self, session_id: str) -> threading.Condition:
        with self._lock:
            if session_id not in self._conditions:
                self._conditions[session_id] = threading.Condition()
                self._results.setdefault(session_id, {})
                self._cancelled.setdefault(session_id, False)
            return self._conditions[session_id]


result_dispatcher = ResultDispatcher()

