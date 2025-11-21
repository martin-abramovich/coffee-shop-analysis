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
        self._lock = threading.Lock()

    def register_session(self, session_id: str) -> None:
        """Asegura que exista el estado interno para una sesión."""
        self._ensure_session_structures(session_id)

    def submit_result(self, session_id: str, query_name: str, payload: Dict[str, Any]) -> None:
        """
        Guarda el resultado final de una query para la sesión indicada
        y notifica a los threads que estén esperando esos datos.
        """
        condition = self._ensure_session_structures(session_id)
        with condition:
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
        """
        condition = self._ensure_session_structures(session_id)
        end_time = time.time() + timeout if timeout else None

        with condition:
            while True:
                current_results = self._results.get(session_id, {})
                if not expected_queries or expected_queries.issubset(current_results.keys()):
                    break

                if timeout is None:
                    condition.wait()
                    continue

                remaining = end_time - time.time()
                if remaining <= 0:
                    break
                condition.wait(timeout=remaining)

            final_results = dict(self._results.get(session_id, {}))

        missing = []
        if expected_queries:
            missing = [q for q in expected_queries if q not in final_results]

        return final_results, missing

    def cleanup_session(self, session_id: str) -> None:
        """Elimina el estado en memoria para una sesión ya finalizada."""
        with self._lock:
            self._results.pop(session_id, None)
            self._conditions.pop(session_id, None)

    def _ensure_session_structures(self, session_id: str) -> threading.Condition:
        with self._lock:
            if session_id not in self._conditions:
                self._conditions[session_id] = threading.Condition()
                self._results.setdefault(session_id, {})
            return self._conditions[session_id]


result_dispatcher = ResultDispatcher()

