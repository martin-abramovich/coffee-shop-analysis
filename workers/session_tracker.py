from collections import defaultdict
import threading


class SessionTracker:
    """
    Tracker concurrente y multi-tipo de rangos de batch_ids por sesión.
    Ultra optimizado para detección de completitud sin ordenar listas.
    """

    def __init__(self, tracked_types: list[str]):
        self.tracked_types = set(tracked_types)
        
        self.sessions = defaultdict(lambda: {"_lock": threading.Lock()})
        self.global_lock = threading.Lock()

    @staticmethod
    def _merge_ranges(ranges, new_start, new_end):
        """Fusión incremental en O(n) sin ordenar: asume rangos cortos o contiguos."""
        merged = []
        placed = False
        for start, end in ranges:
            if end + 1 < new_start:
                merged.append((start, end))
            elif new_end + 1 < start:
                if not placed:
                    merged.append((new_start, new_end))
                    placed = True
                merged.append((start, end))
            else:
                # Solapados o contiguos → fusionar
                new_start = min(new_start, start)
                new_end = max(new_end, end)
        if not placed:
            merged.append((new_start, new_end))
        return merged

    def update(self, session_id: str, entity_type: str, batch_id: int, is_eos: bool) -> bool:
        """
        Actualiza el estado de una sesión para un tipo específico.
        Devuelve True si todos los tipos de esa sesión están completos.
        """
        if entity_type not in self.tracked_types:
            return False
        
        with self.global_lock:
            session_info = self.sessions[session_id]
            if entity_type not in session_info:
                session_info[entity_type] = {"ranges": [], "expected": None}
                
        with session_info["_lock"]:
            tipo_info = session_info[entity_type]
            tipo_info["ranges"] = self._merge_ranges(tipo_info["ranges"], batch_id, batch_id)

            if is_eos:
                tipo_info["expected"] = batch_id
                print(tipo_info)

            if tipo_info["expected"] is not None:
                if len(tipo_info["ranges"]) == 1:
                    start, end = tipo_info["ranges"][0]
                    if start == 0 and end >= tipo_info["expected"]:
                        tipo_info["done"] = True

            all_done = all(
                session_info.get(t, {}).get("done", False)
                for t in self.tracked_types
            )

            if all_done:
                with self.global_lock:
                    del self.sessions[session_id]
                return True

        return False