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

    def previus_update(self, session_id:str, entity_type:str, batch_id: int) -> bool:
        """
        Devuelve True si el batch_id ya fue procesado previamente.
        No modifica el estado.
        """
        # Si la sesión no existe, seguro no se procesó
        if session_id not in self.sessions:
            return False

        session_info = self.sessions[session_id]

        # Si ese tipo aún no tiene registros → tampoco se procesó
        if entity_type not in session_info:
            return False

        # Sección crítica por tipo
        with session_info["_lock"]:
            ranges = session_info[entity_type].get("ranges", [])
            
            for start, end in ranges:
                if start <= batch_id <= end:
                    return True

        return False

            
         
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
    
    def count_batches(self, session_id: str, entity_type: str) -> int:
        """
        Devuelve la cantidad TOTAL de batch_ids recibidos para una sesión y tipo.
        Cuenta todos los rangos acumulados.
        """
        if session_id not in self.sessions:
            return 0

        session_info = self.sessions[session_id]

        with session_info["_lock"]:
            tipo_info = session_info.get(entity_type)
            if not tipo_info:
                return 0
            
            ranges = tipo_info.get("ranges", [])
            total = 0
            for start, end in ranges:
                total += (end - start + 1)

            return total
    
    def load_state_snapshot(self, snapshot):
        """Reconstruye el estado desde un snapshot."""
        with self.global_lock:
            self.sessions.clear()
            for session_id, data in snapshot.items():
                self.sessions[session_id] = data
                self.sessions[session_id]["_lock"] = threading.Lock()
    
    def get_single_session_type_snapshot(self, session_id, entity_type):
        """Retorna el estado del tracker SOLO para un tipo específico."""
        if session_id not in self.sessions:
            return {}
            
        session_info = self.sessions[session_id]
        with session_info["_lock"]:
            if entity_type in session_info:
                # Retornamos copia del dict de ese tipo (ranges, expected, done)            
                return session_info[entity_type].copy()
        return {}