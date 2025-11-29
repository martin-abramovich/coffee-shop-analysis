from collections import defaultdict
import pickle
import os
import logging
import glob
import shutil

logger = logging.getLogger(__name__)

INDEX_SAVE = {
    "data": 0,
    "tracker": 1,
}

def merge_sum_dicts(base_data, new_chunk):
    """Función de fusión para tipos acumulativos (ej: 'transactions'). Suma los valores de las claves existentes."""
    for key, value in new_chunk.items():
        base_data[key] += value
    return base_data

def merge_replace_dicts(base_data, new_chunk):
    """Función de fusión para tipos de reemplazo/actualización (ej: 'users', 'stores'). Las nuevas claves reemplazan o se añaden."""
    base_data.update(new_chunk)
    return base_data

# --- Configuración Base para Inicialización ---


class SessionStateManager:
    def __init__(self, data_type_configs = None, base_dir="./state"):
        """
        Inicializa State Manager, para la persisitencia de los datos.\n 
        - data_type_configs: es un diccionario con la configuracion para 
         el merge a la hora de load. Ej: \n
         DEFAULT_DATA_CONFIGS = {
            Tipo:           (Elemento Base, Función de Fusión)
            "transactions": (lambda: defaultdict(int), merge_sum_dicts),
            "users":        (dict, merge_replace_dicts),
            }
        """
        
        self.base_dir = base_dir
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)
        
        self.data_type_configs = data_type_configs
            
    def save_type_state(self, session_id, data_type, aggregator_data, tracker_data):
        """
        Guarda atómicamente SOLO el estado de un tipo específico para una sesión.
        Crea la carpeta de la sesión si no existe.
        """
        # Estructura: ./data/sessions/{session_id}/
        session_dir = os.path.join(self.base_dir, str(session_id))
        
        # exist_ok=True es thread-safe a nivel de OS
        os.makedirs(session_dir, exist_ok=True)

        # Estructura: ./data/sessions/{session_id}/{data_type}.pkl
        filename = os.path.join(session_dir, f"{data_type}.pkl")
        temp_filename = filename + ".tmp"
        
        state = (aggregator_data, tracker_data)
        
        try:
            with open(temp_filename, 'wb') as f:
                # Protocolo más alto para velocidad máxima
                pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
                f.flush()
                os.fsync(f.fileno()) 

            # Reemplazo atómico del archivo específico
            os.replace(temp_filename, filename)
            
        except Exception as e:
            logger.error(f"Error guardando {data_type} para sesión {session_id}: {e}")
    
    def save_type_state_add(self, session_id, data_type, aggregator_data, tracker_data):
        # Estructura: ./data/sessions/{session_id}/{data_type}
        session_dir = os.path.join(self.base_dir, f"{session_id}/{data_type}")
        os.makedirs(session_dir, exist_ok=True)
        
        data_filename = os.path.join(session_dir, f"data.pkl")
        info_filename = os.path.join(session_dir, f"info.pkl")
        
        # 1. APPEND: Escribimos la data primero
        # Obtenemos la posición actual antes de escribir por si hay fallo
        start_pos = 0
        
        if aggregator_data: 
            if os.path.exists(data_filename):
                start_pos = os.path.getsize(data_filename)
                
            try:
                with open(data_filename, 'ab') as f:
                    pickle.dump(aggregator_data, f, protocol=pickle.HIGHEST_PROTOCOL)
                    f.flush()
                    os.fsync(f.fileno()) # Forzamos escritura física en disco
            except Exception as e:
                # Si falla la escritura del append, intentamos truncar al estado anterior
                self._truncate_file(data_filename, start_pos)
                raise e

        # 2. ATOMIC UPDATE: Actualizamos el tracker
        # El tracker DEBE saber cuántos chunks válidos hay o hasta qué byte leer
        # Agregamos metadata de validación al tracker
        if os.path.exists(data_filename):
            start_pos = os.path.getsize(data_filename)
            
        info_data = (start_pos, tracker_data)
        
        temp_info = info_filename + ".tmp"
        
        try:
            with open(temp_info, 'wb') as f:
                pickle.dump(info_data, f, protocol=pickle.HIGHEST_PROTOCOL)
                f.flush()
                os.fsync(f.fileno())
            # Este es el "Commit": solo cuando este rename ocurre, la data anterior es válida
            os.replace(temp_info, info_filename)
        except Exception as e:
            # Si esto falla, la data en aggregator es "basura huérfana" que limpiaremos al leer
            logger.error(f"Fallo actualizando tracker, estado inconsistente pero recuperable: {e}")

    def _truncate_file(self, filepath, size):
        try:
            with open(filepath, 'r+b') as f:
                f.truncate(size)
        except Exception:
            pass
        
    def delete_session(self, session_id):
        """
        Borra EL DIRECTORIO COMPLETO de la sesión al finalizar.
        """
        session_dir = os.path.join(self.base_dir, str(session_id))
        try:
            if os.path.exists(session_dir):
                shutil.rmtree(session_dir) # Borra carpeta y todo su contenido
        except OSError as e:
            logger.warning(f"No se pudo borrar directorio de sesión {session_id}: {e}")
    
  
    def load_all_sessions(self):
        """
        Recorre carpetas y archivos para reconstruir el estado global.
        Maneja tanto archivos .pkl simples (legado/update) como carpetas optimizadas (append).
        """
        idx = INDEX_SAVE
        aggregator_map = {}
        tracker_map = {}
        
        if not os.path.exists(self.base_dir):
            return aggregator_map, tracker_map

        # Listar todas las carpetas de sesiones (IDs numéricos o strings)
        session_dirs = [d for d in os.listdir(self.base_dir) if os.path.isdir(os.path.join(self.base_dir, d))]
        
        logger.info(f"Recuperando {len(session_dirs)} sesiones del disco...")

        for s_id in session_dirs:
            s_path = os.path.join(self.base_dir, s_id)
            
            # Inicializar diccionarios para esta sesión
            if s_id not in aggregator_map: aggregator_map[s_id] = {}
            if s_id not in tracker_map: tracker_map[s_id] = {}

            # Listar contenido dentro de la sesión (pueden ser archivos .pkl o carpetas)
            for item_name in os.listdir(s_path):
                item_path = os.path.join(s_path, item_name)
                
                # --- CASO A: ES UNA CARPETA (Lógica Optimizada Append) ---
                if os.path.isdir(item_path):
                    data_type = item_name # El nombre de la carpeta es el tipo de dato
                    try:
                        agg_data, track_data = self.__load_optimized_directory(item_path, data_type)
                        if agg_data is not None:
                            aggregator_map[s_id][data_type] = agg_data
                            tracker_map[s_id][data_type] = track_data
                    except Exception as e:
                         logger.error(f"Error cargando carpeta {item_path}: {e}")

                # --- CASO B: ES UN ARCHIVO (Lógica Simple Update) ---
                elif item_name.endswith(".pkl") and not item_name.endswith(".tmp"):
                    data_type = os.path.splitext(item_name)[0]
                    try:
                        with open(item_path, 'rb') as f:
                            state = pickle.load(f)
                        
                        # state es tu tupla (aggregator, tracker)
                        aggregator_map[s_id][data_type] = state[idx['data']] 
                        tracker_map[s_id][data_type] = state[idx['tracker']]
                        
                    except Exception as e:
                        logger.error(f"Archivo simple corrupto {item_path}: {e}")
        
        return aggregator_map, tracker_map

    def __load_optimized_directory(self, dir_path, type):
        """
        Lee una estructura de carpeta optimizada:
        - info.pkl: Fuente de la verdad (Metadata + Tracker).
        - data.pkl: Datos apilados (Append only).
        
        Realiza saneamiento automático si data.pkl es más grande de lo que info.pkl dice.
        """
        info_path = os.path.join(dir_path, "info.pkl")
        data_path = os.path.join(dir_path, "data.pkl")
        
        # 1. Si no hay info, la carpeta es inválida o está vacía
        if not os.path.exists(info_path):
            return None, None

        # 2. Leer INFO (Tracker + Tamaño válido)
        try:
            with open(info_path, 'rb') as f:
                # Tupla guardada: (size_bytes, tracker_data)
                valid_size, tracker_data = pickle.load(f)
        except Exception:
            logger.error(f"Info corrupta en {dir_path}, omitiendo.")
            return None, None

        # 3. Leer DATA con validación de tamaño
        BaseClass, merge_func = self.data_type_configs[type]
        full_aggregator_data = BaseClass()
        
        if os.path.exists(data_path):
            current_size = os.path.getsize(data_path)
            
            # --- AUTO-REPARACIÓN ---
            # Si el archivo data es mayor que lo que dice el tracker, hubo un crash escribiendo.
            if current_size > valid_size:
                logger.warning(f"Inconsistencia detectada en {dir_path}. Real: {current_size}, Valido: {valid_size}. Truncando...")
                self._truncate_file(data_path, valid_size)
            
            # 4. Loop de lectura de objetos apilados
            try:
                with open(data_path, 'rb') as f:
                    # Solo leemos hasta donde el tracker dijo que era válido
                    while f.tell() < valid_size:
                        try:
                            chunk = pickle.load(f)
                            
                            if isinstance(chunk, dict):
                                full_aggregator_data = merge_func(full_aggregator_data, chunk)
                            else:
                                logger.warning(f"Chunk inesperado encontrado en {data_path}. Esperado dict, encontrado {type(chunk)}.")    
                                
                        except EOFError:
                            break
            except Exception as e:
                logger.error(f"Error leyendo flujo de datos en {data_path}: {e}")
                
        return full_aggregator_data, tracker_data