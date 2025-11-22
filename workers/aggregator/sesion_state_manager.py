import pickle
import os
import logging
import glob

logger = logging.getLogger(__name__)

class SessionStateManager:
    def __init__(self, base_dir="./state"):
        self.base_dir = base_dir
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

    def save_session(self, session_id, aggregator_data, tracker_data):
        """
        Guarda atómicamente EL ESTADO DE UNA SOLA SESIÓN.
        """
        filename = os.path.join(self.base_dir, f"{session_id}.pkl")
        temp_filename = filename + ".tmp"
        
        state = {
            'session_id': session_id,
            'aggregator_data': aggregator_data, # Datos de transacciones de ESTA sesión
            'tracker_data': tracker_data        # Rangos de batch de ESTA sesión
        }
        
        try:
            with open(temp_filename, 'wb') as f:
                # Pickle al máximo protocolo es muy rápido
                pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
                # f.flush() y os.fsync() son opcionales. 
                # Sin fsync es mucho más rápido pero con riesgo leve si se va la luz.
                f.flush() 
                os.fsync(f.fileno()) 

            # Reemplazo atómico: solo afecta a este archivo de sesión
            os.replace(temp_filename, filename)
            
        except Exception as e:
            logger.error(f"Error guardando sesión {session_id}: {e}")
        

    def delete_session(self, session_id):
        """Borra el archivo cuando la sesión termina para no llenar el disco."""
        filename = os.path.join(self.base_dir, f"{session_id}.pkl")
        try:
            if os.path.exists(filename):
                os.remove(filename)
        except OSError as e:
            logger.warning(f"No se pudo borrar estado de sesión {session_id}: {e}")

    def load_all_sessions(self):
        """
        Se ejecuta SOLO al inicio. Carga todos los archivos .pkl en memoria.
        Retorna: (dict_global_aggregator, dict_global_tracker)
        """
        aggregator_map = {}
        tracker_map = {}
        
        # Buscar todos los .pkl en la carpeta
        files = glob.glob(os.path.join(self.base_dir, "*.pkl"))
        logger.info(f"Recuperando {len(files)} sesiones del disco...")

        for filepath in files:
            try:
                with open(filepath, 'rb') as f:
                    state = pickle.load(f)
                    
                s_id = state['session_id']
                aggregator_map[s_id] = state['aggregator_data']
                tracker_map[s_id] = state['tracker_data']
                
            except Exception as e:
                logger.error(f"Archivo corrupto ignorado {filepath}: {e}")
        
        return aggregator_map, tracker_map
