import pickle
import os
import logging
import glob
import shutil

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
        
        state = {
            'data': aggregator_data,
            'tracker': tracker_data     
        }
        
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
        Retorna: (aggregator_map, tracker_map)
        Estructura aggregator_map: { session_id: { 'transactions': [...], 'stores': [...] } }
        """
        aggregator_map = {}
        tracker_map = {}
        
        if not os.path.exists(self.base_dir):
            return aggregator_map, tracker_map

        # Listar todas las carpetas de sesiones
        session_dirs = [d for d in os.listdir(self.base_dir) if os.path.isdir(os.path.join(self.base_dir, d))]
        
        logger.info(f"Recuperando {len(session_dirs)} sesiones del disco...")

        for s_id in session_dirs:
            s_path = os.path.join(self.base_dir, s_id)
            
            # Inicializar diccionarios para esta sesión
            if s_id not in aggregator_map: aggregator_map[s_id] = {}
            if s_id not in tracker_map: tracker_map[s_id] = {}

            # Leer cada archivo de tipo dentro de la carpeta de sesión
            for file in os.listdir(s_path):
                if file.endswith(".pkl") and not file.endswith(".tmp"):
                    try:
                        filepath = os.path.join(s_path, file)
                        with open(filepath, 'rb') as f:
                            state = pickle.load(f)
                        
                        data_type_key = os.path.splitext(file)[0]
                        
                        # Reconstruir memoria
                        # aggregator_map[session][tipo] = datos
                        aggregator_map[s_id][data_type_key] = state['data'] 
                        
                        # tracker_map[session][tipo] = info_tracker
                        tracker_map[s_id][data_type_key] = state['tracker']
                        
                    except Exception as e:
                        logger.error(f"Archivo corrupto {filepath}: {e}")
        
        return aggregator_map, tracker_map

    # def load_all_sessions(self):
    #     """
    #     Se ejecuta SOLO al inicio. Carga todos los archivos .pkl en memoria.
    #     Retorna: (dict_global_aggregator, dict_global_tracker)
    #     """
    #     aggregator_map = {}
    #     tracker_map = {}
        
    #     # Buscar todos los .pkl en la carpeta
    #     files = glob.glob(os.path.join(self.base_dir, "*.pkl"))
    #     logger.info(f"Recuperando {len(files)} sesiones del disco...")

    #     for filepath in files:
    #         try:
    #             with open(filepath, 'rb') as f:
    #                 state = pickle.load(f)
                    
    #             s_id = state['session_id']
    #             aggregator_map[s_id] = state['aggregator_data']
    #             tracker_map[s_id] = state['tracker_data']
                
    #         except Exception as e:
    #             logger.error(f"Archivo corrupto ignorado {filepath}: {e}")
        
    #     return aggregator_map, tracker_map