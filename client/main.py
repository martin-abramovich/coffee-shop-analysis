import logging
from common.client import Client
from common.protocol import entity_batch_iterator, detect_entity_type_from_filename
import os
import glob
import configparser
import signal
import sys
import threading
import queue
import time
from typing import List, Tuple

# Variable global para el cliente (necesario para signal handler)
global_client = None

def setup_logging(level: str):
    """Configura el logger global"""
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    
def signal_handler(signum, frame):
    """
    Maneja las señales SIGTERM e SIGINT para graceful shutdown
    """
    signal_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
    logging.info(f"\nSeñal {signal_name} recibida. Iniciando cierre ordenado...")
    
    if global_client:
        global_client.request_shutdown()
    
    logging.info("Cliente terminado por señal del sistema.")
    sys.exit(0)

def setup_signal_handlers():
    """Configura los manejadores de señales"""
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    
    logging.info("Manejadores de señales configurados (SIGTERM, SIGINT)")

def load_config(config_path="config.ini", data_subfolder=None):
    """
    Carga la configuración desde el archivo config.ini
    """
    config = configparser.ConfigParser(interpolation=None)
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")
    
    config.read(config_path)
    
    # Validar que existan las secciones necesarias
    if 'CLIENT' not in config:
        raise ValueError("Sección [CLIENT] no encontrada en el archivo de configuración")
    
    # Construir la ruta del dataset
    base_dataset_path = config.get('CLIENT', 'dataset_path', fallback='./datasets/')
    if data_subfolder:
        # Si se especifica una subcarpeta, usarla
        dataset_path = os.path.join(base_dataset_path, data_subfolder)
    else:
        # Usar la ruta base (comportamiento original)
        dataset_path = base_dataset_path
    
    return {
        'dataset_path': dataset_path,
        'host': config.get('CLIENT', 'host', fallback='localhost'),
        'port': config.getint('CLIENT', 'port', fallback=9000),
        'batch_size': config.getint('CLIENT', 'batch_size', fallback=100),
        'stream_id': config.get('CLIENT', 'stream_id', fallback='report-123'),
        'log_level': config.get('LOGGING', 'log_level', fallback='INFO'),
    }

def smart_sort_csv_files(csv_files: List[str]) -> List[str]:
    """
    Ordena los archivos CSV para optimizar el procesamiento de queries.
    
    Orden óptimo basado en dependencias:
    - Query 1: transactions
    - Query 2: transaction_items + menu_items
    - Query 3: transaction_items + stores
    - Query 4: transactions + users + stores
    
    Estrategia: Enviar primero metadata pequeña, luego transactions para que
    Query 1 y 4 empiecen inmediatamente, finalmente transaction_items.
    """
    priority_order = [
        'menu_items',        # 1. Pequeño, necesario para Q2
        'stores',            # 2. Pequeño, necesario para Q3 y Q4
        'users',             # 3. Pequeño, necesario para Q4
        'transactions',      # 4. Grande, pero Q1 y Q4 NECESITAN empezar YA
        'transaction_items'  # 5. El más grande, pero Q2 y Q3 ya tienen metadata
    ]
    
    def get_priority(filepath):
        basename = os.path.basename(filepath).lower()
        for i, keyword in enumerate(priority_order):
            if keyword in basename:
                return i
        # Si no coincide con ningún patrón conocido, poner al final
        return 999
    
    sorted_files = sorted(csv_files, key=get_priority)
    
    return sorted_files

def find_csv_files_in_data_structure(data_path: str) -> List[str]:
    """
    Encuentra todos los archivos CSV en la estructura de datos de .data/
    Busca recursivamente en todas las subcarpetas.
    """
    csv_files = []
    
    if not os.path.exists(data_path):
        raise ValueError(f"La ruta de datos {data_path} no existe")
    
    if not os.path.isdir(data_path):
        raise ValueError(f"La ruta {data_path} no es un directorio")
    
    # Buscar recursivamente en todas las subcarpetas
    for root, dirs, files in os.walk(data_path):
        # Saltar archivos ZIP
        if any(f.endswith('.zip') for f in files):
            continue
            
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    
    if not csv_files:
        raise ValueError(f"No se encontraron archivos CSV en {data_path}")
    
    # Ordenar con algoritmo inteligente basado en dependencias de queries
    return smart_sort_csv_files(csv_files)

def find_csv_files(dataset_path):
    """
    Encuentra todos los archivos CSV. Mantiene compatibilidad con versión anterior
    pero prioriza la estructura de .data/
    """
    if os.path.isfile(dataset_path):
        if dataset_path.endswith('.csv'):
            return [dataset_path]
        else:
            raise ValueError(f"El archivo {dataset_path} no es un archivo CSV")
    elif os.path.isdir(dataset_path):
        # Si es la carpeta .data, usar nueva función
        if dataset_path.endswith('.data') or '.data' in dataset_path:
            return find_csv_files_in_data_structure(dataset_path)
        else:
            csv_files = glob.glob(os.path.join(dataset_path, "*.csv"))
            if not csv_files:
                raise ValueError(f"No se encontraron archivos CSV en el directorio {dataset_path}")
            return smart_sort_csv_files(csv_files)
    else:
        raise ValueError(f"La ruta {dataset_path} no existe o no es válida")

def csv_reader_thread(csv_files: List[str], batch_size: int, data_queue: queue.Queue, stop_event: threading.Event):
    logging.debug("Hilo lector iniciado")
    
    try:
        for i, csv_file_path in enumerate(csv_files, 1):
            if stop_event.is_set():
                logging.debug(f"Hilo lector detenido. Archivos procesados: {i-1}/{len(csv_files)}")
                break
                
            logging.debug(f"[{i}/{len(csv_files)}] Leyendo: {os.path.basename(csv_file_path)}")
            try:
                entity_type = detect_entity_type_from_filename(os.path.basename(csv_file_path))
            except ValueError:
                entity_type = None
            
            batch_count = 0
            for batch in entity_batch_iterator(csv_file_path, batch_size, entity_type):
                if stop_event.is_set():
                    logging.warning(f"Detención solicitada durante lectura de {os.path.basename(csv_file_path)}")
                    break
                
                data_queue.put(('batch', batch, csv_file_path))
                batch_count += 1
                
                if batch_count % 100 == 0:
                    logging.debug(f"Batches leídos: {batch_count}, entidades en último: {len(batch)}")

            logging.debug(f"Completada lectura: {batch_count} batches")

        data_queue.put(('end', None, None))
        logging.debug("Hilo lector terminado")
        
    except Exception as e:
        logging.error(f"Error crítico en hilo lector: {e}", exc_info=True)
        data_queue.put(('error', str(e), None))

def sender_thread(client: Client, data_queue: queue.Queue, stop_event: threading.Event):
    """
    Thread que envía los batches al servidor.
    """
    logging.debug("Hilo sender iniciado")
    
    total_batches_sent = 0
    total_entities_sent = 0
    
    try:
        while not stop_event.is_set():
            try:
                # Esperar por datos con timeout
                item = data_queue.get(timeout=1.0)
                
                if item[0] == 'end':
                    logging.debug("Fin de datos recibidos del hilo lector")
                    break
                elif item[0] == 'error':
                    logging.error(f"Error recibido del lector: {item[1]}")
                    continue
                elif item[0] == 'batch':
                    _, batch, _ = item
                    
                    if client.is_shutdown_requested():
                        logging.warning("Cierre solicitado durante envío")
                        break
                    
                    client.send_batch(batch)
                    total_batches_sent += 1
                    total_entities_sent += len(batch)
                    
                    if total_batches_sent % 100 == 0:
                        logging.debug(f"Enviados {total_batches_sent} batches, entidades en último: {len(batch)}")
                    
                    data_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Error enviando batch: {e}", exc_info=True)
                continue
        logging.info(f"Hilo enviador terminado. Total enviado: {total_batches_sent} batches, {total_entities_sent} entidades")
    except Exception as e:
        logging.error(f"Error crítico en hilo enviador: {e}", exc_info=True)

def read_and_send_threaded(csv_files: List[str], batch_size: int, client: Client):
    """
    Coordina la lectura y envío usando dos threads separados.
    """
    logging.info(f"Iniciando procesamiento con threading para {len(csv_files)} archivos")
    
    # Cola para comunicar entre threads
    data_queue = queue.Queue(maxsize=50)  # Limitar tamaño para evitar usar mucha memoria
    stop_event = threading.Event()
    
    # Crear threads
    reader_thread = threading.Thread(
        target=csv_reader_thread, 
        args=(csv_files, batch_size, data_queue, stop_event),
        name="CSVReader"
    )
    
    sender_thread_obj = threading.Thread(
        target=sender_thread,
        args=(client, data_queue, stop_event),
        name="DataSender"
    )
    
    try:
        # Iniciar threads
        reader_thread.start()
        sender_thread_obj.start()
        
        # Esperar a que terminen
        reader_thread.join()
        sender_thread_obj.join()
        
        logging.info("Procesamiento threaded completado")
        
    except KeyboardInterrupt:
        logging.warning("\nInterrupción detectada, cerrando threads...")
        stop_event.set()
        client.request_shutdown()
        
        # Esperar a que los threads terminen
        reader_thread.join(timeout=5)
        sender_thread_obj.join(timeout=5)
        
        logging.warning("Threads cerrados por interrupción")
        raise
    
    except Exception as e:
        logging.error(f"Error en procesamiento threaded: {e}")
        stop_event.set()
        raise


def main():
    global global_client
    
    # Parsear argumentos de línea de comandos
    import argparse
    parser = argparse.ArgumentParser(description='Cliente para análisis de coffee shop')
    parser.add_argument('--data-folder', '-d', 
                       help='Subcarpeta dentro de .data de donde leer los archivos (ej: dataset1, dataset2)')
    parser.add_argument('--config', '-c', default='config.ini',
                       help='Archivo de configuración (default: config.ini)')
    
    args = parser.parse_args()
    
    
    try:
        # Cargar configuración
        config = load_config(args.config, args.data_folder)
        
        #config logging
        setup_logging(config['log_level'])
        
        # Configurar manejadores de señales
        setup_signal_handlers()
        
        dataset_path = config['dataset_path']
        host = config['host']
        port = config['port']
        batch_size = config['batch_size']
        
        logging.info(
            f"Configuración cargada: dataset={dataset_path}, servidor={host}:{port}, batch_size={batch_size}")


        # Encontrar todos los archivos CSV
        csv_files = find_csv_files(dataset_path)
        logging.debug(f"{len(csv_files)} archivos CSV encontrados para procesar")

        
        start_time = time.time()
        with Client(host, port) as client:
            global_client = client  # Asignar para signal handler
            logging.info(f"Iniciando cliente. Conectado al servidor en {host}:{port}")
            
            # Procesar archivos CSV usando threading
            read_and_send_threaded(csv_files, batch_size, client)
            
            # Opcional: recibir respuesta final del servidor (solo si no hay cierre pendiente)
            if not client.is_shutdown_requested():
                try:
                    logging.info("Esperando respuesta del servidor...")
                    response = client.receive_response()
                    elapsed = time.time() - start_time
                    logging.info(f"Tiempo total desde envío hasta confirmación: {elapsed:.2f}s")
                    logging.debug(f"Respuesta del servidor: {response}")
                    
                except Exception as e:
                   logging.error(f"No se pudo recibir respuesta del servidor: {e}", exc_info=True)
            else:
                logging.info("Omitiendo recepción de respuesta por cierre solicitado")
        
        global_client = None
        logging.info("Cliente terminó el procesamiento.")
        
    except KeyboardInterrupt:
        logging.warning("Interrupción por teclado (Ctrl+C)")
        return 1
    except (FileNotFoundError, ValueError) as e:
        logging.error(f"Error de configuración: {e}")
        return 1
    except ConnectionError as e:
        logging.error(f"Error de conexión: {e}")
        return 1
    except Exception as e:
        logging.exception(f"Error inesperado: {e}")
        return 1
    return 0

if __name__ == "__main__":
    main()