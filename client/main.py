import argparse
import logging
import json
import csv
from pathlib import Path
from typing import Optional
from common.client import Client
from common.protocol import batch_eos, entity_batch_iterator
import os
import configparser
import signal
import sys
import threading
import queue
import time

# Variable global para el cliente (necesario para signal handler)
DEFAULT_RESULTS_DIR = Path(os.environ.get("CLIENT_RESULTS_DIR", "./results"))

def setup_logging(level: str):
    """Configura el logger global"""
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    
def signal_handler(signum, frame, stop_event: threading.Event):
    """
    Maneja las señales SIGTERM e SIGINT para graceful shutdown
    """
    signal_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
    logging.warning(f"Señal {signal_name} recibida. Iniciando cierre ordenado...")
    
    stop_event.set()
    

def setup_signal_handlers(stop_event: threading.Event):
    """Configura los manejadores de señales"""
    signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, stop_event))
    signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, stop_event)) # Ctrl+C
    
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

def store_results_locally(response_text: str, output_root: Optional[Path] = None):
    """
    Genera los archivos CSV de resultados usando la respuesta JSON enviada por el gateway.
    Si la respuesta no es JSON (compatibilidad hacia atrás), solo se loguea el contenido.
    """
    try:
        payload = json.loads(response_text)
    except json.JSONDecodeError:
        logging.info("Respuesta del servidor: %s", response_text)
        return

    results = payload.get("results") or {}
    session_id = payload.get("session_id", "unknown")

    if not results:
        logging.warning("Respuesta recibida sin resultados para la sesión %s.", session_id)
        return

    base_dir = output_root or DEFAULT_RESULTS_DIR
    output_dir = base_dir / f"session_{session_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    for query_name, data in results.items():
        rows = data.get("rows") or []
        if not rows:
            logging.warning("No hay filas para %s; se omite la creación de CSV.", query_name)
            continue

        columns = data.get("columns")
        if not columns:
            columns = list(rows[0].keys())

        file_path = output_dir / f"{query_name}.csv"
        with open(file_path, "w", newline='', encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=columns)
            writer.writeheader()
            for row in rows:
                writer.writerow({col: row.get(col, '') for col in columns})

        logging.info("Resultados de %s guardados en %s (%d filas)", query_name, file_path, len(rows))

    missing = payload.get("missing_results") or []
    if missing:
        logging.warning("Queries sin resultado recibido: %s", ", ".join(missing))

def csv_reader_users_thread(base_path: str, batch_size: int, data_queue: queue.Queue, stop_event: threading.Event):
    logging.debug("Hilo lector iniciado")
    batch_count = 0
    batch_id = [0] 
    try:
        base_path = Path(base_path)

        folder_name = "users"
        entity_type = "users"
          
        folder_path = base_path / folder_name
        if not folder_path.exists() or not folder_path.is_dir():
            logging.warning(f"Advertencia: No existe la carpeta {folder_path}")
        
        else:
            # Iterar todos los CSV dentro de la carpeta
            for csv_file in folder_path.glob("*.csv"):
                if stop_event.is_set():
                    break
                
                # Llamamos a entity_batch_iterator para cada CSV
                logging.debug(f"Leyendo: {csv_file}")
                for batch in entity_batch_iterator(str(csv_file), batch_size, entity_type, batch_id):
                    if stop_event.is_set():
                        break
                    
                    data_queue.put(('batch', batch))
                    batch_count += 1
                    batch_id[0] += 1 
                    
                    if batch_count <= 3 or batch_count % 10000 == 0:
                        logging.debug(f"Batches leídos: {batch_count}, entidades en último: {len(batch)}")
                        
            batch = batch_eos(entity_type, batch_id)
            data_queue.put(('batch', batch))
            logging.debug(f"Batch EOS para {entity_type}")
            
                
        
        data_queue.put(('end', None, None))
        logging.debug("Hilo lector terminado")
        
        
    except Exception as e:
        logging.error(f"Error crítico en hilo lector: {e}", exc_info=True)
        data_queue.put(('error', str(e), None))
        
def csv_reader_transacctions_thread(base_path: str, batch_size: int, data_queue: queue.Queue, stop_event: threading.Event):
    logging.debug("Hilo lector iniciado")
    batch_count = 0
    batch_id = [0] 
    try:
        base_path = Path(base_path)

        folder_name = "transaction_items"
        entity_type = "transaction_items"
          
        folder_path = base_path / folder_name
        if not folder_path.exists() or not folder_path.is_dir():
            logging.warning(f"Advertencia: No existe la carpeta {folder_path}")
        
        else:
            # Iterar todos los CSV dentro de la carpeta
            for csv_file in folder_path.glob("*.csv"):
                if stop_event.is_set():
                    break
                
                # Llamamos a entity_batch_iterator para cada CSV
                logging.debug(f"Leyendo: {csv_file}")
                for batch in entity_batch_iterator(str(csv_file), batch_size, entity_type, batch_id):
                    if stop_event.is_set():
                        break
                    
                    data_queue.put(('batch', batch))
                    batch_count += 1
                    batch_id[0] += 1
                    
                    if batch_count <= 3 or batch_count % 10000 == 0:
                        logging.debug(f"Batches leídos: {batch_count}, entidades en último: {len(batch)}")
                        
            batch = batch_eos(entity_type, batch_id)
            data_queue.put(('batch', batch))
            logging.debug(f"Batch EOS para {entity_type}")
            
                
        
        data_queue.put(('end', None, None))
        logging.debug("Hilo lector terminado")
        
        
    except Exception as e:
        logging.error(f"Error crítico en hilo lector: {e}", exc_info=True)
        data_queue.put(('error', str(e), None))
        
def csv_reader_thread(base_path: str, batch_size: int, data_queue: queue.Queue, stop_event: threading.Event):
    logging.debug("Hilo lector iniciado")
    batch_count = 0
    try:
        base_path = Path(base_path)

        # Mapeo de carpetas a tipos de entidad
        folder_to_entity = {
            "menu_items": "menu_items",
            "stores": "stores",
            "transactions": "transactions",
        }

        for folder_name, entity_type in folder_to_entity.items():
            if stop_event.is_set():
                logging.debug(f"Hilo lector detenido.")
                break
            
            folder_path = base_path / folder_name
            if not folder_path.exists() or not folder_path.is_dir():
                logging.warning(f"Advertencia: No existe la carpeta {folder_path}")
                continue
            
            batch_id = [0] 
            
            # Iterar todos los CSV dentro de la carpeta
            for csv_file in folder_path.glob("*.csv"):
                if stop_event.is_set():
                    break
                
                # Llamamos a entity_batch_iterator para cada CSV
                logging.debug(f"Leyendo: {csv_file}")
                for batch in entity_batch_iterator(str(csv_file), batch_size, entity_type, batch_id):
                    if stop_event.is_set():
                        break
                    
                    data_queue.put(('batch', batch))
                    batch_count += 1
                    batch_id[0] += 1
                    
                    if batch_count <= 3 or batch_count % 10000 == 0:
                        logging.debug(f"Batches leídos: {batch_count}, entidades en último: {len(batch)}")
            
            batch = batch_eos(entity_type, batch_id)
            data_queue.put(('batch', batch))
            logging.debug(f"Batch EOS para {entity_type}")
                
        
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
    count_read_threads = 0 
    try:
        while True:
            try:
                # Esperar por datos con timeout
                item = data_queue.get(timeout=1.0)
                
                if item[0] == 'end':
                    count_read_threads += 1
                    if count_read_threads >= 3: 
                        logging.debug("Fin de datos recibidos del hilo lector")
                        break
                elif item[0] == 'error':
                    logging.error(f"Error recibido del lector: {item[1]}")
                    continue
                elif item[0] == 'batch':
                    _, batch = item
                    
                    # Intentar enviar el batch
                    # send_batch() retorna False si se canceló, True si se envió, o lanza excepción si falló
                    sent = client.send_batch(batch)
                    
                    if sent:
                        total_batches_sent += 1
                        total_entities_sent += len(batch)
                        
                        if total_batches_sent <=  3 or total_batches_sent % 10000 == 0:
                            logging.debug(f"Enviados {total_batches_sent} batches, entidades en último: {len(batch)}")
                    else:
                        # send_batch retornó False: cancelado por stop_event
                        logging.debug("Envío cancelado por stop_event, finalizando...")
                        break
                    
                    data_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logging.debug(f"Error enviando batch: {e}", exc_info=True)
                if isinstance(e, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, ConnectionError, OSError, ConnectionResetError)):
                    logging.error("La conexión con el gateway se cerró; deteniendo envío.")
                    stop_event.set()
                continue
        logging.info(f"Hilo enviador terminado. Total enviado: {total_batches_sent} batches, {total_entities_sent} entidades")
    except Exception as e:
        logging.error(f"Error crítico en hilo enviador: {e}", exc_info=True)

def read_and_send_threaded(base_path: str, batch_size: int, client: Client, stop_event: threading.Event):
    """
    Coordina la lectura y envío usando dos threads separados.
    """
    logging.info(f"Iniciando procesamiento con threading para archivos")
    
    # Cola para comunicar entre threads
    data_queue = queue.Queue(maxsize=150)  # Limitar tamaño para evitar usar mucha memoria
    
    # Crear threads
    reader_thread = threading.Thread(
        target=csv_reader_thread, 
        args=(base_path, batch_size, data_queue, stop_event),
        name="CSVReader"
    )
    
    reader_trans_thread = threading.Thread(
        target=csv_reader_transacctions_thread, 
        args=(base_path, batch_size, data_queue, stop_event),
        name="CSVReaderTrans"
    )
    
    reader_users_thread = threading.Thread(
        target=csv_reader_users_thread, 
        args=(base_path, batch_size, data_queue, stop_event),
        name="CSVReaderUsers"
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
        reader_trans_thread.start()
        reader_users_thread.start()
        
        # Esperar a que terminen
        reader_thread.join()
        sender_thread_obj.join()
        reader_trans_thread.join()
        reader_users_thread.join()
        
        
        logging.info("Procesamiento threaded completado")
    except Exception as e:
        logging.error(f"Error en procesamiento threaded: {e}")
        stop_event.set()
        raise


def main():
    
    # Parsear argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Cliente para análisis de coffee shop')
    parser.add_argument('--data-folder', '-d', 
                       help='Subcarpeta dentro de .data de donde leer los archivos (ej: dataset1, dataset2)')
    parser.add_argument('--config', '-c', default='config.ini',
                       help='Archivo de configuración (default: config.ini)')
    
    args = parser.parse_args()
    stop_event = threading.Event()
        
    try:
        # Cargar configuración
        config = load_config(args.config, args.data_folder)
        
        #config logging
        setup_logging(config['log_level'])
        
        # Configurar manejadores de señales
        setup_signal_handlers(stop_event)
        
        dataset_path = config['dataset_path']
        host = config['host']
        port = config['port']
        batch_size = config['batch_size']
        
        logging.info(
            f"Configuración cargada: dataset={dataset_path}, servidor={host}:{port}, batch_size={batch_size}")
        
        start_time = time.time()
        with Client(host, port, stop_event) as client:
            logging.info(f"Iniciando cliente. Conectado al servidor en {host}:{port}")
            
            # Procesar archivos CSV usando threading
            read_and_send_threaded(dataset_path, batch_size, client, stop_event)
            
            # Opcional: recibir respuesta final del servidor (solo si no hay cierre pendiente)
            if not stop_event.is_set():
                try:
                    logging.info("Esperando respuesta del servidor...")
                    response = client.receive_response()
                    elapsed = time.time() - start_time
                    logging.info(f"Tiempo total desde envío hasta confirmación: {elapsed:.2f}s")
                    store_results_locally(response)
                    
                except RuntimeError as e:
                    # RuntimeError es lanzado cuando el usuario cancela con Ctrl+C
                    if "detenido por usuario" in str(e):
                        logging.info("Recepción de respuesta cancelada por el usuario")
                    else:
                        logging.error(f"Error: {e}")
                except Exception as e:
                   logging.error(f"No se pudo recibir respuesta del servidor: {e}", exc_info=True)
            else:
                logging.info("Omitiendo recepción de respuesta por cierre solicitado")
        
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