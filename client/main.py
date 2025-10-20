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
VERBOSE = os.environ.get('CLIENT_VERBOSE', '0') == '1'

def signal_handler(signum, frame):
    """
    Maneja las señales SIGTERM e SIGINT para graceful shutdown
    """
    signal_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
    if VERBOSE:
        print(f"\nSeñal {signal_name} recibida. Iniciando cierre ordenado...")
    
    if global_client:
        global_client.request_shutdown()
    
    if VERBOSE:
        print("Cliente terminado por señal del sistema.")
    sys.exit(0)

def setup_signal_handlers():
    """Configura los manejadores de señales"""
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    if VERBOSE:
        print("Manejadores de señales configurados (SIGTERM, SIGINT)")

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
        'log_format': config.get('LOGGING', 'log_format', fallback='%(asctime)s - %(levelname)s - %(message)s')
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
    
    if VERBOSE:
        print("\nOrden de envío optimizado:")
        for i, f in enumerate(sorted_files, 1):
            print(f"  {i}. {os.path.basename(f)}")
    
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
    """
    Thread que lee archivos CSV y coloca los batches en la cola.
    """
    if VERBOSE:
        print("Hilo lector iniciado")
    
    try:
        for i, csv_file_path in enumerate(csv_files, 1):
            if stop_event.is_set():
                if VERBOSE:
                    print(f"  Hilo lector detenido. Archivos procesados: {i-1}/{len(csv_files)}")
                break
                
            if VERBOSE:
                print(f"  [{i}/{len(csv_files)}] Leyendo: {os.path.basename(csv_file_path)}")
            
            # Detectar tipo de entidad desde el nombre del archivo
            try:
                entity_type = detect_entity_type_from_filename(os.path.basename(csv_file_path))
            except ValueError:
                entity_type = None
            
            batch_count = 0
            
            try:
                for batch in entity_batch_iterator(csv_file_path, batch_size, entity_type):
                    if stop_event.is_set():
                        print(f"    Detención solicitada durante lectura de {os.path.basename(csv_file_path)}")
                        break
                    
                    # Enviar batch a la cola
                    data_queue.put(('batch', batch, csv_file_path))
                    batch_count += 1
                    # Solo log cada 100 batches
                    if VERBOSE and batch_count % 100 == 0:
                        print(f"    Batches leídos: {batch_count}, entidades en último: {len(batch)}")
                
                if not stop_event.is_set() and VERBOSE:
                    print(f"    Completada lectura: {batch_count} batches")
                
            except Exception as e:
                print(f"    Error leyendo archivo: {e}")
                data_queue.put(('error', str(e), csv_file_path))
                continue
        
        # fin de datos
        data_queue.put(('end', None, None))
        if VERBOSE:
            print("Hilo lector terminado")
        
    except Exception as e:
        print(f"Error crítico en hilo lector: {e}")
        data_queue.put(('error', str(e), None))

def sender_thread(client: Client, data_queue: queue.Queue, stop_event: threading.Event, error_queue: queue.Queue = None):
    """
    Thread que envía los batches al servidor.
    """
    if VERBOSE:
        print("Hilo enviador iniciado")
    
    total_batches_sent = 0
    total_entities_sent = 0
    
    try:
        while not stop_event.is_set():
            try:
                # Esperar por datos con timeout
                item = data_queue.get(timeout=1.0)
                
                if item[0] == 'end':
                    if VERBOSE:
                        print("Fin de datos recibido")
                    break
                elif item[0] == 'error':
                    print(f"Error recibido del lector: {item[1]}")
                    continue
                elif item[0] == 'batch':
                    _, batch, source_file = item
                    
                    if client.is_shutdown_requested():
                        if VERBOSE:
                            print("Cierre solicitado durante envío")
                        break
                    
                    client.send_batch(batch)
                    total_batches_sent += 1
                    total_entities_sent += len(batch)
                    # Solo log cada 100 batches
                    if VERBOSE and total_batches_sent % 100 == 0:
                        print(f"Enviados {total_batches_sent} batches, entidades en último: {len(batch)}")
                    
                    data_queue.task_done()
                
            except queue.Empty:
                # Timeout normal, continuar verificando stop_event
                continue
            except ConnectionRefusedError as e:
                # Conexión rechazada por el servidor - detener inmediatamente
                print(f"\n❌ Conexión rechazada: {e}")
                stop_event.set()
                if error_queue:
                    error_queue.put(('connection_refused', e))
                return
            except Exception as e:
                if "proceso de cierre" in str(e):
                    print(f"Cierre ordenado durante envío: {e}")
                    break
                elif "REJECTED" in str(e):
                    # Error de rechazo - detener inmediatamente
                    print(f"\n❌ {e}")
                    stop_event.set()
                    if error_queue:
                        error_queue.put(('connection_refused', ConnectionRefusedError(str(e))))
                    return
                else:
                    print(f"Error enviando batch: {e}")
                    # En caso de error de envío, seguir intentando con los siguientes
                    continue
        
        if VERBOSE:
            print(f"Hilo enviador terminado. Total enviado: {total_batches_sent} batches, {total_entities_sent} entidades")
        
    except ConnectionRefusedError as e:
        # Propagar el error de rechazo a través de la cola
        if error_queue:
            error_queue.put(('connection_refused', e))
    except Exception as e:
        print(f"Error crítico en hilo enviador: {e}")
        if error_queue:
            error_queue.put(('error', e))

def read_and_send_threaded(csv_files: List[str], batch_size: int, client: Client):
    """
    Coordina la lectura y envío usando dos threads separados.
    """
    if VERBOSE:
        print(f"\nIniciando procesamiento con threading para {len(csv_files)} archivos")
    
    # Cola para comunicar entre threads
    data_queue = queue.Queue(maxsize=50)  # Limitar tamaño para evitar usar mucha memoria
    error_queue = queue.Queue()  # Cola para errores de los threads
    stop_event = threading.Event()
    
    # Crear threads
    reader_thread = threading.Thread(
        target=csv_reader_thread, 
        args=(csv_files, batch_size, data_queue, stop_event),
        name="CSVReader"
    )
    
    sender_thread_obj = threading.Thread(
        target=sender_thread,
        args=(client, data_queue, stop_event, error_queue),
        name="DataSender"
    )
    
    try:
        # Iniciar threads
        reader_thread.start()
        sender_thread_obj.start()
        
        # Esperar a que terminen
        reader_thread.join()
        sender_thread_obj.join()
        
        # Verificar si hubo errores en los threads
        if not error_queue.empty():
            error_type, error = error_queue.get()
            if error_type == 'connection_refused':
                raise error
            else:
                raise error
        
        if VERBOSE:
            print("Procesamiento threaded completado")
        
    except KeyboardInterrupt:
        if VERBOSE:
            print("\nInterrupción detectada, cerrando threads...")
        stop_event.set()
        client.request_shutdown()
        
        # Esperar a que los threads terminen
        reader_thread.join(timeout=5)
        sender_thread_obj.join(timeout=5)
        
        if VERBOSE:
            print("Threads cerrados por interrupción")
        raise
    
    except Exception as e:
        print(f"Error en procesamiento threaded: {e}")
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
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Activar modo verbose')
    
    args = parser.parse_args()
    
    # Configurar verbose global
    global VERBOSE
    VERBOSE = args.verbose or os.environ.get('CLIENT_VERBOSE', '0') == '1'
    
    try:
        # Configurar manejadores de señales
        setup_signal_handlers()
        
        # Cargar configuración
        config = load_config(args.config, args.data_folder)
        
        dataset_path = config['dataset_path']
        host = config['host']
        port = config['port']
        batch_size = config['batch_size']
        
        if VERBOSE:
            print(f"Configuración cargada:")
            print(f"  Dataset: {dataset_path}")
            print(f"  Servidor: {host}:{port}")
            print(f"  Tamaño de batch: {batch_size}")
        
        # Encontrar todos los archivos CSV
        csv_files = find_csv_files(dataset_path)
        if VERBOSE:
            print(f"\nEncontrados {len(csv_files)} archivo(s) CSV para procesar:")
            for csv_file in csv_files:
                print(f"  - {os.path.basename(csv_file)}")
        
        # Usar la clase Client con context manager
        start_time = time.time()
        with Client(host, port) as client:
            global_client = client  # Asignar para signal handler
            if VERBOSE:
                print(f"\nConectado al servidor en {host}:{port}")
            
            # Procesar archivos CSV usando threading
            read_and_send_threaded(csv_files, batch_size, client)
            
            # Opcional: recibir respuesta final del servidor (solo si no hay cierre pendiente)
            if not client.is_shutdown_requested():
                try:
                    if VERBOSE:
                        print("\nEsperando respuesta del servidor...")
                    response = client.receive_response()
                    elapsed = time.time() - start_time
                    print(f"Tiempo total desde envío hasta confirmación: {elapsed:.2f} segundos")
                    if VERBOSE:
                        print(f"Respuesta del servidor: {response}")
                except Exception as e:
                    print(f"No se pudo recibir respuesta del servidor: {e}")
            else:
                if VERBOSE:
                    print("\nOmitiendo recepción de respuesta debido a cierre solicitado")
        
        global_client = None
        if VERBOSE:
            print("\nCliente terminó el procesamiento.")
        
    except KeyboardInterrupt:
        print(f"\nInterrupción por teclado (Ctrl+C)")
        return 1
    except ConnectionRefusedError as e:
        # Error específico cuando el servidor rechaza la conexión por estar lleno
        print(f"\n❌ Conexión rechazada por el servidor:")
        print(f"   {e}")
        print(f"\n   El servidor ha alcanzado su límite de clientes concurrentes.")
        print(f"   Por favor, intenta nuevamente más tarde.")
        return 1
    except (FileNotFoundError, ValueError) as e:
        print(f"Error de configuración: {e}")
        return 1
    except ConnectionError as e:
        print(f"Error de conexión: {e}")
        return 1
    except Exception as e:
        print(f"Error inesperado: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    main()