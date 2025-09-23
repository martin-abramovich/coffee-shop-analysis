from common.client import Client
from common.protocol import entity_batch_iterator, detect_entity_type_from_filename
import os
import glob
import configparser
import signal
import sys

# Variable global para el cliente (necesario para signal handler)
global_client = None

def signal_handler(signum, frame):
    """
    Maneja las señales SIGTERM e SIGINT para graceful shutdown
    """
    signal_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
    print(f"\n⚠️  Señal {signal_name} recibida. Iniciando cierre ordenado...")
    
    if global_client:
        global_client.request_shutdown()
    
    # Dar tiempo para cerrar la conexión
    import time
    time.sleep(1)
    
    print("👋 Cliente terminado por señal del sistema.")
    sys.exit(0)

def setup_signal_handlers():
    """Configura los manejadores de señales"""
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    print("🛡️  Manejadores de señales configurados (SIGTERM, SIGINT)")

def load_config(config_path="config.ini"):
    """
    Carga la configuración desde el archivo config.ini
    """
    config = configparser.ConfigParser()
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Archivo de configuración no encontrado: {config_path}")
    
    config.read(config_path)
    
    # Validar que existan las secciones necesarias
    if 'CLIENT' not in config:
        raise ValueError("Sección [CLIENT] no encontrada en el archivo de configuración")
    
    return {
        'dataset_path': config.get('CLIENT', 'dataset_path', fallback='./datasets/'),
        'host': config.get('CLIENT', 'host', fallback='localhost'),
        'port': config.getint('CLIENT', 'port', fallback=9000),
        'batch_size': config.getint('CLIENT', 'batch_size', fallback=100),
        'stream_id': config.get('CLIENT', 'stream_id', fallback='report-123'),
        'log_level': config.get('LOGGING', 'log_level', fallback='INFO'),
        'log_format': config.get('LOGGING', 'log_format', fallback='%(asctime)s - %(levelname)s - %(message)s')
    }

def find_csv_files(dataset_path):
    """
    Encuentra todos los archivos CSV en el directorio especificado.
    Si dataset_path es un archivo, devuelve una lista con ese archivo.
    Si es un directorio, devuelve todos los archivos .csv en ese directorio.
    """
    if os.path.isfile(dataset_path):
        if dataset_path.endswith('.csv'):
            return [dataset_path]
        else:
            raise ValueError(f"El archivo {dataset_path} no es un archivo CSV")
    elif os.path.isdir(dataset_path):
        csv_files = glob.glob(os.path.join(dataset_path, "*.csv"))
        if not csv_files:
            raise ValueError(f"No se encontraron archivos CSV en el directorio {dataset_path}")
        return sorted(csv_files)  # Ordenar para procesamiento consistente
    else:
        raise ValueError(f"La ruta {dataset_path} no existe o no es válida")

def read_and_send(csv_file_path: str, batch_size: int, client: Client):
    """
    Lee un archivo CSV y envía los datos en batches usando el protocolo binario.
    """
    # Detectar tipo de entidad desde el nombre del archivo
    try:
        entity_type = detect_entity_type_from_filename(os.path.basename(csv_file_path))
        print(f"  Tipo detectado: {entity_type}")
    except ValueError as e:
        print(f"  Error detectando tipo: {e}")
        print(f"  Intentando detectar desde cabeceras...")
        entity_type = None  # Se detectará automáticamente
    
    batch_count = 0
    total_entities = 0
    
    try:
        for batch in entity_batch_iterator(csv_file_path, batch_size, entity_type):
            # Verificar si se solicitó el cierre
            if client.is_shutdown_requested():
                print(f"  ⚠️  Cierre solicitado durante procesamiento de {os.path.basename(csv_file_path)}")
                print(f"  📊 Progreso: {batch_count} batches, {total_entities} entidades enviadas")
                return
            
            client.send_batch(batch)
            batch_count += 1
            total_entities += len(batch)
            print(f"  Enviado batch {batch_count}, entidades: {len(batch)}")
        
        print(f"  ✓ Completado: {batch_count} batches, {total_entities} entidades totales")
        
    except RuntimeError as e:
        if "proceso de cierre" in str(e):
            print(f"  ⚠️  Cierre ordenado durante envío: {e}")
            print(f"  📊 Progreso: {batch_count} batches, {total_entities} entidades enviadas")
        else:
            print(f"  ✗ Error de runtime: {e}")
            raise
    except Exception as e:
        print(f"  ✗ Error procesando archivo: {e}")
        raise


def main():
    global global_client
    
    try:
        # Configurar manejadores de señales
        setup_signal_handlers()
        
        # Cargar configuración
        config = load_config()
        
        dataset_path = config['dataset_path']
        host = config['host']
        port = config['port']
        batch_size = config['batch_size']
        
        print(f"Configuración cargada:")
        print(f"  Dataset: {dataset_path}")
        print(f"  Servidor: {host}:{port}")
        print(f"  Tamaño de batch: {batch_size}")
        
        # Encontrar todos los archivos CSV
        csv_files = find_csv_files(dataset_path)
        print(f"\nEncontrados {len(csv_files)} archivo(s) CSV para procesar:")
        for csv_file in csv_files:
            print(f"  - {os.path.basename(csv_file)}")
        
        # Usar la clase Client con context manager
        with Client(host, port) as client:
            global_client = client  # Asignar para signal handler
            print(f"\n✓ Conectado al servidor en {host}:{port}")
            
            # Procesar cada archivo CSV
            for i, csv_file in enumerate(csv_files, 1):
                # Verificar si se solicitó el cierre
                if client.is_shutdown_requested():
                    print(f"\n⚠️  Cierre solicitado. Archivos procesados: {i-1}/{len(csv_files)}")
                    break
                    
                print(f"\n[{i}/{len(csv_files)}] Procesando: {os.path.basename(csv_file)}")
                read_and_send(csv_file, batch_size, client)
            
            # Opcional: recibir respuesta final del servidor (solo si no hay cierre pendiente)
            if not client.is_shutdown_requested():
                try:
                    print("\nEsperando respuesta del servidor...")
                    response = client.receive_response()
                    print(f"Respuesta del servidor: {response}")
                except Exception as e:
                    print(f"No se pudo recibir respuesta del servidor: {e}")
            else:
                print("\n⚠️  Omitiendo recepción de respuesta debido a cierre solicitado")
        
        global_client = None  # Limpiar referencia
        print("\n✓ Cliente terminó el procesamiento.")
        
    except KeyboardInterrupt:
        print(f"\n⚠️  Interrupción por teclado (Ctrl+C)")
        return 1
    except (FileNotFoundError, ValueError) as e:
        print(f"✗ Error de configuración: {e}")
        return 1
    except ConnectionError as e:
        print(f"✗ Error de conexión: {e}")
        return 1
    except Exception as e:
        print(f"✗ Error inesperado: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    main()