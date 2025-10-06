import sys
import os
import signal
import threading
from collections import defaultdict

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
NUM_GROUP_BY_QUERY3_WORKERS = int(os.environ.get('NUM_GROUP_BY_QUERY3_WORKERS', '2'))

INPUT_EXCHANGE = "transactions_query3"    # exchange del group_by query 3
INPUT_ROUTING_KEY = "query3"              # routing key del group_by
OUTPUT_EXCHANGE = "results_query3"        # exchange de salida para resultados finales
ROUTING_KEY = "query3_results"            # routing para resultados

# Control de EOS
eos_count = 0
eos_lock = threading.Lock()

# Exchanges adicionales para JOIN
STORES_EXCHANGE = "stores_raw"
STORES_ROUTING_KEY = "q3"

class AggregatorQuery3:
    def __init__(self):
        # Acumulador de TPV por (semestre, store_id)
        # Estructura: {(semester, store_id): total_payment_value}
        self.semester_store_tpv = defaultdict(float)
        
        # Diccionario para JOIN: store_id -> store_name
        self.store_id_to_name = {}
        
        # Control de flujo
        self.batches_received = 0
        self.stores_loaded = False
        self.eos_received = False
        self.results_sent = False  # ← AGREGADO: Flag para evitar procesamiento duplicado
        
    def load_stores(self, rows):
        """Carga stores para construir el diccionario store_id -> store_name."""
        for row in rows:
            store_id = row.get('store_id')
            store_name = row.get('store_name')
            
            if store_id and store_name:
                self.store_id_to_name[store_id] = store_name.strip()
        
        print(f"[AggregatorQuery3] Cargadas {len(self.store_id_to_name)} stores para JOIN")
    
    def accumulate_tpv(self, rows):
        """Acumula TPV parciales de group_by_query3."""
        for row in rows:
            semester = row.get('semester')
            store_id = row.get('store_id')
            total_payment_value = row.get('total_payment_value', 0.0)
            
            # Validar campos requeridos
            if not semester or not store_id:
                continue
                
            # Convertir a tipo correcto
            try:
                if isinstance(total_payment_value, str):
                    total_payment_value = float(total_payment_value)
            except (ValueError, TypeError):
                continue
            
            # Clave compuesta: (semestre, store_id)
            key = (semester, store_id)
            
            # Acumular TPV
            self.semester_store_tpv[key] += total_payment_value
        
        self.batches_received += 1
        # Log solo cada 10 batches para no saturar
        if self.batches_received % 10 == 0 or self.batches_received == 1:
            print(f"[AggregatorQuery3] Procesado batch {self.batches_received} con {len(rows)} registros. Total combinaciones: {len(self.semester_store_tpv)}")
    
    def generate_final_results(self):
        """Genera los resultados finales para Query 3 con JOIN."""
        print(f"[AggregatorQuery3] Generando resultados finales...")
        print(f"[AggregatorQuery3] Total combinaciones procesadas: {len(self.semester_store_tpv)}")
        print(f"[AggregatorQuery3] Stores disponibles para JOIN: {len(self.store_id_to_name)}")
        
        if not self.semester_store_tpv:
            print(f"[AggregatorQuery3] No hay datos para procesar")
            return []
        
        if not self.store_id_to_name:
            print(f"[AggregatorQuery3] WARNING: No hay stores cargadas. No se puede hacer JOIN.")
            return []
        
        final_results = []
        
        # Procesar cada combinación (semestre, store_id) con JOIN
        for (semester, store_id), total_tpv in self.semester_store_tpv.items():
            # JOIN: buscar store_name para el store_id
            store_name = self.store_id_to_name.get(store_id)
            
            if not store_name:
                print(f"[AggregatorQuery3] WARNING: store_id {store_id} no encontrado en stores")
                continue
            
            # Convertir formato de semestre: "2024-S1" -> "2024-H1", "2024-S2" -> "2024-H2"
            year_half = semester.replace('-S', '-H')
            
            final_results.append({
                'year_half_created_at': year_half,
                'store_name': store_name,
                'tpv': round(total_tpv, 2)  # Redondear TPV a 2 decimales
            })
        
        # Ordenar por semestre y luego por store_name para consistencia
        final_results.sort(key=lambda x: (x['year_half_created_at'], x['store_name']))
        
        print(f"[AggregatorQuery3] Resultados generados: {len(final_results)} combinaciones")
        
        # Mostrar ejemplos
        print(f"[AggregatorQuery3] Ejemplos de resultados:")
        for i, result in enumerate(final_results[:5]):
            print(f"  {i+1}. {result['year_half_created_at']} - {result['store_name']}: TPV ${result['tpv']:,.2f}")
        
        if len(final_results) > 5:
            print(f"  ... y {len(final_results) - 5} más")
        
        # Estadísticas adicionales
        total_tpv_all = sum(r['tpv'] for r in final_results)
        unique_stores = len(set(r['store_name'] for r in final_results))
        unique_semesters = len(set(r['year_half_created_at'] for r in final_results))
        
        print(f"[AggregatorQuery3] Estadísticas: {unique_semesters} semestres, {unique_stores} sucursales, TPV total: ${total_tpv_all:,.2f}")
            
        return final_results

# Instancia global del agregador
aggregator = AggregatorQuery3()

# Variable global para control de shutdown
shutdown_event = None

def on_tpv_message(body):
    """Maneja mensajes de TPV de group_by_query3."""
    # Si ya enviamos resultados, ignorar mensajes adicionales
    if aggregator.results_sent:
        return
    
    try:
        header, rows = deserialize_message(body)
    except Exception as e:
        print(f"[AggregatorQuery3] Error deserializando mensaje: {e}")
        return
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        with eos_lock:
            global eos_count
            eos_count += 1
            print(f"[AggregatorQuery3] EOS recibido en TPV ({eos_count}/{NUM_GROUP_BY_QUERY3_WORKERS})")
            
            # Solo procesar cuando recibimos de TODOS los workers
            if eos_count < NUM_GROUP_BY_QUERY3_WORKERS:
                print(f"[AggregatorQuery3] Esperando más EOS...")
                return
            
            print("[AggregatorQuery3] ✅ EOS recibido de TODOS los workers. Marcando como listo...")
            aggregator.eos_received = True
            
            # Si ya tenemos stores cargadas y no hemos enviado, generar resultados
            if aggregator.stores_loaded and not aggregator.results_sent:
                generate_and_send_results()
        return
    
    # Procesamiento normal: acumular TPV parciales
    if rows:
        aggregator.accumulate_tpv(rows)

def on_stores_message(body):
    """Maneja mensajes de stores para el JOIN."""
    # Si ya enviamos resultados, ignorar mensajes adicionales
    if aggregator.results_sent:
        return
    
    try:
        header, rows = deserialize_message(body)
    except Exception as e:
        print(f"[AggregatorQuery3] Error deserializando mensaje: {e}")
        return
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print("[AggregatorQuery3] EOS recibido en stores. Marcando como listo...")
        aggregator.stores_loaded = True
        
        # Si ya recibimos EOS de TPV y no hemos enviado, generar resultados
        if aggregator.eos_received and not aggregator.results_sent:
            generate_and_send_results()
        return
    
    # Cargar stores para JOIN
    if rows:
        aggregator.load_stores(rows)

def generate_and_send_results():
    """Genera y envía los resultados finales cuando ambos flujos terminaron."""
    global shutdown_event, mq_out
    
    # Evitar procesamiento duplicado
    if aggregator.results_sent:
        return
    
    print("[AggregatorQuery3] Ambos flujos completados. Generando resultados finales...")
    
    # Marcar como enviado ANTES de generar para evitar race conditions
    aggregator.results_sent = True
    
    # Generar resultados finales
    final_results = aggregator.generate_final_results()
    
    if final_results:
        # Enviar resultados finales con headers completos
        results_header = {
            "type": "result",
            "stream_id": "query3_results",
            "batch_id": "final",
            "is_batch_end": "true",
            "is_eos": "false",
            "query": "query3",
            "total_results": str(len(final_results)),
            "description": "TPV_por_semestre_y_sucursal_2024-2025_06:00-23:00",
            "is_final_result": "true"
        }
        
        # Enviar en batches si hay muchos resultados
        batch_size = 50
        total_batches = (len(final_results) + batch_size - 1) // batch_size
        
        for i in range(0, len(final_results), batch_size):
            batch = final_results[i:i + batch_size]
            batch_header = results_header.copy()
            batch_header["batch_number"] = str((i // batch_size) + 1)
            batch_header["total_batches"] = str(total_batches)
            
            result_msg = serialize_message(batch, batch_header)
            
            # Intentar enviar con reconexión automática si falla
            max_retries = 3
            sent = False
            for retry in range(max_retries):
                try:
                    mq_out.send(result_msg)
                    sent = True
                    print(f"[AggregatorQuery3] Enviado batch {batch_header['batch_number']}/{batch_header['total_batches']}")
                    break
                except Exception as e:
                    print(f"[AggregatorQuery3] Error enviando batch {batch_header['batch_number']} (intento {retry+1}/{max_retries}): {e}")
                    if retry < max_retries - 1:
                        try:
                            mq_out.close()
                        except:
                            pass
                        print(f"[AggregatorQuery3] Reconectando exchange de salida...")
                        from middleware.middleware import MessageMiddlewareExchange
                        mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
            
            if not sent:
                print(f"[AggregatorQuery3] CRÍTICO: No se pudo enviar batch {batch_header['batch_number']}")
    
    print("[AggregatorQuery3] Resultados finales enviados. Agregador terminado.")
    if shutdown_event:
        shutdown_event.set()

if __name__ == "__main__":
    import threading
    
    # Control de EOS - esperamos EOS de ambas fuentes
    shutdown_event = threading.Event()
    
    def signal_handler(signum, frame):
        print(f"[AggregatorQuery3] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada 1: TPV del group_by_query3
    mq_tpv = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Entrada 2: stores para JOIN
    mq_stores = MessageMiddlewareExchange(RABBIT_HOST, STORES_EXCHANGE, [STORES_ROUTING_KEY])
    
    # Salida: exchange para resultados finales
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] AggregatorQuery3 esperando mensajes...")
    print("[*] Query 3: TPV por semestre y sucursal 2024-2025 (06:00-23:00)")
    print("[*] Consumiendo de 2 fuentes: TPV + stores para JOIN")
    
    def consume_tpv():
        try:
            mq_tpv.start_consuming(on_tpv_message)
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"[AggregatorQuery3] Error en consumo de TPV: {e}")
    
    def consume_stores():
        try:
            mq_stores.start_consuming(on_stores_message)
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"[AggregatorQuery3] Error en consumo de stores: {e}")
    
    try:
        # Ejecutar ambos consumidores en paralelo como daemon threads
        tpv_thread = threading.Thread(target=consume_tpv, daemon=True)
        stores_thread = threading.Thread(target=consume_stores, daemon=True)
        
        tpv_thread.start()
        stores_thread.start()
        
        # Esperar hasta recibir EOS
        shutdown_event.wait()
        
    except KeyboardInterrupt:
        print("\n[AggregatorQuery3] Interrupción recibida")
    finally:
        # Detener consumo
        try:
            mq_tpv.stop_consuming()
        except:
            pass
        try:
            mq_stores.stop_consuming()
        except:
            pass
        
        # Cerrar conexiones
        try:
            mq_tpv.close()
        except:
            pass
        try:
            mq_stores.close()
        except:
            pass
        try:
            mq_out.close()
        except:
            pass
        print("[x] AggregatorQuery3 detenido")
