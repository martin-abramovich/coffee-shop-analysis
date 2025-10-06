import sys
import os
import signal
import threading
from collections import defaultdict

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
NUM_GROUP_BY_QUERY2_WORKERS = int(os.environ.get('NUM_GROUP_BY_QUERY2_WORKERS', '2'))

INPUT_EXCHANGE = "transactions_query2"    # exchange del group_by query 2
INPUT_ROUTING_KEY = "query2"              # routing key del group_by
OUTPUT_EXCHANGE = "results_query2"        # exchange de salida para resultados finales
ROUTING_KEY = "query2_results"            # routing para resultados

# Exchanges adicionales para JOIN
MENU_ITEMS_EXCHANGE = "menu_items_raw"
MENU_ITEMS_ROUTING_KEY = "menu_items"
MENU_ITEMS_QUEUE = "menu_items_raw"

class AggregatorQuery2:
    def __init__(self):
        # Acumulador de métricas por (mes, item_id) - CAMBIO: usando item_id
        # Estructura: {(month, item_id): {'total_quantity': int, 'total_subtotal': float}}
        self.month_item_metrics = defaultdict(lambda: {
            'total_quantity': 0,
            'total_subtotal': 0.0
        })
        
        # Diccionario para JOIN: item_id -> item_name
        self.item_id_to_name = {}
        
        # Control de flujo
        self.batches_received = 0
        self.menu_items_loaded = False
        self.eos_received = False
        self.results_sent = False
        # Flags para deduplicar EOS
        self.eos_metrics_done = False
        self.eos_menu_done = False
        
    def load_menu_items(self, rows):
        """Carga menu_items para construir el diccionario item_id -> item_name."""
        for row in rows:
            item_id = row.get('item_id')
            item_name = row.get('item_name')
            
            if item_id and item_name:
                self.item_id_to_name[item_id] = item_name.strip()
        
        print(f"[AggregatorQuery2] Cargados {len(self.item_id_to_name)} menu items para JOIN")
    
    def accumulate_metrics(self, rows):
        """Acumula métricas parciales de group_by_query2."""
        for row in rows:
            month = row.get('month')
            item_id = row.get('item_id')  # CAMBIO: usar item_id en lugar de item_name
            total_quantity = row.get('total_quantity', 0)
            total_subtotal = row.get('total_subtotal', 0.0)
            
            # Validar campos requeridos
            if not month or not item_id:
                continue
                
            # Convertir a tipos correctos
            try:
                if isinstance(total_quantity, str):
                    total_quantity = int(float(total_quantity))
                if isinstance(total_subtotal, str):
                    total_subtotal = float(total_subtotal)
            except (ValueError, TypeError):
                continue
            
            # Clave compuesta: (mes, item_id)
            key = (month, item_id if not isinstance(item_id, str) else item_id.strip())
            
            # Acumular métricas
            self.month_item_metrics[key]['total_quantity'] += total_quantity
            self.month_item_metrics[key]['total_subtotal'] += total_subtotal
        
        self.batches_received += 1
        print(f"[AggregatorQuery2] Procesado batch {self.batches_received} con {len(rows)} registros")
        print(f"[AggregatorQuery2] Total combinaciones (mes, item_id): {len(self.month_item_metrics)}")
    
    def generate_final_results(self):
        """Genera los resultados finales para Query 2 con JOIN."""
        print(f"[AggregatorQuery2] Generando resultados finales...")
        print(f"[AggregatorQuery2] Total combinaciones procesadas: {len(self.month_item_metrics)}")
        print(f"[AggregatorQuery2] Menu items disponibles para JOIN: {len(self.item_id_to_name)}")
        
        if not self.month_item_metrics:
            print(f"[AggregatorQuery2] No hay datos para procesar")
            return []
        
        # Si no hay menu_items, haremos un fallback usando item_id como nombre
        if not self.item_id_to_name:
            print(f"[AggregatorQuery2] WARNING: No hay menu_items cargados. Se usará item_id como item_name (fallback)")
        
        # Preparar resultados por mes con JOIN
        results_by_month = defaultdict(list)
        
        # Agrupar por mes y hacer JOIN con menu_items
        for (month, item_id), metrics in self.month_item_metrics.items():
            # JOIN: buscar item_name para el item_id (o fallback al propio id)
            item_name = self.item_id_to_name.get(item_id) or str(item_id)
            
            results_by_month[month].append({
                'item_id': item_id,
                'item_name': item_name,
                'total_quantity': metrics['total_quantity'],
                'total_subtotal': metrics['total_subtotal']
            })
        
        final_results = []
        
        # Para cada mes, encontrar el producto más vendido y el de mayor ganancia
        for month, products in results_by_month.items():
            # Producto más vendido (mayor quantity)
            most_sold = max(products, key=lambda x: x['total_quantity'])
            
            # Producto con mayor ganancia (mayor subtotal)
            most_profitable = max(products, key=lambda x: x['total_subtotal'])
            
            final_results.append({
                'year_month_created_at': month,
                'item_name': most_sold['item_name'],
                'sellings_qty': most_sold['total_quantity'],
                'profit_sum': '',
                'metric_type': 'most_sold'
            })
            final_results.append({
                'year_month_created_at': month,
                'item_name': most_profitable['item_name'],
                'sellings_qty': '',
                'profit_sum': most_profitable['total_subtotal'],
                'metric_type': 'most_profitable'
            })
        
        # Ordenar por mes para consistencia
        final_results.sort(key=lambda x: x['year_month_created_at'])
        
        print(f"[AggregatorQuery2] Resultados generados para {len(results_by_month)} meses")
        print(f"[AggregatorQuery2] Total registros de resultados: {len(final_results)}")
        
        # Mostrar ejemplos
        print(f"[AggregatorQuery2] Ejemplos de resultados:")
        for i, result in enumerate(final_results[:5]):
            if 'sellings_qty' in result:
                print(f"  {i+1}. {result['year_month_created_at']} - {result['item_name']}: {result['sellings_qty']} unidades vendidas")
            if 'profit_sum' in result:
                try:
                    print(f"      Ganancia: ${float(result['profit_sum']):.2f}")
                except Exception:
                    print(f"      Ganancia: {result['profit_sum']}")
        
        if len(final_results) > 5:
            print(f"  ... y {len(final_results) - 5} más")
            
        return final_results
    
    def generate_detailed_results(self):
        """Genera resultados detallados con TODOS los productos por mes (opcional)."""
        detailed_results = []
        
        for (month, item_id), metrics in self.month_item_metrics.items():
            item_name = self.item_id_to_name.get(item_id) or str(item_id)
            # Registro para cantidad vendida
            detailed_results.append({
                'year_month_created_at': month,
                'item_id': item_id,
                'item_name': item_name,
                'sellings_qty': metrics['total_quantity']
            })
            
            # Registro para ganancia
            detailed_results.append({
                'year_month_created_at': month,
                'item_id': item_id,
                'item_name': item_name,
                'profit_sum': metrics['total_subtotal']
            })
        
        # Ordenar por mes y producto
        detailed_results.sort(key=lambda x: (x['year_month_created_at'], x['item_name']))
        
        return detailed_results

# Instancia global del agregador
aggregator = AggregatorQuery2()

# Variable global para control de shutdown
shutdown_event = None

# Control de EOS de múltiples workers
eos_count = 0
eos_lock = threading.Lock()

def on_metrics_message(body):
    """Maneja mensajes de métricas de group_by_query2."""
    global eos_count
    # Si ya enviamos resultados, ignorar mensajes adicionales
    if aggregator.results_sent:
        return
    
    try:
        header, rows = deserialize_message(body)
    except Exception as e:
        print(f"[AggregatorQuery2] Error deserializando mensaje: {e}")
        return
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        with eos_lock:
            eos_count += 1
            print(f"[AggregatorQuery2] EOS recibido ({eos_count}/{NUM_GROUP_BY_QUERY2_WORKERS})")
            
            # Solo procesar cuando recibimos de TODOS los workers
            if eos_count < NUM_GROUP_BY_QUERY2_WORKERS:
                print(f"[AggregatorQuery2] Esperando más EOS...")
                return
            
            if not aggregator.eos_metrics_done:
                print("[AggregatorQuery2] ✅ EOS recibido de TODOS los workers. Marcando como listo...")
                aggregator.eos_received = True
                aggregator.eos_metrics_done = True
                
                # Si ya tenemos menu_items cargados y aún no enviamos, generar resultados una sola vez
                if aggregator.menu_items_loaded and not aggregator.results_sent:
                    generate_and_send_results()
        return
    
    # Procesamiento normal: acumular métricas parciales
    if rows:
        aggregator.accumulate_metrics(rows)

def on_menu_items_message(body):
    """Maneja mensajes de menu_items para el JOIN."""
    # Si ya enviamos resultados, ignorar mensajes adicionales
    if aggregator.results_sent:
        return
    
    try:
        header, rows = deserialize_message(body)
    except Exception as e:
        print(f"[AggregatorQuery2] Error deserializando mensaje: {e}")
        return
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        if not aggregator.eos_menu_done:
            print("[AggregatorQuery2] EOS recibido en menu_items. Marcando como listo...")
            aggregator.menu_items_loaded = True
            aggregator.eos_menu_done = True
            
            # Si ya recibimos EOS de métricas y aún no enviamos, generar resultados
            if aggregator.eos_received and not aggregator.results_sent:
                generate_and_send_results()
        return
    
    # Cargar menu_items para JOIN
    if rows:
        aggregator.load_menu_items(rows)
        # Marcar como cargado si recibimos datos, aunque aún no llegue EOS
        aggregator.menu_items_loaded = True

def generate_and_send_results():
    """Genera y envía los resultados finales cuando ambos flujos terminaron."""
    global shutdown_event, mq_out
    
    # Evitar procesamiento duplicado
    if aggregator.results_sent:
        print("[AggregatorQuery2] ⚠️ Resultados ya enviados, ignorando llamada duplicada")
        return
    
    print("[AggregatorQuery2] 🔚 Ambos flujos completados. Generando resultados finales...")
    
    # Marcar como enviado ANTES de generar para evitar race conditions
    aggregator.results_sent = True
    
    # Generar resultados finales (TOP productos por mes)
    final_results = aggregator.generate_final_results()
    
    if final_results:
        # Enviar resultados finales con headers completos
        results_header = {
            "type": "result",
            "stream_id": "query2_results",
            "batch_id": "final",
            "is_batch_end": "true",
            "is_eos": "false",
            "query": "query2",
            "total_results": str(len(final_results)),
            "description": "Productos_mas_vendidos_y_mayor_ganancia_por_mes_2024-2025",
            "is_final_result": "true"
        }
        
        # Enviar en batches el conjunto combinado (dos filas por mes)
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
                    print(f"[AggregatorQuery2] Enviado batch {batch_header['batch_number']}/{batch_header['total_batches']}")
                    break
                except Exception as e:
                    print(f"[AggregatorQuery2] Error enviando batch {batch_header['batch_number']} (intento {retry+1}/{max_retries}): {e}")
                    if retry < max_retries - 1:
                        try:
                            mq_out.close()
                        except:
                            pass
                        print(f"[AggregatorQuery2] Reconectando exchange de salida...")
                        from middleware.middleware import MessageMiddlewareExchange
                        mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
            
            if not sent:
                print(f"[AggregatorQuery2] CRÍTICO: No se pudo enviar batch {batch_header['batch_number']}")
    else:
        print("[AggregatorQuery2] No hay resultados para enviar")
    
    print("[AggregatorQuery2] Resultados finales enviados. Agregador terminado.")
    shutdown_event.set()

if __name__ == "__main__":
    import threading
    
    # Control de EOS - esperamos EOS de ambas fuentes
    shutdown_event = threading.Event()
    
    def signal_handler(signum, frame):
        print(f"[AggregatorQuery2] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada 1: métricas del group_by_query2
    mq_metrics = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Entrada 2: menu_items para JOIN
    mq_menu_items = MessageMiddlewareQueue(RABBIT_HOST, MENU_ITEMS_QUEUE)
    
    # Salida: exchange para resultados finales
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] AggregatorQuery2 esperando mensajes...")
    print("[*] Query 2: Productos más vendidos y mayor ganancia por mes 2024-2025")
    print("[*] Consumiendo de 2 fuentes: métricas + menu_items para JOIN")
    print("[*] 🎯 Esperará hasta recibir EOS de ambas fuentes para generar reporte")
    
    def consume_metrics():
        try:
            mq_metrics.start_consuming(on_metrics_message)
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"[AggregatorQuery2] ❌ Error en consumo de métricas: {e}")
    
    def consume_menu_items():
        try:
            mq_menu_items.start_consuming(on_menu_items_message)
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"[AggregatorQuery2] ❌ Error en consumo de menu_items: {e}")
    
    try:
        # Ejecutar ambos consumidores en paralelo como daemon threads
        metrics_thread = threading.Thread(target=consume_metrics, daemon=True)
        menu_items_thread = threading.Thread(target=consume_menu_items, daemon=True)
        
        metrics_thread.start()
        menu_items_thread.start()
        
        # Esperar hasta recibir EOS (sin timeout, espera indefinidamente)
        shutdown_event.wait()
        print("[AggregatorQuery2] ✅ Terminando por EOS completo o señal")
        
    except KeyboardInterrupt:
        print("\n[AggregatorQuery2] Interrupción recibida")
    finally:
        # Detener consumo
        try:
            mq_metrics.stop_consuming()
        except:
            pass
        try:
            mq_menu_items.stop_consuming()
        except:
            pass
        
        # Cerrar conexiones
        try:
            mq_metrics.close()
        except:
            pass
        try:
            mq_menu_items.close()
        except:
            pass
        try:
            mq_out.close()
        except:
            pass
        print("[x] AggregatorQuery2 detenido")
