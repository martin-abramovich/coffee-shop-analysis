import sys
import os
import signal
import threading
import time

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
NUM_FILTER_AMOUNT_WORKERS = int(os.environ.get('NUM_FILTER_AMOUNT_WORKERS', '2'))

INPUT_EXCHANGE = "transactions_amount"    # exchange del filtro por amount
INPUT_ROUTING_KEY = "amount"              # routing key del filtro por amount
OUTPUT_EXCHANGE = "results_query1"        # exchange de salida para resultados finales
ROUTING_KEY = "query1_results"            # routing para resultados


class AggregatorQuery1:
    def __init__(self):
        # Acumulador de transacciones válidas por sesión
        self.session_data = {}  # {session_id: {'transactions': [], 'total_received': 0}}
        self.total_received = 0
        
    def accumulate_transactions(self, rows, session_id):
        """Acumula transacciones que pasaron todos los filtros para una sesión específica."""
        if session_id not in self.session_data:
            self.session_data[session_id] = {'transactions': [], 'total_received': 0}
        
        session_info = self.session_data[session_id]
        
        for row in rows:
            # Extraer los campos requeridos para Query 1: transaction_id y final_amount
            transaction_record = {
                'transaction_id': row.get('transaction_id'),
                'final_amount': row.get('final_amount')
            }
            
            # Validar que tengamos los campos necesarios
            if transaction_record.get('transaction_id') and transaction_record.get('final_amount') is not None:
                session_info['transactions'].append(transaction_record)
        
        session_info['total_received'] += len(rows)
        self.total_received += len(rows)
        
        # Log solo cada 1000 transacciones recibidas
        if self.total_received % 1000 < len(rows):
            total_accumulated = sum(len(data['transactions']) for data in self.session_data.values())
            print(f"[AggregatorQuery1] Total acumulado: {total_accumulated}/{self.total_received} (sesiones: {len(self.session_data)})")
    
    def generate_final_results(self, session_id):
        """Genera los resultados finales para Query 1 de una sesión específica."""
        print(f"[AggregatorQuery1] Generando resultados finales para sesión {session_id}...")
        
        if session_id not in self.session_data:
            print(f"[AggregatorQuery1] No hay datos para sesión {session_id}")
            return []
        
        session_info = self.session_data[session_id]
        accumulated_transactions = session_info['transactions']
        
        print(f"[AggregatorQuery1] Total transacciones válidas para sesión {session_id}: {len(accumulated_transactions)}")
        
        if not accumulated_transactions:
            print(f"[AggregatorQuery1] No hay transacciones válidas para reportar en sesión {session_id}")
            return []
        
        # Para Query 1, retornamos todas las transacciones con transaction_id y final_amount
        results = []
        for txn in accumulated_transactions:
            # Asegurar tipos de datos correctos
            try:
                result = {
                    'transaction_id': str(txn['transaction_id']),
                    'final_amount': float(txn['final_amount']) if isinstance(txn['final_amount'], str) else txn['final_amount']
                }
                results.append(result)
            except (ValueError, TypeError) as e:
                print(f"[AggregatorQuery1] Error procesando transacción {txn}: {e}")
                continue
        
        # Ordenar por transaction_id para consistencia
        results.sort(key=lambda x: x['transaction_id'])
        
        print(f"[AggregatorQuery1] Resultados generados para sesión {session_id}: {len(results)} transacciones")
        print(f"[AggregatorQuery1] Ejemplo de resultados:")
        for i, result in enumerate(results[:5]):  # Mostrar solo los primeros 5
            print(f"  {i+1}. TXN: {result['transaction_id']}, Monto: ${result['final_amount']:.2f}")
        if len(results) > 5:
            print(f"  ... y {len(results) - 5} más")
        
        # Estadísticas
        total_amount = sum(r['final_amount'] for r in results)
        avg_amount = total_amount / len(results) if results else 0
        print(f"[AggregatorQuery1] Monto total: ${total_amount:,.2f}, Promedio: ${avg_amount:.2f}")
            
        return results

# Instancia global del agregador
aggregator = AggregatorQuery1()
results_sent = {}  # {session_id: True} - Flags para evitar procesamiento duplicado de EOS por sesión

if __name__ == "__main__":
    import threading
    
    # Control de EOS por sesión - esperamos hasta recibir EOS de todos los workers para cada sesión
    eos_received = {}  # {session_id: threading.Event()}
    shutdown_event = threading.Event()
    
    # Control de EOS - necesitamos recibir de todos los workers de filter_amount por sesión
    class EOSCounter:
        def __init__(self):
            self.session_counts = {}  # {session_id: count}
            self.lock = threading.Lock()
        
        def increment(self, session_id):
            with self.lock:
                if session_id not in self.session_counts:
                    self.session_counts[session_id] = 0
                self.session_counts[session_id] += 1
                return self.session_counts[session_id]
    
    eos_counter = EOSCounter()
    
    def enhanced_on_message(body):
        global results_sent
        header, rows = deserialize_message(body)
        session_id = header.get("session_id", "unknown")
        
        # Verificar si es mensaje de End of Stream
        if header.get("is_eos") == "true":
            count = eos_counter.increment(session_id)
            print(f"[AggregatorQuery1] 🔚 EOS recibido para sesión {session_id} ({count}/{NUM_FILTER_AMOUNT_WORKERS})")
            
            # Solo procesar cuando recibimos de TODOS los workers para esta sesión
            if count < NUM_FILTER_AMOUNT_WORKERS:
                print(f"[AggregatorQuery1] Esperando más EOS para sesión {session_id}...")
                return
            
            # Prevenir procesamiento duplicado
            if session_id in results_sent:
                print(f"[AggregatorQuery1] ⚠️ EOS duplicado ignorado para sesión {session_id} (resultados ya enviados)")
                return
            
            print(f"[AggregatorQuery1] ✅ EOS recibido de TODOS los workers para sesión {session_id}. Generando resultados finales...")
            results_sent[session_id] = True
            
            # Generar resultados finales para esta sesión
            final_results = aggregator.generate_final_results(session_id)
            
            if final_results:
                # Enviar resultados finales
                results_header = {
                    "type": "result",
                    "stream_id": f"query1_results_{session_id}",
                    "batch_id": f"final_{session_id}",
                    "is_batch_end": "true",
                    "is_eos": "false",
                    "query": "query1",
                    "total_results": str(len(final_results)),
                    "description": "Transacciones_2024-2025_06:00-23:00_monto>=75",
                    "columns": "transaction_id:final_amount",
                    "is_final_result": "true",
                    "session_id": session_id
                }
                
                # Enviar en batches si hay muchos resultados
                batch_size = 100
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
                            # Log cada 100 batches para no saturar
                            if (i // batch_size + 1) % 100 == 0 or (i // batch_size + 1) == total_batches:
                                print(f"[AggregatorQuery1] Progreso: batch {batch_header['batch_number']}/{batch_header['total_batches']}")
                            break
                        except Exception as e:
                            print(f"[AggregatorQuery1] Error enviando batch {batch_header['batch_number']} (intento {retry+1}/{max_retries}): {e}")
                            if retry < max_retries - 1:
                                try:
                                    mq_out.close()
                                except:
                                    pass
                                from middleware.middleware import MessageMiddlewareExchange
                                mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
                    
                    if not sent:
                        print(f"[AggregatorQuery1] CRÍTICO: No se pudo enviar batch {batch_header['batch_number']}")
                
                print(f"[AggregatorQuery1] Envío completado para sesión {session_id}: {total_batches} batches, {len(final_results)} transacciones")
            
            print(f"[AggregatorQuery1] 🎉 Resultados finales enviados para sesión {session_id}.")
            
            # Marcar EOS recibido para esta sesión
            if session_id not in eos_received:
                eos_received[session_id] = threading.Event()
            eos_received[session_id].set()
            
            # Programar limpieza de la sesión después de un delay
            def delayed_cleanup():
                time.sleep(30)  # Esperar 30 segundos antes de limpiar
                with eos_counter.lock:
                    if session_id in eos_counter.session_counts:
                        del eos_counter.session_counts[session_id]
                if session_id in eos_received:
                    del eos_received[session_id]
                if session_id in results_sent:
                    del results_sent[session_id]
                # Limpiar datos de la sesión del agregador
                if session_id in aggregator.session_data:
                    del aggregator.session_data[session_id]
                print(f"[AggregatorQuery1] Sesión {session_id} limpiada")
            
            cleanup_thread = threading.Thread(target=delayed_cleanup, daemon=True)
            cleanup_thread.start()
            
            # No terminar el worker, puede recibir más sesiones
            return
        
        # Procesamiento normal: acumular datos para esta sesión
        if rows:
            aggregator.accumulate_transactions(rows, session_id)
    
    def signal_handler(signum, frame):
        print(f"[AggregatorQuery1] Señal {signum} recibida, cerrando...")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Entrada: suscripción al exchange del filtro por amount
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Salida: exchange para resultados finales
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] AggregatorQuery1 esperando mensajes...")
    print("[*] Query 1: Transacciones 2024-2025, 06:00-23:00, monto >= 75")
    print("[*] Columnas output: transaction_id, final_amount")
    print("[*] 🎯 Esperará hasta recibir EOS para generar reporte")
    
    try:
        def consume_messages():
            try:
                mq_in.start_consuming(enhanced_on_message)
            except Exception as e:
                if not shutdown_event.is_set():
                    print(f"[AggregatorQuery1] ❌ Error consumiendo: {e}")
        
        # Iniciar consumo en thread separado
        consumer_thread = threading.Thread(target=consume_messages)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Esperar indefinidamente - el worker NO termina después de EOS
        # Solo termina por señal externa (SIGTERM, SIGINT)
        print("[AggregatorQuery1] ✅ Worker iniciado, esperando mensajes de múltiples sesiones...")
        print("[AggregatorQuery1] 💡 El worker continuará procesando múltiples clientes")
        
        # Loop principal - solo termina por señal
        while not shutdown_event.is_set():
            time.sleep(1)
        
        print("[AggregatorQuery1] ✅ Terminando por señal externa")
            
    except KeyboardInterrupt:
        print("\n[AggregatorQuery1] Interrupción recibida")
    finally:
        # Detener consumo
        try:
            mq_in.stop_consuming()
        except:
            pass
        
        # Cerrar conexiones
        try:
            mq_in.close()
        except:
            pass
        try:
            mq_out.close()
        except:
            pass
        print("[x] AggregatorQuery1 detenido")