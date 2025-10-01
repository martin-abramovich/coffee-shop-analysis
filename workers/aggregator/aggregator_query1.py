import sys
import os
import signal

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from middleware.middleware import MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
INPUT_EXCHANGE = "transactions_amount"    # exchange del filtro por amount
INPUT_ROUTING_KEY = "amount"              # routing key del filtro por amount
OUTPUT_EXCHANGE = "results_query1"        # exchange de salida para resultados finales
ROUTING_KEY = "query1_results"            # routing para resultados

class AggregatorQuery1:
    def __init__(self):
        # Acumulador de transacciones válidas
        self.accumulated_transactions = []
        self.total_received = 0
        
    def accumulate_transactions(self, rows):
        """Acumula transacciones que pasaron todos los filtros."""
        for row in rows:
            # Extraer los campos requeridos para Query 1: transaction_id y final_amount
            transaction_record = {
                'transaction_id': row.get('transaction_id'),
                'final_amount': row.get('final_amount')
            }
            
            # Validar que tengamos los campos necesarios
            if transaction_record.get('transaction_id') and transaction_record.get('final_amount') is not None:
                self.accumulated_transactions.append(transaction_record)
            else:
                missing = []
                if not transaction_record.get('transaction_id'):
                    missing.append('transaction_id')
                if transaction_record.get('final_amount') is None:
                    missing.append('final_amount')
                print(f"[AggregatorQuery1] Transacción descartada por campos faltantes: {missing}")
        
        self.total_received += len(rows)
        print(f"[AggregatorQuery1] Acumuladas {len(rows)} transacciones. Total acumulado: {len(self.accumulated_transactions)}")
    
    def generate_final_results(self):
        """Genera los resultados finales para Query 1."""
        print(f"[AggregatorQuery1] Generando resultados finales...")
        print(f"[AggregatorQuery1] Total transacciones válidas: {len(self.accumulated_transactions)}")
        
        if not self.accumulated_transactions:
            print(f"[AggregatorQuery1] No hay transacciones válidas para reportar")
            return []
        
        # Para Query 1, retornamos todas las transacciones con transaction_id y final_amount
        results = []
        for txn in self.accumulated_transactions:
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
        
        print(f"[AggregatorQuery1] Resultados generados: {len(results)} transacciones")
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

def on_message(body):
    header, rows = deserialize_message(body)
    
    # Verificar si es mensaje de End of Stream
    if header.get("is_eos") == "true":
        print("[AggregatorQuery1] End of Stream recibido. Generando resultados finales...")
        
        # Generar resultados finales
        final_results = aggregator.generate_final_results()
        
        if final_results:
            # Enviar resultados finales
            results_header = {
                "query": "query1",
                "total_results": str(len(final_results)),
                "description": "Transacciones 2024-2025, 06:00-23:00, monto >= 75",
                "columns": "transaction_id,final_amount",
                "is_final_result": "true"
            }
            
            # Enviar en batches si hay muchos resultados
            batch_size = 100
            for i in range(0, len(final_results), batch_size):
                batch = final_results[i:i + batch_size]
                batch_header = results_header.copy()
                batch_header["batch_number"] = str((i // batch_size) + 1)
                batch_header["total_batches"] = str((len(final_results) + batch_size - 1) // batch_size)
                
                result_msg = serialize_message(batch, batch_header)
                mq_out.send(result_msg)
                print(f"[AggregatorQuery1] Enviado batch {batch_header['batch_number']}/{batch_header['total_batches']} con {len(batch)} transacciones")
        
        print("[AggregatorQuery1] Resultados finales enviados. Agregador terminado.")
        return
    
    # Procesamiento normal: acumular datos
    if rows:
        aggregator.accumulate_transactions(rows)

if __name__ == "__main__":
    import threading
    
    # Control de EOS - esperamos hasta recibir EOS
    eos_received = threading.Event()
    shutdown_event = threading.Event()
    
    def enhanced_on_message(body):
        header, rows = deserialize_message(body)
        
        # Verificar si es mensaje de End of Stream
        if header.get("is_eos") == "true":
            print("[AggregatorQuery1] 🔚 End of Stream recibido. Generando resultados finales...")
            
            # Generar resultados finales
            final_results = aggregator.generate_final_results()
            
            if final_results:
                # Enviar resultados finales
                results_header = {
                    "query": "query1",
                    "total_results": str(len(final_results)),
                    "description": "Transacciones 2024-2025, 06:00-23:00, monto >= 75",
                    "columns": "transaction_id,final_amount",
                    "is_final_result": "true"
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
                    mq_out.send(result_msg)
                    print(f"[AggregatorQuery1] ✅ Enviado batch {batch_header['batch_number']}/{batch_header['total_batches']} con {len(batch)} transacciones")
            
            print("[AggregatorQuery1] 🎉 Resultados finales enviados. Agregador terminado.")
            eos_received.set()
            shutdown_event.set()
            return
        
        # Procesamiento normal: acumular datos
        if rows:
            aggregator.accumulate_transactions(rows)
    
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
        
        # Esperar hasta recibir EOS o timeout
        timeout = 300  # 5 minutos timeout
        if shutdown_event.wait(timeout):
            if eos_received.is_set():
                print("[AggregatorQuery1] ✅ Terminando por EOS recibido")
            else:
                print("[AggregatorQuery1] ⚠️ Terminando por señal")
        else:
            print(f"[AggregatorQuery1] ⏰ Timeout después de {timeout}s, terminando...")
            
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