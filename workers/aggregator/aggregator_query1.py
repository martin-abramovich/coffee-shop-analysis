from middleware import MessageMiddlewareExchange
from workers.utils import deserialize_message, serialize_message

# --- Configuración ---
RABBIT_HOST = "localhost"
INPUT_EXCHANGE = "transactions_amount"    # exchange del filtro por amount
INPUT_ROUTING_KEY = "amount"              # routing key del filtro por amount
OUTPUT_EXCHANGE = "results_query1"        # exchange de salida para resultados finales
ROUTING_KEY = "query1_results"            # routing para resultados

class AggregatorQuery1:
    def __init__(self):
        # Acumulador de transaction_items válidos
        self.accumulated_items = []
        self.total_received = 0
        
    def accumulate_transaction_items(self, rows):
        """Acumula transaction_items que pasaron todos los filtros."""
        for row in rows:
            # Extraer los campos requeridos para Query 1: transaction_items completos
            item_record = {
                'transaction_id': row.get('transaction_id'),
                'item_id': row.get('item_id'),
                'quantity': row.get('quantity'),
                'unit_price': row.get('unit_price'),
                'subtotal': row.get('subtotal'),
                'created_at': row.get('created_at')
            }
            
            # Validar que tengamos los campos necesarios
            required_fields = ['transaction_id', 'item_id', 'quantity', 'subtotal', 'created_at']
            if all(item_record.get(field) is not None for field in required_fields):
                self.accumulated_items.append(item_record)
            else:
                missing_fields = [field for field in required_fields if item_record.get(field) is None]
                print(f"[AggregatorQuery1] Item descartado por campos faltantes: {missing_fields}")
        
        self.total_received += len(rows)
        print(f"[AggregatorQuery1] Acumulados {len(rows)} items. Total acumulado: {len(self.accumulated_items)}")
    
    def generate_final_results(self):
        """Genera los resultados finales para Query 1."""
        print(f"[AggregatorQuery1] Generando resultados finales...")
        print(f"[AggregatorQuery1] Total transaction_items válidos: {len(self.accumulated_items)}")
        
        if not self.accumulated_items:
            print(f"[AggregatorQuery1] No hay transaction_items válidos para reportar")
            return []
        
        # Para Query 1, retornamos todos los transaction_items con las columnas requeridas
        results = []
        for item in self.accumulated_items:
            # Asegurar tipos de datos correctos
            try:
                result = {
                    'transaction_id': str(item['transaction_id']),
                    'item_id': str(item['item_id']),
                    'quantity': int(item['quantity']) if item['quantity'] is not None else 0,
                    'unit_price': float(item['unit_price']) if item.get('unit_price') is not None else 0.0,
                    'subtotal': float(item['subtotal']) if item['subtotal'] is not None else 0.0,
                    'created_at': str(item['created_at'])
                }
                results.append(result)
            except (ValueError, TypeError) as e:
                print(f"[AggregatorQuery1] Error procesando item {item}: {e}")
                continue
        
        # Ordenar por transaction_id y luego por item_id para consistencia
        results.sort(key=lambda x: (x['transaction_id'], x['item_id']))
        
        print(f"[AggregatorQuery1] Resultados generados: {len(results)} transaction_items")
        print(f"[AggregatorQuery1] Ejemplo de resultados:")
        for i, result in enumerate(results[:3]):  # Mostrar solo los primeros 3
            print(f"  {i+1}. TXN: {result['transaction_id']}, Item: {result['item_id']}, "
                  f"Qty: {result['quantity']}, Subtotal: ${result['subtotal']:.2f}")
        if len(results) > 3:
            print(f"  ... y {len(results) - 3} más")
            
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
                "total_results": len(final_results),
                "description": "Transaction_items de transacciones 2024-2025, 06:00-23:00, monto >= 75",
                "columns": "transaction_id,item_id,quantity,unit_price,subtotal,created_at",
                "is_final_result": "true"
            }
            
            # Enviar en batches si hay muchos resultados
            batch_size = 50  # Menos items por batch debido a más datos por registro
            for i in range(0, len(final_results), batch_size):
                batch = final_results[i:i + batch_size]
                batch_header = results_header.copy()
                batch_header["batch_number"] = (i // batch_size) + 1
                batch_header["total_batches"] = (len(final_results) + batch_size - 1) // batch_size
                
                result_msg = serialize_message(batch, batch_header)
                mq_out.send(result_msg)
                print(f"[AggregatorQuery1] Enviado batch {batch_header['batch_number']}/{batch_header['total_batches']} con {len(batch)} items")
        
        print("[AggregatorQuery1] Resultados finales enviados. Agregador terminado.")
        return
    
    # Procesamiento normal: acumular datos
    if rows:
        aggregator.accumulate_transaction_items(rows)

if __name__ == "__main__":
    # Entrada: suscripción al exchange del filtro por amount
    mq_in = MessageMiddlewareExchange(RABBIT_HOST, INPUT_EXCHANGE, [INPUT_ROUTING_KEY])
    
    # Salida: exchange para resultados finales
    mq_out = MessageMiddlewareExchange(RABBIT_HOST, OUTPUT_EXCHANGE, [ROUTING_KEY])
    
    print("[*] AggregatorQuery1 esperando mensajes...")
    print("[*] Query 1: Transaction_items de transacciones 2024-2025, 06:00-23:00, monto >= 75")
    print("[*] Columnas output: transaction_id, item_id, quantity, unit_price, subtotal, created_at")
    
    try:
        mq_in.start_consuming(on_message)
    except KeyboardInterrupt:
        mq_in.stop_consuming()
        mq_in.close()
        mq_out.close()
        print("[x] AggregatorQuery1 detenido")