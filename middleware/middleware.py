from abc import ABC, abstractmethod
import pika 

class MessageMiddlewareMessageError(Exception):
    pass

class MessageMiddlewareDisconnectedError(Exception):
    pass

class MessageMiddlewareCloseError(Exception):
    pass

class MessageMiddlewareDeleteError(Exception):
    pass

class MessageMiddleware(ABC):

	#Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
	#cada mensaje de datos o de control.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	@abstractmethod
	def start_consuming(self, on_message_callback):
		pass
	
	#Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	#no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	@abstractmethod
	def stop_consuming(self):
		pass
	
	#Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	@abstractmethod
	def send(self, message):
		pass

	#Se desconecta de la cola o exchange al que estaba conectado.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
	@abstractmethod
	def close(self):
		pass

	# Se fuerza la eliminación remota de la cola o exchange.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
	@abstractmethod
	def delete(self):
		pass


class MessageMiddlewareExchange(MessageMiddleware):
	def __init__(self, host, exchange_name, route_keys):
		self.host = host
		self.exchange_name = exchange_name
		self.route_keys = route_keys

		try:
			self.connection = pika.BlockingConnection(
				pika.ConnectionParameters(host=self.host)
			)
			self.channel = self.connection.channel()
			self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="topic")
		except pika.exceptions.AMQPConnectionError:
			raise MessageMiddlewareDisconnectedError("No se pudo conectar al middleware.")

	def start_consuming(self, on_message_callback):
		try:
			# Cola temporal para consumir mensajes del exchange
			result = self.channel.queue_declare(queue="", exclusive=True)
			queue_name = result.method.queue

			for key in self.route_keys:
				self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=key)

			def wrapper_callback(ch, method, properties, body):
				try:
					on_message_callback(body)  # Ejecutamos tu lógica
					ch.basic_ack(delivery_tag=method.delivery_tag)  # Confirmamos que fue procesado
				except Exception as e:
					ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
					# print(f"Error procesando mensaje: {e}")

			self.channel.basic_consume(
				queue=queue_name,
				on_message_callback=wrapper_callback,
				auto_ack=False 
			)

			self.channel.start_consuming()

		except pika.exceptions.AMQPConnectionError:
			raise MessageMiddlewareDisconnectedError("Se perdió la conexión con el middleware.")
		except Exception as e:
			raise MessageMiddlewareMessageError(f"Error inesperado: {e}")

	def stop_consuming(self):
		try:
			self.channel.stop_consuming()
		except pika.exceptions.AMQPConnectionError:
			raise MessageMiddlewareDisconnectedError("Se perdió la conexión con el middleware.")

	def send(self, message):
		try:
			for key in self.route_keys:
				self.channel.basic_publish(
					exchange=self.exchange_name,
					routing_key=key,
					body=message
				)
		except pika.exceptions.AMQPConnectionError:
			raise MessageMiddlewareDisconnectedError("Se perdió la conexión con el middleware.")
		except Exception as e:
			raise MessageMiddlewareMessageError(f"Error enviando mensaje: {e}")

	def close(self):
		try:
			self.connection.close()
		except Exception as e:
			raise MessageMiddlewareCloseError(f"Error cerrando la conexión: {e}")

	def delete(self):
		try:
			self.channel.exchange_delete(exchange=self.exchange_name)
		except Exception as e:
			raise MessageMiddlewareDeleteError(f"No se pudo eliminar el exchange: {e}")
		
class MessageMiddlewareQueue(MessageMiddleware):
	def __init__(self, host, queue_name):
		pass