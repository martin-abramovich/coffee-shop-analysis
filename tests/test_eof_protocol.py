"""
Caso de prueba aislado para el protocolo de finalización EOF.

Este test demuestra que el protocolo EOF funciona correctamente para:
1. Múltiples clientes concurrentes (N clientes)
2. Múltiples workers downstream (M workers)
3. Coordinación correcta de EOF entre N->M nodos
4. Limpieza adecuada de recursos
"""

import sys
import os
import threading
import time
import socket
import uuid
import logging
from typing import List, Dict, Set
from dataclasses import dataclass

# Añadir paths al PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from gateway.eof_protocol import EOFProtocolManager
from gateway.resource_manager import ResourceManager, ResourceType

# Configurar logging para el test
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class MockClient:
    """Cliente simulado para el test"""
    client_id: str
    entity_types: Set[str]
    processing_time: float  # Tiempo simulado de procesamiento
    
class MockWorker:
    """Worker simulado que recibe EOF"""
    def __init__(self, worker_id: str, expected_clients: int):
        self.worker_id = worker_id
        self.expected_clients = expected_clients
        self.received_eofs = []
        self.eof_count = 0
        self.lock = threading.Lock()
        
    def receive_eof(self, client_id: str):
        """Simula recepción de EOF de un cliente"""
        with self.lock:
            self.received_eofs.append(client_id)
            self.eof_count += 1
            logger.info(f"[WORKER {self.worker_id}] EOF recibido de cliente {client_id} ({self.eof_count}/{self.expected_clients})")
            
            if self.eof_count == self.expected_clients:
                logger.info(f"[WORKER {self.worker_id}] ✅ Todos los EOFs recibidos, worker puede terminar")
                return True
        return False
    
    def get_status(self):
        with self.lock:
            return {
                'worker_id': self.worker_id,
                'expected_clients': self.expected_clients,
                'received_eofs': len(self.received_eofs),
                'complete': self.eof_count >= self.expected_clients,
                'clients': self.received_eofs.copy()
            }

class EOFProtocolTest:
    """
    Test completo del protocolo EOF con múltiples clientes y workers.
    
    Escenario de prueba:
    - N clientes procesando diferentes tipos de entidad
    - M workers downstream esperando EOF
    - Coordinación N->M con protocolo robusto
    """
    
    def __init__(self, num_clients: int = 3, num_workers: int = 4):
        self.num_clients = num_clients
        self.num_workers = num_workers
        
        # Protocolo EOF y gestor de recursos
        self.eof_protocol = EOFProtocolManager()
        self.resource_manager = ResourceManager()
        
        # Clientes y workers simulados
        self.clients: List[MockClient] = []
        self.workers: Dict[str, MockWorker] = {}
        
        # Control de test
        self.test_results = {
            'clients_completed': 0,
            'workers_completed': 0,
            'protocol_errors': 0,
            'start_time': 0,
            'end_time': 0
        }
        
        self.setup_test_scenario()
    
    def setup_test_scenario(self):
        """Configura el escenario de prueba"""
        logger.info(f"=== CONFIGURANDO TEST EOF PROTOCOL ===")
        logger.info(f"Clientes: {self.num_clients}, Workers: {self.num_workers}")
        
        # Crear clientes simulados con diferentes tipos de entidad
        entity_combinations = [
            {"transactions", "users"},
            {"transaction_items", "stores"},
            {"transactions", "transaction_items", "menu_items"},
            {"users", "stores", "menu_items"},
            {"transactions", "transaction_items", "users", "stores"}
        ]
        
        for i in range(self.num_clients):
            client_id = f"client_{i+1}"
            entity_types = entity_combinations[i % len(entity_combinations)]
            processing_time = 1.0 + (i * 0.5)  # Diferentes tiempos de procesamiento
            
            client = MockClient(
                client_id=client_id,
                entity_types=entity_types,
                processing_time=processing_time
            )
            self.clients.append(client)
            
            logger.info(f"Cliente {client_id}: tipos {entity_types}, tiempo {processing_time}s")
        
        # Crear workers simulados
        worker_types = ["filter_year", "aggregator_query2", "aggregator_query3", "aggregator_query4"]
        
        for i in range(self.num_workers):
            worker_id = worker_types[i % len(worker_types)] + f"_{i}"
            worker = MockWorker(worker_id, self.num_clients)
            self.workers[worker_id] = worker
            
            logger.info(f"Worker {worker_id}: esperando EOF de {self.num_clients} clientes")
    
    def simulate_client_processing(self, client: MockClient):
        """Simula el procesamiento de un cliente"""
        logger.info(f"[CLIENT {client.client_id}] Iniciando procesamiento...")
        
        # Registrar sesión en protocolo EOF
        self.eof_protocol.register_session(client.client_id, client.entity_types)
        
        # Registrar recursos del cliente
        for entity_type in client.entity_types:
            self.resource_manager.register_resource(
                resource_id=f"{client.client_id}_{entity_type}_conn",
                resource_type=ResourceType.CONNECTION,
                session_id=client.client_id,
                metadata={'entity_type': entity_type}
            )
        
        # Simular procesamiento (tiempo variable por cliente)
        logger.info(f"[CLIENT {client.client_id}] Procesando por {client.processing_time}s...")
        time.sleep(client.processing_time)
        
        # Enviar EOF para cada tipo de entidad
        logger.info(f"[CLIENT {client.client_id}] Enviando EOFs...")
        
        for entity_type in client.entity_types:
            # Simular EOF del cliente
            workers_to_notify = self.eof_protocol.receive_eof(
                client.client_id,
                entity_type,
                f"{client.client_id}_{entity_type}_EOF"
            )
            
            # Notificar a workers si el protocolo lo autoriza
            if workers_to_notify:
                logger.info(f"[CLIENT {client.client_id}] Protocolo autoriza EOF a workers: {workers_to_notify}")
                for worker_id in workers_to_notify:
                    if worker_id in self.workers:
                        completed = self.workers[worker_id].receive_eof(client.client_id)
                        if completed:
                            self.test_results['workers_completed'] += 1
            
            # Pequeña pausa entre EOFs
            time.sleep(0.1)
        
        # Limpiar recursos del cliente
        cleaned = self.resource_manager.cleanup_session_resources(client.client_id)
        logger.info(f"[CLIENT {client.client_id}] Recursos limpiados: {cleaned}")
        
        # Desregistrar del protocolo EOF
        additional_workers = self.eof_protocol.unregister_session(client.client_id)
        if additional_workers:
            logger.info(f"[CLIENT {client.client_id}] Desconexión permite EOF adicional a: {additional_workers}")
            for worker_id in additional_workers:
                if worker_id in self.workers:
                    completed = self.workers[worker_id].receive_eof(client.client_id)
                    if completed:
                        self.test_results['workers_completed'] += 1
        
        self.test_results['clients_completed'] += 1
        logger.info(f"[CLIENT {client.client_id}] ✅ Procesamiento completado")
    
    def run_test(self) -> bool:
        """
        Ejecuta el test completo del protocolo EOF.
        
        Returns:
            True si el test pasa, False si falla
        """
        logger.info("=== INICIANDO TEST EOF PROTOCOL ===")
        self.test_results['start_time'] = time.time()
        
        # Crear threads para simular clientes concurrentes
        client_threads = []
        
        for client in self.clients:
            thread = threading.Thread(
                target=self.simulate_client_processing,
                args=(client,),
                name=f"Client-{client.client_id}"
            )
            client_threads.append(thread)
        
        # Iniciar todos los clientes
        logger.info("Iniciando clientes concurrentes...")
        for thread in client_threads:
            thread.start()
            time.sleep(0.2)  # Pequeño delay entre inicios
        
        # Esperar a que terminen todos los clientes
        logger.info("Esperando que terminen todos los clientes...")
        for thread in client_threads:
            thread.join()
        
        self.test_results['end_time'] = time.time()
        
        # Verificar resultados
        return self.verify_test_results()
    
    def verify_test_results(self) -> bool:
        """Verifica que el test haya pasado correctamente"""
        logger.info("=== VERIFICANDO RESULTADOS ===")
        
        duration = self.test_results['end_time'] - self.test_results['start_time']
        logger.info(f"Duración del test: {duration:.2f}s")
        
        # Verificar que todos los clientes completaron
        clients_ok = self.test_results['clients_completed'] == self.num_clients
        logger.info(f"Clientes completados: {self.test_results['clients_completed']}/{self.num_clients} {'✅' if clients_ok else '❌'}")
        
        # Verificar estado de workers
        workers_status = []
        all_workers_ok = True
        
        for worker_id, worker in self.workers.items():
            status = worker.get_status()
            workers_status.append(status)
            
            worker_ok = status['complete']
            all_workers_ok = all_workers_ok and worker_ok
            
            logger.info(f"Worker {worker_id}: {status['received_eofs']}/{status['expected_clients']} EOFs {'✅' if worker_ok else '❌'}")
        
        # Verificar protocolo EOF
        protocol_status = self.eof_protocol.get_protocol_status()
        protocol_ok = protocol_status['active_sessions'] == 0
        logger.info(f"Protocolo EOF - Sesiones activas: {protocol_status['active_sessions']} {'✅' if protocol_ok else '❌'}")
        
        # Verificar recursos
        resource_stats = self.resource_manager.get_resource_stats()
        resources_ok = resource_stats['total_active_resources'] == 0
        logger.info(f"Recursos activos: {resource_stats['total_active_resources']} {'✅' if resources_ok else '❌'}")
        
        # Resultado final
        test_passed = clients_ok and all_workers_ok and protocol_ok and resources_ok
        
        logger.info("=== RESULTADO FINAL ===")
        logger.info(f"Test EOF Protocol: {'✅ PASÓ' if test_passed else '❌ FALLÓ'}")
        
        if test_passed:
            logger.info("✅ Todos los clientes enviaron EOF correctamente")
            logger.info("✅ Todos los workers recibieron EOF de todos los clientes")
            logger.info("✅ Protocolo EOF coordinó correctamente N->M nodos")
            logger.info("✅ Todos los recursos fueron limpiados")
        else:
            logger.error("❌ El test falló - revisar logs para detalles")
        
        # Mostrar estadísticas detalladas
        logger.info("\n=== ESTADÍSTICAS DETALLADAS ===")
        logger.info(f"Protocolo EOF: {protocol_status['stats']}")
        logger.info(f"Recursos: {resource_stats['stats']}")
        
        return test_passed
    
    def cleanup(self):
        """Limpia recursos del test"""
        logger.info("Limpiando recursos del test...")
        self.resource_manager.shutdown()

def run_simple_eof_test():
    """Test simple con 2 clientes y 2 workers"""
    logger.info("=== TEST SIMPLE EOF (2 clientes, 2 workers) ===")
    
    test = EOFProtocolTest(num_clients=2, num_workers=2)
    try:
        result = test.run_test()
        return result
    finally:
        test.cleanup()

def run_complex_eof_test():
    """Test complejo con múltiples clientes y workers"""
    logger.info("=== TEST COMPLEJO EOF (5 clientes, 4 workers) ===")
    
    test = EOFProtocolTest(num_clients=5, num_workers=4)
    try:
        result = test.run_test()
        return result
    finally:
        test.cleanup()

def run_stress_eof_test():
    """Test de estrés con muchos clientes"""
    logger.info("=== TEST ESTRÉS EOF (10 clientes, 6 workers) ===")
    
    test = EOFProtocolTest(num_clients=10, num_workers=6)
    try:
        result = test.run_test()
        return result
    finally:
        test.cleanup()

if __name__ == "__main__":
    """
    Ejecuta todos los tests del protocolo EOF.
    
    Este test demuestra:
    1. Coordinación correcta entre N clientes y M workers
    2. Protocolo EOF robusto que espera a todos los clientes
    3. Limpieza correcta de recursos
    4. Logs detallados para debugging
    """
    
    logger.info("🧪 INICIANDO TESTS DEL PROTOCOLO EOF")
    logger.info("Este test demuestra el protocolo de finalización N->M nodos")
    
    all_tests_passed = True
    
    # Test 1: Simple
    try:
        result1 = run_simple_eof_test()
        all_tests_passed = all_tests_passed and result1
        time.sleep(2)
    except Exception as e:
        logger.error(f"Error en test simple: {e}")
        all_tests_passed = False
    
    # Test 2: Complejo
    try:
        result2 = run_complex_eof_test()
        all_tests_passed = all_tests_passed and result2
        time.sleep(2)
    except Exception as e:
        logger.error(f"Error en test complejo: {e}")
        all_tests_passed = False
    
    # Test 3: Estrés
    try:
        result3 = run_stress_eof_test()
        all_tests_passed = all_tests_passed and result3
    except Exception as e:
        logger.error(f"Error en test de estrés: {e}")
        all_tests_passed = False
    
    # Resultado final
    logger.info("\n" + "="*50)
    if all_tests_passed:
        logger.info("🎉 TODOS LOS TESTS PASARON")
        logger.info("✅ El protocolo EOF funciona correctamente para múltiples clientes")
        logger.info("✅ Coordinación N->M nodos implementada correctamente")
        logger.info("✅ Limpieza de recursos funciona adecuadamente")
        exit(0)
    else:
        logger.error("❌ ALGUNOS TESTS FALLARON")
        logger.error("Revisar logs para identificar problemas")
        exit(1)
