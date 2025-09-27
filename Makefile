.PHONY: test

test-middleware:
	@echo "🏁 Ejecutando TODOS los tests del middleware..."
	@bash middleware/test/start_rabbit.sh
	@bash middleware/test/test_working_queue_1to1.sh
	@bash middleware/test/test_working_queue_1toN.sh
	@bash middleware/test/test_exchange_1to1.sh
	@bash middleware/test/test_exchange_1toN.sh
	@bash middleware/test/stop_rabbit.sh
	@echo "✅ TODOS LOS TESTS PASARON"
