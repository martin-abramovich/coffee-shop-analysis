#!/usr/bin/env python3
"""
Script para ejecutar el test del protocolo EOF de forma independiente.

Este script permite probar el protocolo EOF sin necesidad de levantar
todo el sistema distribuido.
"""

import sys
import os

# Añadir el directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tests.test_eof_protocol import (
    run_simple_eof_test,
    run_complex_eof_test, 
    run_stress_eof_test
)

def main():
    print("🧪 Ejecutando tests del protocolo EOF...")
    print("Este test demuestra el protocolo de finalización para múltiples clientes")
    print("-" * 60)
    
    # Ejecutar test simple
    print("\n1️⃣ Test Simple (2 clientes, 2 workers)")
    result1 = run_simple_eof_test()
    
    print("\n2️⃣ Test Complejo (5 clientes, 4 workers)")  
    result2 = run_complex_eof_test()
    
    print("\n3️⃣ Test de Estrés (10 clientes, 6 workers)")
    result3 = run_stress_eof_test()
    
    # Resultado final
    all_passed = result1 and result2 and result3
    
    print("\n" + "="*60)
    if all_passed:
        print("🎉 TODOS LOS TESTS PASARON")
        print("✅ Protocolo EOF funciona correctamente")
        return 0
    else:
        print("❌ ALGUNOS TESTS FALLARON")
        return 1

if __name__ == "__main__":
    exit(main())
