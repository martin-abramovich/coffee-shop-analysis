"""
Chaos Monkey Script
-------------------

Este script mata o reinicia contenedores Docker al azar para probar la
resiliencia del sistema distribuido definido en `docker-compose.yml`.

Uso básico:
    python scripts/chaos_monkey.py --label role=node --min-interval 10 --max-interval 45

Requiere tener Docker CLI instalado y acceso al daemon (por ejemplo, el mismo
host donde corren los contenedores de docker-compose).
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence


def log(message: str) -> None:
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}][CHAOS] {message}", flush=True)


def run_cmd(cmd: Sequence[str]) -> str:
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Comando {' '.join(cmd)} falló (code={exc.returncode}): {exc.stderr.strip()}"
        ) from exc
    return completed.stdout.strip()


def parse_labels(labels_str: str) -> Dict[str, str]:
    labels: Dict[str, str] = {}
    if not labels_str:
        return labels
    for pair in labels_str.split(","):
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        labels[key.strip()] = value.strip()
    return labels


@dataclass
class ContainerInfo:
    container_id: str
    name: str
    image: str
    status: str
    labels: Dict[str, str]


def list_containers(label_filter: Optional[str] = None) -> List[ContainerInfo]:
    cmd = ["docker", "ps", "--format", "{{json .}}"]
    if label_filter:
        cmd.extend(["--filter", f"label={label_filter}"])

    output = run_cmd(cmd)
    containers: List[ContainerInfo] = []

    for line in output.splitlines():
        if not line.strip():
            continue
        data = json.loads(line)
        info = ContainerInfo(
            container_id=data.get("ID", ""),
            name=data.get("Names", ""),
            image=data.get("Image", ""),
            status=data.get("Status", ""),
            labels=parse_labels(data.get("Labels", "")),
        )
        containers.append(info)

    return containers


def filter_containers(
    containers: Sequence[ContainerInfo],
    include: Optional[Sequence[str]],
    exclude: Sequence[str],
) -> List[ContainerInfo]:
    include_set = {name.lower() for name in include} if include else None
    exclude_set = {name.lower() for name in exclude}

    filtered: List[ContainerInfo] = []
    for container in containers:
        name_lower = container.name.lower()
        if include_set is not None and name_lower not in include_set:
            continue
        if name_lower in exclude_set:
            continue
        filtered.append(container)
    return filtered


def perform_action(container_name: str, strategy: str, dry_run: bool) -> None:
    if strategy == "kill":
        cmd = ["docker", "kill", container_name]
    elif strategy == "stop":
        cmd = ["docker", "stop", container_name]
    elif strategy == "restart":
        cmd = ["docker", "restart", container_name]
    else:
        raise ValueError(f"Estrategia desconocida: {strategy}")

    if dry_run:
        log(f"[DRY-RUN] {' '.join(cmd)}")
        return

    log(f"Ejecutando {' '.join(cmd)}")
    run_cmd(cmd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chaos Monkey para docker-compose del TP.")
    parser.add_argument(
        "--label",
        default="role=node",
        help="Filtrar contenedores por label Docker (default: role=node).",
    )
    parser.add_argument(
        "--include",
        nargs="+",
        help="Nombres exactos de contenedores permitidos (si se omite toma todos los que matcheen el filtro).",
    )
    parser.add_argument(
        "--exclude",
        nargs="+",
        default=["coffee-rabbitmq"],
        help="Nombres exactos de contenedores a excluir (default: coffee-rabbitmq).",
    )
    parser.add_argument(
        "--strategy",
        choices=["kill", "stop", "restart"],
        default="kill",
        help="Acción aplicada al contenedor seleccionado (default: kill).",
    )
    parser.add_argument(
        "--min-interval",
        type=float,
        default=20.0,
        help="Tiempo mínimo entre fallos en segundos (default: 20).",
    )
    parser.add_argument(
        "--max-interval",
        type=float,
        default=60.0,
        help="Tiempo máximo entre fallos en segundos (default: 60).",
    )
    parser.add_argument(
        "--duration",
        type=float,
        help="Duración total de la prueba en segundos. Si no se especifica corre hasta interrupción manual.",
    )
    parser.add_argument(
        "--max-actions",
        type=int,
        help="Cantidad máxima de fallos a inyectar.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No ejecutar comandos sobre Docker, solo imprimirlos.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Semilla para el generador aleatorio (para reproducibilidad).",
    )
    return parser.parse_args()


def validate_interval(min_interval: float, max_interval: float) -> None:
    if min_interval <= 0 or max_interval <= 0:
        raise ValueError("Los intervalos deben ser mayores que cero.")
    if max_interval < min_interval:
        raise ValueError("--max-interval debe ser >= --min-interval.")


def main() -> int:
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    try:
        validate_interval(args.min_interval, args.max_interval)
    except ValueError as exc:
        log(f"Parámetros inválidos: {exc}")
        return 2

    shutdown_requested = False

    def handle_signal(signum, _frame):
        nonlocal shutdown_requested
        log(f"Señal {signum} recibida, finalizando...")
        shutdown_requested = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    end_time: Optional[float] = None
    if args.duration:
        end_time = time.monotonic() + args.duration

    actions_done = 0
    log("Chaos Monkey iniciado. Ctrl+C para detener.")

    while not shutdown_requested:
        if end_time and time.monotonic() >= end_time:
            log("Tiempo límite alcanzado, terminando ejecución.")
            break
        if args.max_actions and actions_done >= args.max_actions:
            log("Cantidad máxima de fallos alcanzada, terminando ejecución.")
            break

        try:
            candidates = filter_containers(
                list_containers(label_filter=args.label),
                include=args.include,
                exclude=args.exclude,
            )
        except RuntimeError as exc:
            log(f"No se pudieron obtener contenedores: {exc}")
            time.sleep(5)
            continue

        if not candidates:
            log("No hay contenedores elegibles. Reintentando en 10s...")
            time.sleep(10)
            continue

        target = random.choice(candidates)
        log(
            f"Seleccionado '{target.name}' (imagen={target.image}, status={target.status}) "
            f"con estrategia {args.strategy}"
        )

        try:
            perform_action(target.name, args.strategy, args.dry_run)
            actions_done += 1
        except RuntimeError as exc:
            log(f"Falló la acción sobre {target.name}: {exc}")

        interval = random.uniform(args.min_interval, args.max_interval)
        log(f"Durmiendo {interval:.1f}s antes del próximo ataque.")
        time.sleep(interval)

    log(f"Chaos Monkey detenido. Acciones ejecutadas: {actions_done}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

