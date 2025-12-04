import argparse
import csv
import hashlib
import os
import sqlite3
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Iterable, List, Optional, Tuple


def normalize_row(fields: List[str]) -> bytes:
    """Return a canonical bytes representation of a CSV row.

    - Trim trailing/leading whitespace from each field (configurable in the future)
    - Use ASCII Unit Separator (\x1F) between fields to avoid delimiter collisions
    """
    # Keep as-is except strip common surrounding spaces for robustness
    normalized = [f.strip() for f in fields]
    return ("\x1F".join(normalized)).encode("utf-8", errors="strict")


def stable_row_hash(fields: List[str]) -> str:
    """Compute a stable hex hash for a row.

    Uses SHA1 for speed and stability across runs (not for security).
    """
    data = normalize_row(fields)
    return hashlib.sha1(data).hexdigest()


def iter_csv_rows(
    path: Path,
    delimiter: str,
    has_header: bool,
    quotechar: str = '"',
    encoding: str = "utf-8",
) -> Generator[List[str], None, None]:
    with path.open("r", encoding=encoding, newline="") as f:
        reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar)
        if has_header:
            # Skip header
            try:
                next(reader)
            except StopIteration:
                return
        for row in reader:
            yield row


@contextmanager
def sqlite_db(tmp_dir: Path) -> Generator[sqlite3.Connection, None, None]:
    db_path = tmp_dir / "expected_index.sqlite"
    conn = sqlite3.connect(db_path.as_posix())
    try:
        cur = conn.cursor()
        # Speed pragmas for bulk ops; safe for ephemeral index
        cur.execute("PRAGMA journal_mode=MEMORY;")
        cur.execute("PRAGMA synchronous=OFF;")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS rows (h TEXT PRIMARY KEY, c INTEGER NOT NULL)"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rows_c ON rows(c)")
        conn.commit()
        yield conn
    finally:
        conn.close()


def build_expected_index_sqlite(
    expected_csv: Path,
    delimiter: str,
    has_header: bool,
    tmp_dir: Path,
) -> sqlite3.Connection:
    conn: sqlite3.Connection
    with sqlite_db(tmp_dir) as temp_conn:
        conn = temp_conn
        cur = conn.cursor()
        batch: List[Tuple[str, int]] = []
        batch_size = 50_000
        # Wrap in one transaction for speed
        cur.execute("BEGIN TRANSACTION")
        for fields in iter_csv_rows(expected_csv, delimiter, has_header):
            h = stable_row_hash(fields)
            batch.append((h, 1))
            if len(batch) >= batch_size:
                _bulk_upsert(cur, batch)
                batch.clear()
        if batch:
            _bulk_upsert(cur, batch)
        conn.commit()
        # Return an open connection by creating a new one to the same path
        # because contextmanager will close temp_conn
        new_conn = sqlite3.connect((tmp_dir / "expected_index.sqlite").as_posix())
        return new_conn


def _bulk_upsert(cur: sqlite3.Cursor, rows: List[Tuple[str, int]]) -> None:
    # UPSERT based on primary key
    cur.executemany(
        """
        INSERT INTO rows(h, c)
        VALUES(?, ?)
        ON CONFLICT(h) DO UPDATE SET c = c + excluded.c
        """,
        rows,
    )


def validate_subset(
    results_csv: Path,
    expected_csv: Path,
    delimiter: str,
    has_header: bool,
    sample_limit: int = 10,
    show_missing_rows: bool = False,
) -> Tuple[bool, int, List[str]]:
    """Validate that every row in results_csv exists in expected_csv (multiset-aware).

    Returns (ok, missing_count, sample_missing_hex_hashes)
    """
    with tempfile.TemporaryDirectory() as td:
        tmp_dir = Path(td)
        conn = build_expected_index_sqlite(expected_csv, delimiter, has_header, tmp_dir)
        try:
            cur = conn.cursor()
            missing = 0
            samples: List[str] = []
            # Use transaction for faster repeated updates
            cur.execute("BEGIN TRANSACTION")
            for fields in iter_csv_rows(results_csv, delimiter, has_header):
                h = stable_row_hash(fields)
                cur.execute("SELECT c FROM rows WHERE h = ?", (h,))
                row = cur.fetchone()
                if row is None or row[0] <= 0:
                    missing += 1
                    if len(samples) < sample_limit:
                        if show_missing_rows:
                            # Render the original CSV row (joined by current delimiter)
                            samples.append(delimiter.join(fields))
                        else:
                            samples.append(h)
                else:
                    # decrement count
                    cur.execute("UPDATE rows SET c = c - 1 WHERE h = ?", (h,))
            conn.commit()
            return (missing == 0, missing, samples)
        finally:
            conn.close()


def discover_query_files(
    results_dir: Path,
    selected_queries: Optional[List[int]],
) -> List[Tuple[int, Path]]:
    pairs: List[Tuple[int, Path]] = []
    for p in sorted(results_dir.glob("query*.csv")):
        name = p.stem  # e.g., query3
        if not name.startswith("query"):
            continue
        try:
            qnum = int(name[5:])
        except ValueError:
            continue
        if selected_queries is not None and qnum not in selected_queries:
            continue
        pairs.append((qnum, p))
    return pairs


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Valida que cada fila de results/queryX.csv (o results/session_{id}/queryX.csv) "
            "esté incluida en results_expected/queryX.csv.\n"
            "Comparación multiset (considera duplicados) mediante índice SQLite temporal de hashes estables."
        )
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("results"),
        help="Directorio con CSVs de resultados (default: results)",
    )
    parser.add_argument(
        "--expected-dir",
        type=Path,
        default=None,
        help="Directorio con CSVs esperados. Si no se especifica, usa results/results_expected/reduced (o results/results_expected/complete con --complete).",
    )
    parser.add_argument(
        "--session-id",
        type=str,
        help="ID de sesión para validar archivos dentro de results/session_{session_id}/. Si se omite, busca directamente en results/.",
    )
    parser.add_argument(
        "--queries",
        type=int,
        nargs="*",
        help="Lista de IDs de queries a validar (e.g., --queries 1 3 4). Si se omite, se detectan todas.",
    )
    parser.add_argument(
        "--delimiter",
        type=str,
        default=",",
        help="Delimitador CSV (default: ,)",
    )
    parser.add_argument(
        "--header",
        action="store_true",
        help="Indica que los CSVs poseen header en la primera línea (se ignora en la comparación).",
    )
    parser.add_argument(
        "--no-header",
        dest="header",
        action="store_false",
        help="Indica que los CSVs no poseen header.",
    )
    parser.set_defaults(header=False)
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=10,
        help="Cantidad de hashes de ejemplo a reportar cuando faltan filas (default: 10)",
    )
    parser.add_argument(
        "--show-missing-rows",
        action="store_true",
        help=(
            "Muestra las filas faltantes (como líneas CSV) en lugar de hashes. "
            "Útil para depurar diferencias reales de contenido."
        ),
    )
    parser.add_argument(
        "--complete",
        action="store_true",
        help=(
            "Compara con results/results_expected/complete en lugar de "
            "results/results_expected/reduced (default)."
        ),
    )

    args = parser.parse_args(argv)

    results_dir: Path = args.results_dir
    selected: Optional[List[int]] = args.queries
    session_id: Optional[str] = args.session_id

    # Determinar el directorio esperado según si hay session-id o no
    if args.expected_dir is not None:
        expected_dir: Path = args.expected_dir
    else:
        # Determinar el path base según si hay session-id o no
        if session_id:
            # Si hay session-id, results_expected está en results/results_expected
            base_expected_dir = results_dir / "results_expected"
        else:
            # Si no hay session-id, usar results/results_expected
            base_expected_dir = results_dir / "results_expected"
        
        # Agregar /reduced (default) o /complete según la flag
        if args.complete:
            expected_dir = base_expected_dir / "complete"
        else:
            expected_dir = base_expected_dir / "reduced"

    # Si se especifica session-id, buscar dentro de la carpeta de sesión
    if session_id:
        results_dir = results_dir / f"session_{session_id}"
        print(f"[INFO] Validando sesión: {session_id}")
        print(f"[INFO] Buscando archivos en: {results_dir}")
        print(f"[INFO] Comparando con esperados en: {expected_dir}")

    if not results_dir.exists():
        print(f"[ERROR] No existe el directorio de resultados: {results_dir}", file=sys.stderr)
        return 2
    if not expected_dir.exists():
        print(
            f"[WARN] El directorio de esperados no existe aún: {expected_dir}. Se marcarán todas las queries como fallidas por faltantes."
        )

    discovered = discover_query_files(results_dir, selected)
    if not discovered:
        print("[WARN] No se encontraron archivos queryX.csv en results.")
        return 0

    overall_ok = True
    for qnum, res_path in discovered:
        exp_path = expected_dir / f"query{qnum}.csv"
        if not exp_path.exists():
            print(f"[FAIL] Q{qnum}: Falta archivo esperado {exp_path}")
            overall_ok = False
            continue
        ok, missing, samples = validate_subset(
            res_path,
            exp_path,
            delimiter=args.delimiter,
            has_header=args.header,
            sample_limit=args.sample_limit,
            show_missing_rows=args.show_missing_rows,
        )
        if ok:
            print(f"[ OK ] Q{qnum}: todas las filas de {res_path.name} están en {exp_path.name}")
        else:
            overall_ok = False
            if args.show_missing_rows:
                if samples:
                    print(f"[FAIL] Q{qnum}: {missing} filas faltantes. Ejemplos de filas:")
                    for s in samples:
                        print(f"  -> {s}")
                else:
                    print(f"[FAIL] Q{qnum}: {missing} filas faltantes.")
            else:
                extra = f" (ejemplos de hashes faltantes: {', '.join(samples)})" if samples else ""
                print(
                    f"[FAIL] Q{qnum}: {missing} filas de {res_path.name} no se encuentran en {exp_path.name}{extra}"
                )

    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())


