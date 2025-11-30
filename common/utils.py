def timestamp_to_year_month_int(ts: int) -> int:
    """
    Convierte un timestam a un numero yyyymmm, util query 2.
    """
    days = ts // 86400  # días desde 1970-01-01

    # Algoritmo de conversión de días a fecha (Hinnant)
    z = days + 719468
    era = (z >= 0) and z // 146097 or -((-z - 1) // 146097) - 1
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    m = mp + 3 if mp < 10 else mp - 9
    y += (m <= 2)

    return y * 100 + m

def timestamp_to_year_semester(ts: int) -> int:
    """
    Convierte un timestamp UNIX a un entero YYYYS,
    donde S = 1 (enero-junio) o 2 (julio-diciembre),
    sin usar datetime. Ultra rápido. Util query 3. 
    """
    # días desde epoch
    days = ts // 86400

    # Algoritmo de Howard Hinnant (date::year_month_day)
    z = days + 719468
    era = z // 146097 if z >= 0 else -((-z - 1) // 146097) - 1
    doe = z - era * 146097                         # day of era
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400                            # año
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153                      # month prime
    m = mp + 3 if mp < 10 else mp - 9              # mes (1–12)
    y += (m <= 2)

    # semestre
    s = 1 if m <= 6 else 2

    return y * 10 + s

def yyyymm_int_to_str(ym: int) -> str:
    """
    Convierte de un yyyymm int a YYYY - MM string, util query 2. 
    """
    year = ym // 100
    month = ym % 100
    return f"{year:04d}-{month:02d}"

def yyyys_int_to_string(yyyys: int) -> str:
    """
    Convierte un entero YYYYS en string 'YYYY - HS. Util query 3'
    """
    year = yyyys // 10
    sem = yyyys % 10
    return f"{year}-H{sem}"

def hour_from_timestamp(ts: int) -> int:
    """
    Devuelve la HORA (0-23) en UTC desde un timestamp UNIX.
    """
    # segundos del día (0–86399)
    sec_day = ts % 86400
    if sec_day < 0:  # manejar timestamps negativos
        sec_day += 86400

    # hora = segundos / 3600
    return sec_day // 3600