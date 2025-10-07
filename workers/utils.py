
def deserialize_message(body: bytes):
    """Convierte el mensaje de texto en (header_dict, rows: list[dict]).
    Formato esperado:
      header: "type=data;stream_id=...;batch_id=...;is_batch_end=...;is_eos=...;session_id=...;"
      payload: "k=v,k2=v2|k=v,...;"
    
    """
    try:
        text = body.decode("utf-8").strip()
        tokens = [t for t in text.split(';') if t != '']
        # Necesitamos al menos 5 tokens para el header estándar
        if len(tokens) < 5:
            raise ValueError("Mensaje demasiado corto")

        # Separar header de payload de forma robusta:
        # - Los tokens de header no contienen comas ni pipes
        # - El payload (si existe) es el token que contiene ',' o '|'
        header_tokens = []
        payload_str = ''
        for tok in tokens:
            if '|' in tok or ',' in tok:
                payload_str = tok  # único token de payload
            else:
                header_tokens.append(tok)

        header = {}
        for tok in header_tokens:
            if '=' in tok:
                k, v = tok.split('=', 1)
                header[k] = v


        rows = []
        if payload_str:
            for row in payload_str.split('|'):
                row = row.strip()
                if not row:
                    continue
                fields = {}
                for field in row.split(','):
                    if not field:
                        continue
                    if '=' in field:
                        k, v = field.split('=', 1)
                        fields[k] = v
                if fields:
                    rows.append(fields)

        return header, rows
    except Exception as e:
        raise ValueError(f"Error deserializando mensaje: {e}")

def serialize_message(rows, header_dict):
    """Serializa filas de vuelta a string manteniendo el header del gateway."""
    header_parts = []
    # Primero los campos estándar en orden
    for k in ["type", "stream_id", "batch_id", "is_batch_end", "is_eos", "session_id"]:
        if k in header_dict:
            header_parts.append(f"{k}={header_dict[k]}")
    # Luego agregar cualquier campo adicional del header
    for k, v in header_dict.items():
        if k not in ["type", "stream_id", "batch_id", "is_batch_end", "is_eos", "session_id"]:
            header_parts.append(f"{k}={v}")
    header = ';'.join(header_parts) + ';'

    payload = "|".join(
        [",".join(f"{k}={v}" for k, v in row.items()) for row in rows]
    ) + ';'
    return (header + payload).encode("utf-8")
