def deserialize_message(body: bytes):
    """Convierte el mensaje de texto en (header_dict, rows: list[dict]).
    Formato esperado:
      header: "type=data;stream_id=...;batch_id=...;is_batch_end=...;is_eos=...;"
      payload: "k=v,k2=v2|k=v,...;"
    """
    try:
        text = body.decode("utf-8").strip()
        tokens = [t for t in text.split(';') if t != '']
        # Permitimos mensajes sin payload (por ejemplo, EOS) que traen solo el header (5 tokens)
        if len(tokens) < 5:
            raise ValueError("Mensaje demasiado corto")

        header_tokens = tokens[:5]
        header = {}
        for tok in header_tokens:
            if '=' in tok:
                k, v = tok.split('=', 1)
                header[k] = v
            else:
                header[tok] = True

        payload_str = ';'.join(tokens[5:])

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
    for k in ["type", "stream_id", "batch_id", "is_batch_end", "is_eos"]:
        if k in header_dict:
            header_parts.append(f"{k}={header_dict[k]}")
    header = ';'.join(header_parts) + ';'

    payload = "|".join(
        [",".join(f"{k}={v}" for k, v in row.items()) for row in rows]
    ) + ';'
    return (header + payload).encode("utf-8")
