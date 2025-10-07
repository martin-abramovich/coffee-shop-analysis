def serialize_entity(entity) -> str:
    """
    Convierte un dataclass a string "k=v,k2=v2,..."
    """
    kv = []
    for field, value in entity.__dict__.items():
        kv.append(f"{field}={value}")
    return ",".join(kv)

def serialize_message(rows, stream_id="default", batch_id="b001",
                      is_batch_end=False, is_eos=False, session_id=None) -> bytes:
    """
    Serializa un batch completo a texto plano con header + payload
    """
    # Usar 'true'/'false' en minúsculas para compatibilidad con los workers
    is_batch_end_str = "true" if is_batch_end else "false"
    is_eos_str = "true" if is_eos else "false"

    header_parts = [
        f"type=data",
        f"stream_id={stream_id}",
        f"batch_id={batch_id}",
        f"is_batch_end={is_batch_end_str}",
        f"is_eos={is_eos_str}"
    ]
    
    # Agregar session_id si se proporciona
    if session_id:
        header_parts.append(f"session_id={session_id}")
    
    header = ";".join(header_parts) + ";"

    payload_rows = []
    for r in rows:
        payload_rows.append(serialize_entity(r))
    payload = "|".join(payload_rows) + ";"

    # Devolver bytes para que Pika publique correctamente y los workers decodifiquen
    return (header + payload).encode("utf-8")
