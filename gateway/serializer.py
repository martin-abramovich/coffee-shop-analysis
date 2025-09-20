def serialize_entity(entity) -> str:
    """
    Convierte un dataclass a string "k=v,k2=v2,..."
    """
    kv = []
    for field, value in entity.__dict__.items():
        kv.append(f"{field}={value}")
    return ",".join(kv)

def serialize_message(rows, stream_id="default", batch_id="b001",
                      is_batch_end=False, is_eos=False) -> str:
    """
    Serializa un batch completo a texto plano con header + payload
    """
    header_parts = [
        f"type=data",
        f"stream_id={stream_id}",
        f"batch_id={batch_id}",
        f"is_batch_end={is_batch_end}",
        f"is_eos={is_eos}"
    ]
    header = ";".join(header_parts) + ";"

    payload_rows = []
    for r in rows:
        payload_rows.append(serialize_entity(r))
    payload = "|".join(payload_rows) + ";"

    return header + payload
