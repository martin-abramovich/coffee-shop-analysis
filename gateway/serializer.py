import pickle

def serialize_message(rows, batch_id, is_eos=False, session_id=None) -> bytes:
    """
    Serializa header + payload en binario puro usando pickle.
    """
    header = {
        "batch_id": batch_id,
        "is_eos": is_eos,
        "session_id": session_id,
    }

    message = {
        "header": header,
        "rows": rows,  # aseguro lista si vienen iterables
    }

    # Binario puro, máximo protocolo disponible.
    return pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)