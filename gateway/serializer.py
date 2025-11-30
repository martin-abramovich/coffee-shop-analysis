import pickle

BASE_HEADER = {
    "batch_id": None,
    "is_eos": False,
    "session_id": None,
}

def serialize_message(rows, batch_id, is_eos=False, session_id=None) -> bytes:
    """
    Serializa header + payload en binario puro usando pickle,
    evitando recrear dicts grandes.
    """

    # Clon MUY liviano → más rápido que crear 3 strings nuevas
    header = BASE_HEADER.copy()
    header["batch_id"] = batch_id
    header["is_eos"] = is_eos
    header["session_id"] = session_id

    message = {
        "header": header,
        "rows": rows,
    }

    return pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)