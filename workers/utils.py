import pickle


def deserialize_message(b: bytes):
    """
    Deserializa completo un mensaje binario pickleado.
    """
    data = pickle.loads(b)
    return data["header"],data["rows"]

def serialize_message(rows:list, header_dict):
    """Serializa filas de vuelta a string manteniendo el header del gateway."""

    message = {
        "header": header_dict,
        "rows": rows,  # aseguro lista si vienen iterables
    }

    # Binario puro, máximo protocolo disponible.
    return pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)

# def deserialize_message(body: bytes):
#     """Convierte el mensaje de texto en (header_dict, rows: list[dict]).
#     Formato esperado:
#       header: "batch_id=...;is_eos=...;session_id=...;"
#       payload: "k=v,k2=v2|k=v,...;"
    
#     """
#     try:
#         text = body.decode("utf-8").strip()
#         tokens = [t for t in text.split(';') if t != '']
#         # Necesitamos al menos 5 tokens para el header estándar
#         if len(tokens) < 3:
#             raise ValueError("Mensaje demasiado corto")

#         # Separar header de payload de forma robusta:
#         # - Los tokens de header no contienen comas ni pipes
#         # - El payload (si existe) es el token que contiene ',' o '|'
#         header_tokens = []
#         payload_str = ''
#         for tok in tokens:
#             if '|' in tok or ',' in tok:
#                 payload_str = tok  # único token de payload
#             else:
#                 header_tokens.append(tok)

#         header = {}
#         for tok in header_tokens:
#             if '=' in tok:
#                 k, v = tok.split('=', 1)
#                 header[k] = v


#         rows = []
#         if payload_str:
#             for row in payload_str.split('|'):
#                 row = row.strip()
#                 if not row:
#                     continue
#                 fields = {}
#                 for field in row.split(','):
#                     if not field:
#                         continue
#                     if '=' in field:
#                         k, v = field.split('=', 1)
#                         fields[k] = v
#                 if fields:
#                     rows.append(fields)

#         return header, rows
#     except Exception as e:
#         raise ValueError(f"Error deserializando mensaje: {e}")


