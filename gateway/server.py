import socket
from serializer import serialize_message
from processor import determine_data_type, process_data_by_type

HOST = "0.0.0.0"
PORT = 5000

def parse_message(raw_msg: str):
    """
    Protocolo Cliente → Gateway:
    type=data|stream_id=...|batch_id=...|seq_no=...|is_batch_end=...|is_eos=...|payload=key=value,key2=value2|...;
    """
    raw_msg = raw_msg.strip(";")
    
    # Parsear header
    header_parts = raw_msg.split("|payload=")
    if len(header_parts) != 2:
        raise ValueError("Formato de mensaje inválido")
    
    header_str, payload_str = header_parts
    
    # Parsear header
    header = {}
    for part in header_str.split("|"):
        if "=" in part:
            k, v = part.split("=", 1)
            header[k.strip()] = v.strip()
    
    # Parsear payload
    rows = []
    if payload_str.strip():
        for row in payload_str.split("|"):
            kv_pairs = row.split(",")
            row_dict = {}
            for pair in kv_pairs:
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    row_dict[k.strip()] = v.strip()
            if row_dict:  # Solo agregar filas no vacías
                rows.append(row_dict)
    
    return header, rows

def handle_client(conn, addr, mq):
    print(f"[GATEWAY] Conexión de {addr}")
    buffer = ""
    
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break

            buffer += data.decode("utf-8")

            while ";" in buffer:
                raw_msg, buffer = buffer.split(";", 1)
                try:
                    header, rows = parse_message(raw_msg + ";")
                    
                    # Verificar si es End of Stream
                    if header.get("is_eos") == "1":
                        print(f"[GATEWAY] End of Stream recibido para stream_id: {header.get('stream_id')}")
                        break
                    
                    # Determinar tipo de datos basado en las columnas
                    data_type = determine_data_type(rows[0] if rows else {})
                    
                    # Procesar según el tipo de datos
                    reduced = process_data_by_type(rows, data_type)
                    
                    if reduced:  # Solo enviar si hay datos procesados
                        # Serializar mensaje interno
                        msg = serialize_message(
                            reduced, 
                            stream_id=header.get("stream_id", "default"),
                            batch_id=header.get("batch_id", "b001"),
                            is_batch_end=header.get("is_batch_end") == "1",
                            is_eos=header.get("is_eos") == "1"
                        )

                        # Enviar al middleware
                        mq.send(msg)
                        print(f"[GATEWAY] Batch {header.get('batch_id')} de tipo {data_type} enviado al middleware ({len(reduced)} registros)")

                except Exception as e:
                    print(f"[GATEWAY] Error procesando mensaje: {e}")
                    continue

    except Exception as e:
        print(f"[GATEWAY] Error en conexión con {addr}: {e}")
    finally:
        conn.close()
        print(f"[GATEWAY] Conexión con {addr} cerrada")
