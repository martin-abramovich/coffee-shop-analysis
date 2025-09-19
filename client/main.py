# client/main.py
import argparse
import socket
import csv
from entities import Transaction

def send_message(sock: socket.socket, message: str):
    """
    Envía un mensaje como string al gateway TCP.
    Añade un delimitador ';' al final para separar mensajes.
    """
    data = message + ";"
    sock.sendall(data.encode("utf-8"))

def batch_iterator(csv_path, batch_size):
    """
    Lee un CSV y genera listas de dicts de tamaño batch_size.
    """
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append(row)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

def serialize_message(batch, stream_id, batch_id, seq_no, is_batch_end=False, is_eos=False):
    """
    Convierte un batch en un string delimitado manualmente.
    Ejemplo de formato simple: key1=value1,key2=value2|...
    """
    payload_lines = []
    for row in batch:
        fields = [f"{k}={v}" for k, v in row.items()]
        payload_lines.append(",".join(fields))
    payload_str = "|".join(payload_lines)

    message = (
        f"type=data|stream_id={stream_id}|batch_id={batch_id}|seq_no={seq_no}|"
        f"is_batch_end={int(is_batch_end)}|is_eos={int(is_eos)}|payload={payload_str}"
    )
    return message

def read_and_send(csv_path, batch_size, sock, stream_id):
    seq_no = 1
    batch_id = 1

    for batch in batch_iterator(csv_path, batch_size):
        msg = serialize_message(batch, stream_id, f"b{batch_id:04d}", seq_no, is_batch_end=True)
        send_message(sock, msg)
        print(f"Sent batch {batch_id} seq {seq_no}, size={len(batch)}")
        seq_no += 1
        batch_id += 1

    # Enviar mensaje End of Stream
    eos_msg = serialize_message([], stream_id, f"b{batch_id:04d}", seq_no, is_batch_end=True, is_eos=True)
    send_message(sock, eos_msg)
    print("Sent End of Stream.")

def main():
    parser = argparse.ArgumentParser(description="Client sending CSV to Gateway via TCP")
    parser.add_argument("--dataset", required=True, help="CSV file path")
    parser.add_argument("--host", default="localhost", help="Gateway host")
    parser.add_argument("--port", type=int, default=9000, help="Gateway port")
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per batch")
    parser.add_argument("--stream-id", default="report-123", help="Stream ID")
    args = parser.parse_args()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((args.host, args.port))
        print(f"Connected to gateway at {args.host}:{args.port}")
        read_and_send(args.dataset, args.batch_size, sock, args.stream_id)

    print("Client finished.")

if __name__ == "__main__":
    main()
