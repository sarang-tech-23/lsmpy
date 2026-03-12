import socket, traceback
from lsm import LSMTree

HOST = "0.0.0.0"
PORT = 8000

kvstore = LSMTree()

def read(conn):
    buffer = b''
    while b'\n' not in buffer:
        chunk = conn.recv(1024)
        if not chunk:
            break
        buffer += chunk
        
    return buffer.decode()

def req_handler(c_req):
    def parser(s):
        w = s.split(' ')
        op = w[0]

        k = w[1] if len(w) > 1 else ''
        k = k.strip()
        v = None
        if len(w) == 3:
            v = w[2].strip()
        return op, k, v

    op, k, v = parser(c_req)
    print(f'=>> operation_details {op}:::{k}:{v}')
    if k == '':
        return ''

    response = ''
    if op == 'set':
        response = kvstore.add(k, v)
    
    elif op == 'update':
        response = kvstore.update(k, v)
    
    elif op == 'delete':
        response = kvstore.delete(k)

    elif op == 'get':
        response = kvstore.get(k)
    
    return response


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen()

    print(f"Server listening on {PORT}")

    while True:
        conn, addr = s.accept()
        print("Connected:", addr)

        with conn:
            try: 
                creq = read(conn)
                if creq is None:
                    print("Client disconnected")
                    break   # exit connection loop

                elif creq is not None:
                    response = req_handler(c_req=creq)
                    response += '\n'
                    conn.sendall(str(response).encode())
                    conn.close()
                else:
                    conn.sendall('error\n'.encode())
                    conn.close()
            except Exception as e:
                traceback.print_exc()
                print(f'server errro =>> {e}')
