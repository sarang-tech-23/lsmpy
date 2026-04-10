import socket

class LsmpyClient():
    def __init__(self, host='localhost', port=8000):
        self.host = host
        self.port = port
    
    def _send_request(self, command):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))
            sock.sendall((command + "\n").encode())

            response = b''
            while b'\n' not in response:
                chunk = sock.recv(1024)
                response += chunk
            
            return response.decode()

    def get(self, k):
        return self._send_request(f'get {k}')

    def set(self, k, v):
        return self._send_request(f'set {k} {v}')

    def update(self, k, v):
        return self._send_request(f'update {k} {v}')

    def delete(self, k):
        return self._send_request(f'delete {k}')



client = LsmpyClient()


for i in range(1, 5000):
    print(client.set(f'name_{i}', f'aditya_{i}'))
    print(client.get(f'name_{i}'))
    # print(client.update(f'name_{i}', f'aditya_{i}{i}'))

    # print(client.delete(f'name_{i}'))

# print(client.delete('name'))

# client.close()