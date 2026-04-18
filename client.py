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

# set 
key, value = 'name_1', 'aditya'
print(client.set(key, value))

# get
print(client.get(key))

# update
value = 'sarang'
print(client.update(key, value))

# get
print(client.get(key))

# delete
print(client.delete(key))

# get
print(client.get(key))
