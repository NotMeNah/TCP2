import struct
from socket import socket

class ProtocolException(Exception):
    def __init__(self,msg):
        super().__init__(msg)

class Connection:
    sock:socket

    def __init__(self,sock:socket):
        self.sock=sock

    def send_package(self,data:bytes):
        length=len(data)
        header=struct.pack('>I',length)
        package=header+data
        self.sock.sendall(package)

    def recv_package(self,timeout:int | None )->bytes:
        self.sock.settimeout(timeout)
        header:bytes=self.sock.recv(4)
        body_len=struct.unpack('>I',header)[0]
        if body_len < 0:
            raise ProtocolException(f'Packet cannot have negative packet length.Receive:{body_len}')
        body:bytes=self.sock.recv(body_len)
        if not body:
            raise ProtocolException(f'Failed to receive body.Expects length:{body_len}')
        return body