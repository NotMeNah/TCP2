import threading
import queue
from Protocol import  Connection
import socket
import json

#握手、收发包
class RemoteUser:
    connection:Connection
    client_user:str
    user_name: str


    def __init__(self,
                 connection: Connection,
                 client_user: str,
                 user_name: str,
                 ):
        self.user_name=user_name
        self.client_user=client_user
        self.connection=connection

    @staticmethod
    def try_handshake(raw_connection:Connection):
        item:bytes=raw_connection.recv_package(timeout=None)
        array=item.decode().split('\0')
        user_name=array[0]
        client_user=array[1]
        return RemoteUser(raw_connection,client_user,user_name)

    def send_package(self,data:bytes):
        self.connection.send_package(data)

    def recv_package(self,timeout:int | None):
        return self.connection.recv_package(timeout)

connections:dict[str,RemoteUser]={}

event_queue:queue.Queue=queue.Queue()

#发消息给别人
def handle_push_user_message(
    item:dict[str,any],
    sender:RemoteUser):
    forward_msg={
        "event_type":item["event_type"],
        "data":item["data"],
        "sender":sender.user_name
    }
    msg_bytes=json.dumps(forward_msg).encode("utf-8")
    for user in connections.values():
        if user.user_name !=  sender.user_name:
            user.send_package(msg_bytes)

#取出队列数据，判断类型
def event_handler_main():
    while True:
        item=event_queue.get(block=True)
        item_as_dict:dict[str,any]=item
        event_type:str=item_as_dict["event_type"]
        sender:str=item_as_dict["sender"]

        match event_type:
            case "user_push_message":
                sender_user=connections.get(sender)
                if sender_user:
                    handle_push_user_message(item_as_dict,sender_user)

#接收消息，转化成字典，并放入队列
def connection_recv_thread_main(remote_user:RemoteUser):
    while True:
        data_received=remote_user.recv_package(None)
        msg_dict=json.loads(data_received.decode())
        msg_dict["sender"]=remote_user.user_name
        event_queue.put(msg_dict)

def main():
    port=12345
    host="127.0.0.1"

    threading.Thread(
        target=event_handler_main,
        daemon=True
    ).start()

    server_socket=socket.socket()
    server_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    server_socket.bind((host, port))
    server_socket.listen(5)

    print(f'Server is running on {host}:{port}')

    while True:
        client_socket,client_addr=server_socket.accept()
        print(f'Client connected: {client_addr}')

        client_connection=Connection(client_socket)
        remote_user=RemoteUser.try_handshake(client_connection)
        connections[remote_user.user_name]=remote_user

        threading.Thread(
            target=connection_recv_thread_main,
            args=(remote_user,),
            daemon=True
        ).start()


if __name__=='__main__':
    main()