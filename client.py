import json
import threading
import queue
import socket
import argparse
import time

from Protocol import Connection,ProtocolException

send_pending_queue:queue.Queue=queue.Queue()
recv_pending_queue:queue.Queue=queue.Queue()

#负责和用户交流的线程
def user_msg_thread_main():
    while True:
        user_msg:str=input()
        user_msg_event={
            "event_type":"user_push_message",
            "data":{
                "content":user_msg
            }
        }
        send_pending_queue.put(user_msg_event)

#将用户内容打印
def client_ui_thread_main():
    while True:
        msg_get=recv_pending_queue.get()
        if msg_get["event_type"]=="user_push_message":
            sender=msg_get.get("sender","unknown")
            content=msg_get["data"]["content"]
            print(f"{sender}:{content}")

#发送内容
def send_msg_thread_main(connection:Connection):
    while True:
        try:
            item=send_pending_queue.get(block=True)
            json_str=json.dumps(item)
            json_utf=json_str.encode('utf-8')
            connection.send_package(json_utf)
            print(f"send item:{item}")
        except ProtocolException as e:
            print(f"Protocol Exception:{e}")
            pass
        except Exception as e:
            print(f"Fatal exception:{e}")
            exit(1)

#接受内容
def recv_msg_thread_main(connection:Connection):
    while True:
        item=connection.recv_package(timeout=None)
        item_str=item.decode()
        item_dict=json.loads(item_str)
        recv_pending_queue.put(item_dict)

def main():
    print("client started")
    parser = argparse.ArgumentParser(description='服务端参数配置')
    parser.add_argument('-username', default='匿名用户', help='用户名')
    args = parser.parse_args()
    user_name = args.username

    port=12345
    host="127.0.0.1"

    client_socket=socket.socket()
    client_socket.connect((host,port))
    connection=Connection(client_socket)
    data_send_str = user_name + "\0" + "v1.0"
    data_send = data_send_str.encode()
    connection.send_package(data_send)



    threading.Thread(
        target=send_msg_thread_main,
        args=(connection,),
        daemon=True,
    ).start()

    threading.Thread(
        target=recv_msg_thread_main,
        args=(connection,),
        daemon=False,
    ).start()

    threading.Thread(
        target=client_ui_thread_main,
        daemon=True,
    ).start()

    threading.Thread(
        target=user_msg_thread_main,
        daemon=True,
    ).start()

if __name__=='__main__':
    main()















