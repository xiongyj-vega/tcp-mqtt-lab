# -*- coding: utf-8 -*-
"""极简 MQTT 服务器（Broker）

目标：监听 1884，接受客户端连接，处理订阅，把收到的消息转发给订阅者。
运行：python mqtt/broker.py
"""

import socket
import threading

import mqtt_mini as mqtt

HOST = "127.0.0.1"
PORT = 1884  # Mosquitto占着1883, 本实验用 1884

clients = []                     # 所有已连接的客户端
clients_lock = threading.Lock()  # 多个线程同时改 clients 时要加锁


def broadcast(topic, payload):
    """把消息转发给所有订阅了这个主题的客户端。"""
    # 组装 PUBLISH 报文，遍历 clients，给订阅了该主题的连接各发一份
    packet = mqtt.build_publish(topic, payload)

    with clients_lock:
        targets = list(clients)

    for client in targets:
        if topic in client["subs"]:
            try:
                client["sock"].sendall(packet)
            except OSError:
                # 顺手清理；先判断一下，对方的 finally 可能已经把它删过了
                with clients_lock:
                    if client in clients:
                        clients.remove(client)
                client["sock"].close()




def handle_client(sock, addr):
    """处理一个客户端：CONNECT / SUBSCRIBE / PUBLISH / PINGREQ / DISCONNECT。"""
    # 把新客户端登记到 clients
    client = {"sock": sock, "subs": set()}
    with clients_lock:
        clients.append(client)
    # 循环读取报文，按类型分别处理
    # try/finally 保证：不管是正常告别、突然断线还是程序出错，收尾都会执行
    try:
        while True:
            packet_type, body = mqtt.read_packet(sock)
            if packet_type is None:
                break

            if packet_type == mqtt.CONNECT:
                sock.sendall(mqtt.build_connack())

            elif packet_type == mqtt.SUBSCRIBE:
                packet_id = int.from_bytes(body[0:2], "big")
                topic, _ = mqtt.read_string(body, 2)
                client["subs"].add(topic)   #写入订阅表
                sock.sendall(mqtt.build_suback(packet_id))
                print(f"[Broker] {addr} 订阅了 {topic}")

            elif packet_type == mqtt.PUBLISH:
                topic, offset = mqtt.read_string(body, 0)
                text = body[offset:].decode("utf-8")
                print(f"[Broker] 收到消息 {topic} -> {text}")
                broadcast(topic, text)

            elif packet_type == mqtt.PINGREQ:
                sock.sendall(mqtt.build_pingresp())

            elif packet_type == mqtt.DISCONNECT:
                break


    except OSError:
        # 对方被强制关掉（没走正常告别）时，recv 会抛 ConnectionResetError。
        # 这不是程序出错，安静收场即可，收尾交给下面的 finally
        pass

    finally:
        # 连接结束时从 clients 移除并关闭 socket
        with clients_lock:
            if client in clients:
                clients.remove(client)
        sock.close()
        print(f"[Broker] {addr} 已断开")



def main():
    # 创建 socket、绑定、监听；每来一个连接就开一个线程跑 handle_client
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(5)
    # 每 0.5 秒"空手回来"一次，主线程才有机会处理 Ctrl+C
    server.settimeout(0.5)
    print(f"[Broker] 已启动，监听 {HOST}:{PORT}")

    try:
        while True:
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue

            print(f"[Broker] 新连接 {addr}")
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        print()
        print("[Broker] 收到退出信号，服务已停止")
    finally:
        server.close()


if __name__ == "__main__":
    main()
