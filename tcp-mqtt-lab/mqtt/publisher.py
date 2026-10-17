# -*- coding: utf-8 -*-
"""MQTT 发布端（发送文字）

目标：连接 Broker，往指定主题发布一条文字信息。
运行：python mqtt/publisher.py "要发送的文字"
"""

import socket
import sys

import mqtt_mini as mqtt

HOST = "127.0.0.1"
PORT = 1884
TOPIC = "lab/demo"  # 要和 subscriber.py 里填的一样


def main():
    # 从命令行取要发送的文字
    if len(sys.argv) > 1:
        text = sys.argv[1]
    else:
        text = "你好，我是 MQTT 客户端"
    # 连接 Broker，发 CONNECT，等 CONNACK
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))  # 建立 TCP 连接
    sock.sendall(mqtt.build_connect("publisher-lab"))
    packet_type, _ = mqtt.read_packet(sock)
    if packet_type != mqtt.CONNACK:
        print(f"[发布端] 连接被拒绝，收到的是 {packet_type}")
        sock.close()
        return

    # 发 PUBLISH
    sock.sendall(mqtt.build_publish(TOPIC, text))
    print(f"[发布端] 已发送：{text}")

    # 发 DISCONNECT 断开，再关掉管子
    sock.sendall(mqtt.build_disconnect())
    sock.close()

if __name__ == "__main__":
    main()
