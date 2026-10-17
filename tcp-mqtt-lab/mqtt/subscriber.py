# -*- coding: utf-8 -*-
"""MQTT 订阅端（接收并打印）

目标：连接 Broker 并订阅主题，收到消息就打印出来。
运行：python mqtt/subscriber.py
"""

import socket

import mqtt_mini as mqtt

HOST = "127.0.0.1"
PORT = 1884
TOPIC = "lab/demo"  # 要和 publisher.py 里填的一样


def main():
    # 连接 Broker
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    # 每 0.5 秒"空手回来"一次，这样按 Ctrl+C 时主线程有机会处理信号
    sock.settimeout(0.5)

    # 发 CONNECT，等 CONNACK
    sock.sendall(mqtt.build_connect("subscriber-lab"))
    packet_type, _ = mqtt.read_packet(sock)
    if packet_type != mqtt.CONNACK:
        print(f"[订阅端] 连接被拒绝，收到的是 {packet_type}")
        sock.close()
        return

    # 发 SUBSCRIBE，等 SUBACK
    sock.sendall(mqtt.build_subscribe(1, TOPIC))
    packet_type, _ = mqtt.read_packet(sock)
    if packet_type != mqtt.SUBACK:
        print(f"[订阅端] 订阅失败，收到的是 {packet_type}")
        sock.close()
        return
    print(f"[订阅端] 已订阅 {TOPIC}，等待消息……（按 Ctrl+C 退出）")

    # 循环读取报文，收到 PUBLISH 就解析出主题和正文打印
    try:
        while True:
            packet_type, body = mqtt.read_packet(sock)
            if packet_type is None:
                print("[订阅端] Broker 已断开连接")
                break

            if packet_type == mqtt.PUBLISH:
                topic, offset = mqtt.read_string(body, 0)
                text = body[offset:].decode("utf-8")
                print(f"[订阅端] {topic} -> {text}")
    except KeyboardInterrupt:
        print()
        print("[订阅端] 收到退出信号，准备断开……")
    except OSError:
        print("[订阅端] 与Broker的连接已中断")

    # 保护性告别
    try:
        sock.sendall(mqtt.build_disconnect())
    except OSError:
        pass
    sock.close()


if __name__ == "__main__":
    main()
