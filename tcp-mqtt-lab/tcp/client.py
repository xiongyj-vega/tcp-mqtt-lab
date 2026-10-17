# -*- coding: utf-8 -*-
"""简易 TCP 客户端

目标：连接本机的 TCP 服务端，发送一条文字信息。
运行：python tcp/client.py "要发送的文字"
"""

import socket
import sys

HOST = "127.0.0.1"  # 要和 server.py 里填的一致
PORT = 9000         # 要和 server.py 里填的一致


def main():
    # 从命令行取要发送的文字，没给就用一句默认内容
    if len(sys.argv) > 1:
        message = sys.argv[1]
    else:
        message = "你好，我是 TCP 客户端"

    # 创建 socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 连接服务端
    client_socket.connect((HOST, PORT))
    # 把文字编码成字节发出去
    client_socket.sendall(message.encode("utf-8"))
    # 打印一句确认
    print(f"[客户端] 已发送：{message}")
    # 关闭连接，触发四次挥手
    client_socket.close()


if __name__ == "__main__":
    main()