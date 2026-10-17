# -*- coding: utf-8 -*-
"""简易 TCP 服务端

目标：在本机 9000 端口监听，接收客户端发来的文字并打印出来。
运行：python tcp/server.py
"""

import socket

HOST = "127.0.0.1"  # 只在本机通信，用回环地址
PORT = 9000         # 端口号，1024~65535 之间自己选


def main():
    # 创建 socket（IPv4 + TCP），并打开地址复用
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # 绑定 (HOST, PORT)
    server_socket.bind((HOST, PORT))
    # 开始监听，等待别人来连
    server_socket.listen(1)
    print("[服务端] 已启动，正在等待客户端连接……")
    # 接受连接，拿到「连接对象」和「对方地址」
    while True:                              # 外层循环：不断等新的客户端
        conn, addr = server_socket.accept()
        print("[服务端] 客户端已连接：", addr)
        # 循环读取字节 → 解码成文字 → 打印
        while True:                         # 内层循环：读这个客户端的所有信息
            data = conn.recv(1024)
            if not data:                    # 读到空数据 = 对方断开了
                break
            print("[服务端] 收到消息：", data.decode("utf-8"))
        # 对方断开后收尾，回到外循环继续等待
        conn.close()
        print("[服务端] 客户端已断开，继续等待下一个连接……")



if __name__ == "__main__":
    main()
