# -*- coding: utf-8 -*-
"""极简 MQTT 报文工具

把 MQTT 报文的编解码集中放在这里，broker / subscriber / publisher 共用。
不需要单独运行，被其他文件导入使用。
"""

# 用来识别"读取超时"这类异常
import socket

# MQTT 3.1.1 的报文类型编号
CONNECT = 1
CONNACK = 2
PUBLISH = 3
SUBSCRIBE = 8
SUBACK = 9
PINGREQ = 12
PINGRESP = 13
DISCONNECT = 14


# ---------- 基础工具 ----------

def encode_length(n):
    """把剩余长度编码成 MQTT 的变长字节。"""
    result = b""
    while True:
        byte = n % 128 #取这个数字最低的7位（相当于"个位"）
        n = n //128 #把已取走的7位丢掉
        if n > 0:
            byte = byte | 0x80 #后面还有字节，把最高位（第8位）置1
        result += bytes([byte])
        if n == 0:
            break
    return result
    # 每字节低 7 位存数据，最高位表示后面还有没有字节




def recv_exact(sock, size):
    """从 socket 精确读取 size 个字节，读不满就继续读。"""
    data = b""
    while len(data) < size: #对付拆包
        try:
            chunk= sock.recv(size - len(data))
        except socket.timeout:
            # 只是暂时没数据：已经收到的部分留在 data 里，回头继续等。
            # 这一下"空手回来"，也给了主线程处理 Ctrl+C 的机会。
            continue
        if not chunk:
            return None
        data += chunk
    return data

    # TCP 是字节流，一次 recv 不保证读满，需要循环



def read_packet(sock):
    """读取一个完整报文，返回 (报文类型, 报文内容)；断开时返回 (None, None)。"""
    head = recv_exact(sock, 1)
    if head is None:
        return None, None
    packet_type = head[0] >>4

    length = 0
    multiplier = 1
    while True:
        b = recv_exact(sock,1)
        if b is None:
            return None, None
        length +=(b[0] & 0x7F) * multiplier
        if (b[0] & 0x80) == 0:
            break
        multiplier *= 128

    body = b""
    if length > 0:
        body = recv_exact(sock, length)
        if body is None:
            return None, None
    return packet_type, body


    # 先读 1 字节取高 4 位得到类型，再解析变长的剩余长度，最后按长度读完内容



def read_string(data, offset):
    """读取 MQTT 字符串（前 2 字节是长度），返回 (字符串, 新位置)。"""
    length = int.from_bytes(data[offset:offset + 2], "big")
    text = data[offset + 2:offset + 2 + length].decode("utf-8")
    return text, offset + 2 + length

    # 先取 2 字节算出长度，再按长度切出内容并解码



# ---------- 客户端用的组装函数 ----------

def build_connect(client_id):
    """组装 CONNECT 报文（协议名 + 版本 + 连接标志 + 心跳 + 客户端 ID）。"""
    variable_header = (
        b"\x00\x04MQTT"
        + b"\x04"
        + b"\x02"
        + (60).to_bytes(2, "big")
    )
    payload = client_id.encode("utf-8")
    payload = len(payload).to_bytes(2, "big") + payload
    body = variable_header + payload
    return b"\x10" + encode_length(len(body)) + body


def build_subscribe(packet_id, topic):
    """组装 SUBSCRIBE 报文，QoS 用 0。"""
    topic_bytes = topic.encode("utf-8")
    body = (
        packet_id.to_bytes(2, "big")
        + len(topic_bytes).to_bytes(2, "big")
        + topic_bytes
        + b"\x00"
    )
    return b"\x82" + encode_length(len(body)) + body


def build_publish(topic, payload, qos=0, packet_id=1):
    """组装 PUBLISH 报文。"""
    topic_bytes = topic.encode("utf-8")
    body = len(topic_bytes).to_bytes(2, "big") + topic_bytes

    if qos > 0:
        body += packet_id.to_bytes(2, "big")   # ← QoS>0 必须有消息 ID

    body += payload.encode("utf-8")

    first_byte = 0x30 | (qos << 1)   # 把 qos 编进标志位
    return bytes([first_byte]) + encode_length(len(body)) + body

def build_disconnect():
    """组装 DISCONNECT 报文。"""
    return b"\xe0\x00"


# ---------- 服务端用的组装函数 ----------

def build_connack():
    """组装 CONNACK（连接确认）报文。"""
    return b"\x20\x02\x00\x00"


def build_suback(packet_id):
    """组装 SUBACK（订阅确认）报文。"""
    return b"\x90\x03" + packet_id.to_bytes(2, "big") + b"\x00"


def build_pingresp():
    """组装 PINGRESP（心跳回应）报文。"""
    return b"\xd0\x00"
