import json
import pickle
import socket
import struct


class ChannelClosed(Exception):
    pass


class Channel:

    # todo: implement multiple clients for listener

    def __init__(self):
        self.sock = None
        self.conn = None

    def connect(self, address='127.0.0.1', port=4444):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.sock.connect((address, port))
        except (ConnectionRefusedError, OSError) as e:
            raise ChannelClosed(f"Failed to connect to {address}, {e}")
        else:
            self.conn = self.sock
            print(f"Connected to {address}:{port}")

    def listen(self, address='127.0.0.1', port=4444):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Allow port reuse if stuck in TIME_WAIT
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.sock.bind((address, port))
        self.sock.listen(1)
        self.conn, _ = self.sock.accept()

    def recv(self):
        try:
            m_len = self.conn.recv(4)
        except ConnectionResetError:
            raise ChannelClosed

        if not m_len:
            raise ChannelClosed

        return self.conn.recv(struct.unpack('>I', m_len)[0])

    def recv_json(self):
        return json.loads(self.recv())

    def recv_pickle(self):
        return pickle.loads(self.recv())

    def recv_struct(self):
        return struct.unpack('s', self.recv())[0]

    # def _recv_exactly(self, m_size):
    #     fragments = []
    #     while m_size > 0:
    #         fragment = self.sock.recv(m_size)
    #         if not fragment:
    #             raise IOError(f"Recv failed, {m_size} remaining to be read")
    #         fragments.append(fragment)
    #         m_size -= len(fragment)
    #     return b''.join(fragments)

    def send(self, msg: bytes):
        self.conn.send(struct.pack('>I', len(msg)))
        self.conn.send(msg)
        return msg

    def send_json(self, data):
        msg = json.dumps(data)
        return self.send(msg.encode())

    def send_pickle(self, data):
        msg = pickle.dumps(data)
        return self.send(msg)

    def send_struct(self, data):
        msg = struct.pack('s', data.encode())
        return self.send(msg)

    def shutdown(self):
        if self.sock:
            self.sock.close()
            print("Channel sock closed")
        if self.conn:
            self.conn.close()
            print("Channel conn closed")
