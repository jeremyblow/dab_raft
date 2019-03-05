import logging
from time import sleep
from uuid import uuid4

from dab_raft.channel import Channel, ChannelClosed


class Client(logging.Handler):

    retry_delay = 2
    timeout = 2

    def __init__(self):
        super().__init__()
        self.channel = Channel()

    def connect(self):
        connected = False
        while not connected:
            try:
                self.channel.connect()
            except ChannelClosed as e:
                print(f"{e}, retrying in {self.retry_delay}s")
                sleep(self.retry_delay)
            else:
                connected = True

    def emit(self, record):
        record.uuid = uuid4()
        ack = None

        while not ack:
            try:
                self.channel.send_pickle(record)
                ack = self.channel.recv_pickle()
            except ChannelClosed:
                self.connect()

        try:
            assert record.uuid == ack.uuid
        except AssertionError:
            print(f"Emit failed, UUID mismatch ({record.uuid}, {ack.uuid})")
        else:
            print(f"Sent [{ack.log_index}] {record.uuid}: {record.levelname} {record.msg}")

    def shutdown(self):
        self.channel.shutdown()
