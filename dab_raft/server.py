import logging
import pickle

from dab_raft.channel import Channel, ChannelClosed


class Server(logging.Handler):

    data_dir = "data"

    def __init__(self):
        super().__init__()
        self.channel = Channel()
        self.records = {}
        self.uuids = set()
        self.log_index = 0

        try:
            self.load()
        except FileNotFoundError:
            # Allow init defaults on any problem
            pass

    def run(self):
        self.channel.listen()

        while True:
            try:
                record = self.channel.recv_pickle()
            except ChannelClosed:
                break

            self.handler(record)

            # Always send back so client can verify TX
            self.channel.send_pickle(record)

    def shutdown(self):
        self.channel.shutdown()

    def handler(self, record):
        if record.uuid in self.uuids:
            print(f"skipping {record.uuid}, already present")
            return

        self.uuids.add(record.uuid)
        self.emit(record)
        self.save()

    def emit(self, record):
        # name, level, msg
        record.log_index = self.log_index
        self.records[self.log_index] = record
        print(f"Recv [{self.log_index}] {record.uuid}: {record.levelname} {record.msg}")
        self.log_index += 1

    def query(self, log_index):
        record = self.records[log_index]
        print(f"Retrieved #{log_index}-{record.uuid}: {record.levelname} {record.msg}")
        return record

    def load(self):
        with open(f'{self.data_dir}/records.pickle', 'rb') as f:
            self.records = pickle.load(f)
            print(f"Loaded {len(self.records)} records")

        with open(f'{self.data_dir}/uuids.pickle', 'rb') as f:
            self.uuids = pickle.load(f)
            print(f"Loaded {len(self.uuids)} uuids")

        with open(f'{self.data_dir}/index.pickle', 'rb') as f:
            self.log_index = pickle.load(f)
            print(f"Loaded log index: {self.log_index}")

    def save(self):
        with open(f'{self.data_dir}/records.pickle', 'wb') as f:
            pickle.dump(self.records, f, pickle.HIGHEST_PROTOCOL)

        with open(f'{self.data_dir}/uuids.pickle', 'wb') as f:
            pickle.dump(self.uuids, f, pickle.HIGHEST_PROTOCOL)

        with open(f'{self.data_dir}/index.pickle', 'wb') as f:
            pickle.dump(self.log_index, f, pickle.HIGHEST_PROTOCOL)