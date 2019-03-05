import logging
from time import sleep

from dab_raft.client import Client


log = logging.getLogger(__name__)


def main():
    log.level = logging.DEBUG

    client = Client()

    try:
        client.connect()
    except KeyboardInterrupt:
        client.shutdown()
        return

    log.addHandler(client)

    while True:
        try:
            log.info("hiya!")
            sleep(3)
        except KeyboardInterrupt:
            client.shutdown()
            break


if __name__ == "__main__":
    main()
