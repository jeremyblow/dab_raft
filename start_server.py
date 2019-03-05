from dab_raft.server import Server


if __name__ == "__main__":
    s = Server()
    while True:
        try:
            s.run()
        except KeyboardInterrupt:
            s.shutdown()
            break
