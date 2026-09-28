import socket
import threading
import time
import unittest

from transport import Proxy


class TransportTest(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.server = socket.socket()
        self.server.bind(("127.0.0.1", 0))
        self.server.listen()
        self.server.settimeout(0.1)
        self.stopping = threading.Event()

        def serve():
            while not self.stopping.is_set():
                try:
                    stream, _ = self.server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    return

                def echo(stream=stream):
                    with stream:
                        while True:
                            try:
                                data = stream.recv(4096)
                                if not data:
                                    return
                                stream.sendall(data)
                            except OSError:
                                return
                threading.Thread(target=echo, daemon=True).start()

        threading.Thread(target=serve, daemon=True).start()
        self.proxy = Proxy("127.0.0.1", self.server.getsockname()[1],
                           lambda event, **fields: self.events.append(dict(event=event, **fields)))
        self.client = socket.create_connection(("127.0.0.1", self.proxy.port))
        self.client.settimeout(0.3)

    def tearDown(self):
        self.client.close()
        self.proxy.close()
        self.stopping.set()
        self.server.close()

    def test_partition_holds_then_releases_exact_bytes_in_order(self):
        for mode in ("request", "reply", "partition"):
            with self.subTest(mode=mode):
                self.proxy.configure(mode)
                self.client.sendall(b"held")
                with self.assertRaises(socket.timeout):
                    self.client.recv(4)
                self.proxy.configure()
                self.client.sendall(b"live")
                received = b""
                while len(received) < 8:
                    received += self.client.recv(8 - len(received))
                self.assertEqual(received, b"heldlive")
        self.assertTrue(any(event["event"] == "transport_fault_activated" and event["held"]
                            and not event["dropped"] for event in self.events))
        self.assertTrue(any(event["event"] == "transport_held_bytes_released" for event in self.events))

    def test_partition_recovers_multiple_reads_without_loss_or_reordering(self):
        self.proxy.configure("request")
        payload = bytes(range(256)) * 1024
        self.client.sendall(payload)
        with self.assertRaises(socket.timeout):
            self.client.recv(1)
        self.proxy.configure()
        self.client.sendall(b"after-heal")
        expected = payload + b"after-heal"
        received = bytearray()
        self.client.settimeout(2)
        while len(received) < len(expected):
            received.extend(self.client.recv(len(expected) - len(received)))
        self.assertEqual(bytes(received), expected)

    def test_delay_is_measured_in_both_directions(self):
        self.proxy.configure("delay", 80)
        start = time.monotonic()
        self.client.sendall(b"delayed")
        self.assertEqual(self.client.recv(7), b"delayed")
        self.assertGreaterEqual(time.monotonic() - start, 0.14)
        self.assertGreater(self.proxy.snapshot()["fault_bytes"]["request"], 0)
        self.assertGreater(self.proxy.snapshot()["fault_bytes"]["reply"], 0)

    def test_fragmented_commit_is_forwarded_and_reply_withheld(self):
        def frame(kind, payload):
            return kind + (len(payload) + 4).to_bytes(4, "big") + payload

        startup = (8).to_bytes(4, "big") + (196608).to_bytes(4, "big")
        self.client.sendall(startup)
        self.assertEqual(self.client.recv(len(startup)), startup)
        self.proxy.drop_next_commit_reply()
        parsed = frame(b"P", b"statement\0commit\0\0\0")
        self.client.sendall(parsed)
        self.assertEqual(self.client.recv(len(parsed)), parsed)
        self.assertFalse(self.proxy.commit_seen.is_set(), "Parse does not execute COMMIT")
        bound = frame(b"B", b"\0statement\0\0\0\0\0\0\0")
        self.client.sendall(bound)
        self.assertEqual(self.client.recv(len(bound)), bound)
        execute = frame(b"E", b"\0\0\0\0\0")
        self.client.sendall(execute[:5])
        self.assertEqual(self.client.recv(5), execute[:5])
        self.client.sendall(execute[5:])
        self.assertTrue(self.proxy.commit_seen.wait(1))
        with self.assertRaises(socket.timeout):
            self.client.recv(len(execute) - 5)
        self.assertTrue(any(event["event"] == "commit_forwarded_reply_cut" for event in self.events))


if __name__ == "__main__":
    unittest.main()
