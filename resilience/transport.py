"""Real TCP fault controller; independent request/reply directions and witnesses."""

import socket
import threading
import time


class ClientProtocol:
    """Track prepared statements/portals to distinguish Parse from execution."""

    def __init__(self):
        self.buffer = b""
        self.startup = True
        self.statements = {}
        self.portals = {}

    def feed(self, chunk):
        self.buffer += chunk
        executed = []
        while len(self.buffer) >= 5:
            offset = 0 if self.startup else 1
            length = int.from_bytes(self.buffer[offset:offset + 4], "big")
            if length < 4 or length > 16 * 1024 * 1024:
                # Non-PostgreSQL traffic is useful for transparent-proxy tests.
                self.buffer = b""
                return executed
            total = length + offset
            if len(self.buffer) < total:
                return executed
            frame, self.buffer = self.buffer[:total], self.buffer[total:]
            if self.startup:
                self.startup = length == 8 and frame[4:8] == (80877103).to_bytes(4, "big")
                continue
            kind, payload = frame[:1], frame[5:]
            if kind == b"P":
                name, sql, _ = payload.split(b"\0", 2)
                self.statements[name] = sql
            elif kind == b"B":
                portal, statement, _ = payload.split(b"\0", 2)
                self.portals[portal] = statement
            elif kind == b"E":
                portal = payload.split(b"\0", 1)[0]
                executed.append(self.statements.get(self.portals.get(portal), b""))
            elif kind == b"Q":
                executed.append(payload.rstrip(b"\0"))
        return executed


class Proxy:
    def __init__(self, host, port, record):
        self.upstream = (host, port)
        self.record = record
        self.lock = threading.Lock()
        self.changed = threading.Condition(self.lock)
        self.stop_event = threading.Event()
        self.commit_seen = threading.Event()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.2)
        self.port = self.listener.getsockname()[1]
        self.mode = "pass"
        self.delay_ms = 0
        self.generation = 0
        self.witnessed = set()
        self.sockets = set()
        self.blackholes = set()
        self.arm_commit = False
        self.connection_id = 0
        self.threads = []
        self.bytes = {"request": 0, "reply": 0}
        self.fault_bytes = {"request": 0, "reply": 0}
        self.accept_thread = threading.Thread(target=self._accept, daemon=True)
        self.accept_thread.start()

    def configure(self, mode="pass", delay_ms=0):
        if mode not in {"pass", "request", "reply", "partition", "delay"}:
            raise ValueError(mode)
        with self.lock:
            self.mode, self.delay_ms = mode, delay_ms
            self.generation += 1
            self.blackholes.clear()
            self.changed.notify_all()
        self.record("transport_configured", mode=mode, delay_ms_per_direction=delay_ms)

    def drop_next_commit_reply(self):
        with self.lock:
            self.arm_commit = True
            self.commit_seen.clear()
        self.record("commit_reply_loss_armed")

    def cut_connections(self):
        with self.lock:
            sockets = list(self.sockets)
        for stream in sockets:
            self._close(stream)
        with self.changed:
            self.changed.notify_all()
        self.record("connections_cut", sockets=len(sockets))

    def _accept(self):
        while not self.stop_event.is_set():
            try:
                client, _ = self.listener.accept()
            except (TimeoutError, socket.timeout):
                continue
            except OSError:
                return
            try:
                upstream = socket.create_connection(self.upstream, timeout=2)
            except OSError:
                client.close()
                continue
            client.settimeout(0.2)
            upstream.settimeout(0.2)
            with self.lock:
                self.sockets.update((client, upstream))
                self.connection_id += 1
                identity = self.connection_id
            self.threads = [thread for thread in self.threads if thread.is_alive()]
            for source, target, direction in (
                (client, upstream, "request"), (upstream, client, "reply")
            ):
                thread = threading.Thread(target=self._relay,
                                          args=(source, target, direction, identity), daemon=True)
                self.threads.append(thread)
                thread.start()

    def _relay(self, source, target, direction, identity):
        protocol = ClientProtocol()
        try:
            while not self.stop_event.is_set():
                try:
                    data = source.recv(65536)
                except (TimeoutError, socket.timeout):
                    continue
                if not data:
                    break
                executed = protocol.feed(data) if direction == "request" else []
                with self.lock:
                    self.bytes[direction] += len(data)
                    mode, delay, generation = self.mode, self.delay_ms, self.generation
                    commit = self.arm_commit and any(sql.strip().rstrip(b";").lower() == b"commit" for sql in executed)
                    if commit:
                        self.arm_commit = False
                        self.blackholes.add(identity)
                    # A partition holds reliable stream bytes. Dropping them
                    # while forwarding later bytes permanently corrupts the
                    # PostgreSQL protocol and cannot model TCP recovery.
                    drop = direction == "reply" and identity in self.blackholes
                    hold = mode in (direction, "partition") and not drop
                    witness = (generation, direction, identity)
                    first = (drop or hold or delay) and witness not in self.witnessed
                    if first:
                        self.witnessed.add(witness)
                    if drop or hold or delay:
                        self.fault_bytes[direction] += len(data)
                if first:
                    self.record("transport_fault_activated", direction=direction,
                                connection=identity, dropped=drop, held=hold, delay_ms=delay,
                                bytes=len(data), generation=generation)
                if hold:
                    # At most this 64 KiB read is retained per direction. Do not
                    # recv again while partitioned: socket buffers apply bounded
                    # backpressure, and recovery forwards the same bytes first.
                    with self.changed:
                        while self.mode in (direction, "partition") and not self.stop_event.is_set():
                            if source.fileno() < 0 or target.fileno() < 0:
                                return
                            self.changed.wait(timeout=0.2)
                    if self.stop_event.is_set():
                        return
                    self.record("transport_held_bytes_released", direction=direction,
                                connection=identity, bytes=len(data), generation=generation)
                if delay:
                    self.stop_event.wait(delay / 1000)
                if not drop:
                    target.sendall(data)
                if commit:
                    # Forward first, then report the witness. A direct DB receipt
                    # check is still required to prove that this commit happened.
                    self.record("commit_forwarded_reply_cut", connection=identity)
                    self.commit_seen.set()
        except OSError:
            pass
        finally:
            self._close(source)
            self._close(target)
            with self.lock:
                self.sockets.discard(source)
                self.sockets.discard(target)

    @staticmethod
    def _close(stream):
        try:
            stream.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        stream.close()

    def close(self):
        self.stop_event.set()
        with self.changed:
            self.changed.notify_all()
        self.listener.close()
        self.accept_thread.join(timeout=3)
        self.cut_connections()
        for thread in self.threads:
            thread.join(timeout=3)

    def snapshot(self):
        with self.lock:
            return {"bytes": dict(self.bytes), "fault_bytes": dict(self.fault_bytes),
                    "connections_open": len(self.sockets) // 2}
