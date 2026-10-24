#!/usr/bin/env python3
"""Loopback TCP delay for benchmark traffic. Delay is per direction/chunk.

Ledger and observer connections stay direct. This is a real socket-path delay,
not handler sleep, packet loss, bandwidth simulation, or a WAN fidelity claim.
"""
import argparse
import asyncio
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class DelayConfig:
    listen_port: int
    upstream_port: int
    delay_ms: int
    ready: Path


async def serve(args: DelayConfig) -> None:
    async def relay(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while data := await reader.read(65536):
            await asyncio.sleep(args.delay_ms / 1000)
            writer.write(data)
            await writer.drain()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream = None
        tasks = []
        try:
            upstream_reader, upstream = await asyncio.open_connection("127.0.0.1", args.upstream_port)
            tasks = [asyncio.create_task(relay(reader, upstream)), asyncio.create_task(relay(upstream_reader, writer))]
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        except (ConnectionError, OSError):
            pass
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            writer.close()
            if upstream is not None:
                upstream.close()
    async with await asyncio.start_server(handle, "127.0.0.1", args.listen_port) as server:
        args.ready.write_text(str(server.sockets[0].getsockname()[1]) + "\n")
        await server.serve_forever()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--upstream-port", type=int, required=True)
    parser.add_argument("--delay-ms", type=int, required=True)
    parser.add_argument("--ready", required=True)
    args = parser.parse_args()
    if args.delay_ms < 0:
        parser.error("delay must be nonnegative")
    if not 0 <= args.listen_port <= 65535 or not 1 <= args.upstream_port <= 65535:
        parser.error("invalid TCP port")
    asyncio.run(serve(DelayConfig(args.listen_port, args.upstream_port, args.delay_ms, Path(args.ready))))
