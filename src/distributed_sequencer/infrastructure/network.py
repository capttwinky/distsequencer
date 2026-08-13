from __future__ import annotations

import asyncio
import ssl
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from distributed_sequencer.infrastructure.messaging import JsonMessageCodec, MessageEnvelope

_FRAME_HEADER_BYTES = 4


class NetworkTransportError(RuntimeError):
    """Base error for bounded network message transport failures."""


class OversizedMessageError(NetworkTransportError):
    """Raised when a frame declares or encodes more bytes than allowed."""


class MessageDecodeError(NetworkTransportError):
    """Raised when a network frame cannot be decoded into a message envelope."""


@dataclass(slots=True)
class NetworkMessageConnection:
    """Length-prefixed MessageEnvelope stream over asyncio readers/writers."""

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    codec: JsonMessageCodec
    max_message_bytes: int | None = None

    async def send(self, envelope: MessageEnvelope) -> None:
        try:
            encoded = self.codec.encode(envelope)
        except ValueError as exc:
            raise OversizedMessageError("encoded message exceeds configured size limit") from exc
        max_bytes = self._max_bytes
        if len(encoded) > max_bytes:
            raise OversizedMessageError("encoded message exceeds configured size limit")
        self.writer.write(len(encoded).to_bytes(_FRAME_HEADER_BYTES, "big") + encoded)
        await self.writer.drain()

    async def receive(self) -> MessageEnvelope:
        header = await self.reader.readexactly(_FRAME_HEADER_BYTES)
        size = int.from_bytes(header, "big")
        if size > self._max_bytes:
            raise OversizedMessageError("incoming message exceeds configured size limit")
        raw = await self.reader.readexactly(size)
        try:
            return self.codec.decode(raw)
        except (TypeError, ValueError, KeyError, UnicodeDecodeError) as exc:
            raise MessageDecodeError("failed to decode incoming message") from exc

    def close(self) -> None:
        self.writer.close()

    async def wait_closed(self) -> None:
        await self.writer.wait_closed()

    async def __aenter__(self) -> NetworkMessageConnection:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()
        await self.wait_closed()

    @property
    def _max_bytes(self) -> int:
        return self.max_message_bytes or self.codec.max_bytes


ConnectionHandler = Callable[[NetworkMessageConnection], Awaitable[None]]


@dataclass(slots=True)
class AsyncioMessageServer:
    """Asyncio stream server that accepts bounded MessageEnvelope connections."""

    server: asyncio.Server

    @property
    def sockets(self) -> tuple[object, ...]:
        return tuple(self.server.sockets or ())

    async def close(self) -> None:
        self.server.close()
        await self.server.wait_closed()

    async def __aenter__(self) -> AsyncioMessageServer:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        await self.close()


async def open_message_connection(
    host: str,
    port: int,
    *,
    codec: JsonMessageCodec,
    ssl_context: ssl.SSLContext | None = None,
    server_hostname: str | None = None,
    max_message_bytes: int | None = None,
) -> NetworkMessageConnection:
    reader, writer = await asyncio.open_connection(
        host,
        port,
        ssl=ssl_context,
        server_hostname=server_hostname,
        limit=(max_message_bytes or codec.max_bytes) + _FRAME_HEADER_BYTES,
    )
    return NetworkMessageConnection(
        reader=reader,
        writer=writer,
        codec=codec,
        max_message_bytes=max_message_bytes,
    )


async def serve_messages(
    host: str,
    port: int,
    *,
    codec: JsonMessageCodec,
    handler: ConnectionHandler,
    ssl_context: ssl.SSLContext | None = None,
    max_message_bytes: int | None = None,
) -> AsyncioMessageServer:
    async def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connection = NetworkMessageConnection(
            reader=reader,
            writer=writer,
            codec=codec,
            max_message_bytes=max_message_bytes,
        )
        try:
            await handler(connection)
        finally:
            connection.close()
            await connection.wait_closed()

    server = await asyncio.start_server(
        accept,
        host,
        port,
        ssl=ssl_context,
        limit=(max_message_bytes or codec.max_bytes) + _FRAME_HEADER_BYTES,
    )
    return AsyncioMessageServer(server)


def build_server_ssl_context(
    *,
    ca_cert: Path,
    cert: Path,
    key: Path,
    require_client_cert: bool = True,
) -> ssl.SSLContext:
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(certfile=cert, keyfile=key)
    context.load_verify_locations(cafile=ca_cert)
    context.verify_mode = ssl.CERT_REQUIRED if require_client_cert else ssl.CERT_NONE
    return context


def build_client_ssl_context(
    *,
    ca_cert: Path,
    cert: Path | None = None,
    key: Path | None = None,
    check_hostname: bool = True,
) -> ssl.SSLContext:
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = check_hostname
    if cert is not None or key is not None:
        if cert is None or key is None:
            raise ValueError("client certificate and key must be provided together")
        context.load_cert_chain(certfile=cert, keyfile=key)
    return context
