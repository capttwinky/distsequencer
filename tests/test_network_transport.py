from __future__ import annotations

import asyncio
import ssl
from pathlib import Path

import pytest

from distributed_sequencer.infrastructure.messaging import JsonMessageCodec, MessageEnvelope
from distributed_sequencer.infrastructure.network import (
    AsyncioMessageServer,
    MessageDecodeError,
    NetworkMessageConnection,
    OversizedMessageError,
    build_client_ssl_context,
    build_server_ssl_context,
    open_message_connection,
    serve_messages,
)
from distributed_sequencer.infrastructure.pki import LocalCertificateAuthority


def envelope(message_id: str = "m-1", payload: dict[str, object] | None = None) -> MessageEnvelope:
    return MessageEnvelope(
        schema_version=1,
        message_id=message_id,
        sender_id="coordinator",
        kind="snapshot",
        payload=payload or {"transport_epoch": 1},
    )


def server_port(server: AsyncioMessageServer) -> int:
    sockets = server.sockets
    assert sockets
    return int(sockets[0].getsockname()[1])


@pytest.mark.asyncio
async def test_asyncio_transport_round_trips_message_envelopes() -> None:
    codec = JsonMessageCodec(secrets_by_sender={"coordinator": b"secret"}, max_bytes=1024)

    async def handler(connection: NetworkMessageConnection) -> None:
        received = await connection.receive()
        await connection.send(envelope("ack", {"received": received.message_id}))

    async with await serve_messages("127.0.0.1", 0, codec=codec, handler=handler) as server:
        async with await open_message_connection(
            "127.0.0.1",
            server_port(server),
            codec=codec,
        ) as connection:
            await connection.send(envelope())
            response = await connection.receive()

    assert response.message_id == "ack"
    assert response.payload == {"received": "m-1"}


@pytest.mark.asyncio
async def test_transport_rejects_oversized_declared_frame_before_decoding() -> None:
    codec = JsonMessageCodec(max_bytes=32)

    async def raw_handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        del reader
        writer.write((codec.max_bytes + 1).to_bytes(4, "big"))
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(raw_handler, "127.0.0.1", 0)
    try:
        port = int(server.sockets[0].getsockname()[1])
        async with await open_message_connection("127.0.0.1", port, codec=codec) as connection:
            with pytest.raises(OversizedMessageError, match="incoming message"):
                await connection.receive()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_transport_reports_decode_failures_explicitly() -> None:
    codec = JsonMessageCodec(max_bytes=64)

    async def raw_handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        del reader
        raw = b"not-json"
        writer.write(len(raw).to_bytes(4, "big") + raw)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(raw_handler, "127.0.0.1", 0)
    try:
        port = int(server.sockets[0].getsockname()[1])
        async with await open_message_connection("127.0.0.1", port, codec=codec) as connection:
            with pytest.raises(MessageDecodeError, match="failed to decode"):
                await connection.receive()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_transport_supports_mtls_contexts_from_local_pki(tmp_path: Path) -> None:
    ca = LocalCertificateAuthority(tmp_path / "certs")
    ca.bootstrap_ca()
    server_paths = ca.issue_node_certificate("localhost")
    client_paths = ca.issue_node_certificate("node-client")
    codec = JsonMessageCodec(max_bytes=1024)
    server_context = build_server_ssl_context(
        ca_cert=server_paths.ca_cert,
        cert=server_paths.cert,
        key=server_paths.key,
        require_client_cert=True,
    )
    client_context = build_client_ssl_context(
        ca_cert=client_paths.ca_cert,
        cert=client_paths.cert,
        key=client_paths.key,
    )

    assert server_context.verify_mode is ssl.CERT_REQUIRED
    assert client_context.check_hostname

    async def handler(connection: NetworkMessageConnection) -> None:
        received = await connection.receive()
        await connection.send(envelope("tls-ack", {"received": received.message_id}))

    async with await serve_messages(
        "127.0.0.1",
        0,
        codec=codec,
        handler=handler,
        ssl_context=server_context,
    ) as server:
        async with await open_message_connection(
            "127.0.0.1",
            server_port(server),
            codec=codec,
            ssl_context=client_context,
            server_hostname="localhost",
        ) as connection:
            await connection.send(envelope("tls-message"))
            response = await connection.receive()

    assert response.payload == {"received": "tls-message"}


def test_client_tls_context_requires_cert_and_key_together(tmp_path: Path) -> None:
    ca = LocalCertificateAuthority(tmp_path / "certs")
    ca.bootstrap_ca()
    paths = ca.issue_node_certificate("node-client")

    with pytest.raises(ValueError, match="provided together"):
        build_client_ssl_context(ca_cert=paths.ca_cert, cert=paths.cert)
