from __future__ import annotations

import asyncio
import json
import ssl
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field

from distributed_sequencer.adapters.synth import OscSynthBackend, RecordingSynth
from distributed_sequencer.application.composition import (
    CompositionEngine,
    DensityCritic,
    ProceduralCompositionModel,
    RegisterCritic,
)
from distributed_sequencer.application.coordinator import Coordinator
from distributed_sequencer.application.ha import (
    ConsensusCoordinatorService,
    ConsensusCoordinatorSettings,
    NotLeaderError,
)
from distributed_sequencer.application.node import SequencerNode
from distributed_sequencer.application.scheduler import Scheduler
from distributed_sequencer.application.variation import VariationEngine
from distributed_sequencer.domain.state import (
    Assignment,
    CompositionContext,
    NodeCapabilities,
    VariationPolicy,
)
from distributed_sequencer.infrastructure.clock import AsyncioClock
from distributed_sequencer.infrastructure.messaging import (
    InMemoryBus,
    JsonMessageCodec,
    MessageEnvelope,
)
from distributed_sequencer.infrastructure.network import (
    NetworkMessageConnection,
    build_client_ssl_context,
    build_server_ssl_context,
    open_message_connection,
    serve_messages,
)
from distributed_sequencer.runtime.config import (
    CoordinatorRuntimeConfig,
    Endpoint,
    NodeRuntimeConfig,
    TransportRuntimeConfig,
)
from distributed_sequencer.runtime.serde import (
    assignment_from_payload,
    assignment_to_payload,
    phrase_ready_from_payload,
    phrase_ready_to_payload,
)

JsonObject = dict[str, object]
StateProvider = Callable[[], Mapping[str, object]]


@dataclass(slots=True)
class HttpStatusServer:
    endpoint: Endpoint
    routes: Mapping[str, StateProvider]
    _server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle,
            self.endpoint.host,
            self.endpoint.port,
        )

    async def close(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            request_line = await reader.readline()
            parts = request_line.decode("ascii", errors="replace").strip().split()
            path = parts[1] if len(parts) >= 2 else "/"
            while True:
                header = await reader.readline()
                if header in {b"\r\n", b"\n", b""}:
                    break
            status = "200 OK"
            provider = self.routes.get(path)
            if provider is None:
                status = "404 Not Found"
                body: Mapping[str, object] = {"status": "not_found", "path": path}
            else:
                body = provider()
            raw = json.dumps(body, sort_keys=True).encode("utf-8")
            writer.write(
                "\r\n".join(
                    [
                        f"HTTP/1.1 {status}",
                        "Content-Type: application/json",
                        f"Content-Length: {len(raw)}",
                        "Connection: close",
                        "",
                        "",
                    ]
                ).encode("ascii")
                + raw
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


@dataclass(slots=True)
class CoordinatorRuntime:
    config: CoordinatorRuntimeConfig
    ready_endpoint: Endpoint
    coordinator: Coordinator = field(init=False)
    ha_service: ConsensusCoordinatorService | None = field(init=False, default=None)
    connections: dict[str, NetworkMessageConnection] = field(default_factory=dict)
    last_seen_by_node: dict[str, float] = field(default_factory=dict)
    node_tasks: dict[str, asyncio.Task[None]] = field(default_factory=dict)
    _message_counter: int = 0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def __post_init__(self) -> None:
        composition = CompositionEngine(
            ProceduralCompositionModel(),
            critics=(DensityCritic(), RegisterCritic()),
        )
        self.coordinator = Coordinator(
            composition=composition,
            bus=InMemoryBus(),
            tempo_bpm=self.config.tempo_bpm,
            lease_bars=self.config.lease_bars,
        )
        if self.config.ha.enabled:
            assert self.config.ha.local_member_id is not None
            settings = ConsensusCoordinatorSettings.from_member_ids(
                self.config.ha.member_ids,
                local_member_id=self.config.ha.local_member_id,
                storage_dir=self.config.ha.storage_dir,
                bootstrap_leader_id=self.config.ha.bootstrap_leader_id,
            )
            self.ha_service = ConsensusCoordinatorService.create(self.coordinator, settings)

    async def run_forever(self) -> None:
        codec = JsonMessageCodec(max_bytes=self.config.transport.max_message_bytes)
        ssl_context = _server_ssl_context(self.config.transport)
        ready = HttpStatusServer(
            self.ready_endpoint,
            routes={"/readyz": self.ready_state, "/snapshot": self.snapshot_state},
        )
        await self._start_transport()
        await ready.start()
        monitor = asyncio.create_task(self._monitor_node_liveness())
        async with await serve_messages(
            self.config.listen.host,
            self.config.listen.port,
            codec=codec,
            handler=self._handle_connection,
            ssl_context=ssl_context,
            max_message_bytes=self.config.transport.max_message_bytes,
        ):
            try:
                await asyncio.Event().wait()
            finally:
                monitor.cancel()
                try:
                    await monitor
                except asyncio.CancelledError:
                    pass
                await ready.close()

    def ready_state(self) -> Mapping[str, object]:
        return {
            "status": "ready",
            "role": "coordinator",
            "transport_epoch": self.coordinator.transport_epoch,
            "connected_nodes": sorted(self.connections),
        }

    def snapshot_state(self) -> Mapping[str, object]:
        return {
            "status": "ready",
            "role": "coordinator",
            "transport_epoch": self.coordinator.transport_epoch,
            "transport_state": self.coordinator.transport_state.value,
            "current_bar": self.coordinator.current_bar,
            "assignments": {
                part: _assignment_summary(assignment)
                for part, assignment in sorted(self.coordinator.desired_assignments.items())
            },
            "readiness": {
                f"{node_id}:{part_id}": {
                    "node_id": ready.node_id,
                    "part_id": ready.part_id,
                    "assignment_generation": ready.assignment_generation,
                    "ready_through_bar": ready.ready_through_bar,
                    "transport_epoch": ready.transport_epoch,
                }
                for (node_id, part_id), ready in sorted(self.coordinator.readiness.items())
            },
        }

    async def _start_transport(self) -> None:
        if self.ha_service is not None:
            await self.ha_service.replay_committed()
            if self.ha_service.leader_id is None:
                self.ha_service.elect_leader(
                    self.config.ha.bootstrap_leader_id or self.config.ha.local_member_id
                )
            try:
                await self.ha_service.start_transport()
            except NotLeaderError:
                pass
        else:
            self.coordinator.start_transport()

    async def _handle_connection(self, connection: NetworkMessageConnection) -> None:
        node_id: str | None = None
        try:
            first = await connection.receive()
            if first.kind != "register_node":
                await connection.send(self._error("expected register_node as first message"))
                return
            capabilities = _capabilities_from_payload(first.payload)
            node_id = capabilities.node_id
            async with self._lock:
                await self._register_node(capabilities, connection)
                await self._assign_configured_parts()
            while True:
                message = await connection.receive()
                self.last_seen_by_node[node_id] = time.monotonic()
                if message.kind == "phrase_ready":
                    self.coordinator.note_ready(phrase_ready_from_payload(message.payload))
                elif message.kind == "heartbeat":
                    continue
                else:
                    await connection.send(self._error(f"unsupported message kind {message.kind!r}"))
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            if node_id is not None:
                async with self._lock:
                    await self._node_disconnected(node_id)

    async def _register_node(
        self,
        capabilities: NodeCapabilities,
        connection: NetworkMessageConnection,
    ) -> None:
        self.connections[capabilities.node_id] = connection
        self.last_seen_by_node[capabilities.node_id] = time.monotonic()
        if self.ha_service is None:
            self.coordinator.register(capabilities)
        else:
            await self.ha_service.register(capabilities)

    async def _assign_configured_parts(self) -> None:
        for part in self.config.parts:
            existing = self.coordinator.desired_assignments.get(part)
            if existing is not None and existing.node_id in self.connections:
                continue
            node_id = self._select_connected_node(part)
            if node_id is None:
                continue
            assignment = await self._compose_and_assign(part, node_id)
            await self._send_assignment(assignment)

    async def _node_disconnected(self, node_id: str) -> None:
        connection = self.connections.pop(node_id, None)
        self.last_seen_by_node.pop(node_id, None)
        if connection is not None:
            connection.close()
        lost = [
            assignment
            for assignment in self.coordinator.desired_assignments.values()
            if assignment.node_id == node_id
        ]
        for assignment in lost:
            fallback_node_id = self._select_connected_node(
                assignment.part_id or assignment.phrase.role
            )
            if fallback_node_id is None:
                continue
            self.coordinator.mark_current_bar(
                max(self.coordinator.current_bar, assignment.valid_through_bar + 1)
            )
            replacement = await self._compose_and_assign(
                assignment.part_id or assignment.phrase.role,
                fallback_node_id,
            )
            await self._send_assignment(replacement)

    async def _monitor_node_liveness(self) -> None:
        stale_after_seconds = 1.5
        while True:
            await asyncio.sleep(0.25)
            now = time.monotonic()
            stale = [
                node_id
                for node_id, last_seen in self.last_seen_by_node.items()
                if now - last_seen > stale_after_seconds
            ]
            for node_id in stale:
                async with self._lock:
                    if node_id in self.connections:
                        await self._node_disconnected(node_id)

    async def _compose_and_assign(self, part: str, node_id: str) -> Assignment:
        context = CompositionContext(
            role=part,
            root_pitch=_default_root_pitch(part),
            desired_density=_default_density(part),
        )
        policy = VariationPolicy(velocity_jitter=4 if part == "bass" else 6)
        if self.ha_service is None:
            return await self.coordinator.compose_and_assign(context, policy, node_id=node_id)
        return await self.ha_service.compose_and_assign(context, policy, node_id=node_id)

    async def _send_assignment(self, assignment: Assignment) -> None:
        connection = self.connections.get(assignment.node_id)
        if connection is None:
            return
        await connection.send(
            self._envelope(
                sender_id="coordinator",
                kind="assignment",
                payload=assignment_to_payload(assignment),
            )
        )

    def _select_connected_node(self, part: str) -> str | None:
        candidates = [
            node_id
            for node_id, capabilities in self.coordinator.nodes.items()
            if node_id in self.connections and part in capabilities.roles
        ]
        return sorted(candidates)[0] if candidates else None

    def _envelope(self, *, sender_id: str, kind: str, payload: JsonObject) -> MessageEnvelope:
        self._message_counter += 1
        return MessageEnvelope(
            schema_version=1,
            message_id=f"{sender_id}:{kind}:{self._message_counter}",
            sender_id=sender_id,
            kind=kind,
            payload=payload,
        )

    def _error(self, message: str) -> MessageEnvelope:
        return self._envelope(sender_id="coordinator", kind="error", payload={"message": message})


@dataclass(slots=True)
class NodeRuntime:
    config: NodeRuntimeConfig
    ready_endpoint: Endpoint
    node: SequencerNode = field(init=False)
    connected: bool = False
    assignments_received: int = 0
    _message_counter: int = 0

    def __post_init__(self) -> None:
        synth = (
            OscSynthBackend(self.config.synth.osc_host, self.config.synth.osc_port)
            if self.config.synth.backend == "osc"
            else RecordingSynth()
        )
        self.node = SequencerNode(
            capabilities=NodeCapabilities(
                self.config.node_id,
                frozenset(self.config.parts),
                max_polyphony=self.config.max_polyphony,
                learned_variation=self.config.learned_variation,
            ),
            bus=InMemoryBus(),
            variation=VariationEngine(seed=sum(ord(char) for char in self.config.node_id)),
            scheduler=Scheduler(synth, AsyncioClock()),
        )

    async def run_forever(self) -> None:
        ready = HttpStatusServer(self.ready_endpoint, routes={"/readyz": self.ready_state})
        await ready.start()
        ssl_context = _client_ssl_context(self.config.transport)
        codec = JsonMessageCodec(max_bytes=self.config.transport.max_message_bytes)
        try:
            async with await open_message_connection(
                self.config.coordinator_url.host,
                self.config.coordinator_url.port,
                codec=codec,
                ssl_context=ssl_context,
                server_hostname=(
                    None
                    if self.config.transport.insecure_dev_mode
                    else self.config.coordinator_url.host
                ),
                max_message_bytes=self.config.transport.max_message_bytes,
            ) as connection:
                send_lock = asyncio.Lock()

                async def send(envelope: MessageEnvelope) -> None:
                    async with send_lock:
                        await connection.send(envelope)

                await send(self._register_envelope())
                self.connected = True
                heartbeat = asyncio.create_task(self._send_heartbeats(send))
                while True:
                    message = await connection.receive()
                    if message.kind == "assignment":
                        assignment = assignment_from_payload(message.payload)
                        phrase = await self.node.accept_assignment(assignment)
                        if phrase is not None:
                            self.assignments_received += 1
                            await send(self._ready_envelope())
                    elif message.kind == "error":
                        raise RuntimeError(str(message.payload.get("message", "coordinator error")))
        finally:
            self.connected = False
            if "heartbeat" in locals():
                heartbeat.cancel()
                try:
                    await heartbeat
                except asyncio.CancelledError:
                    pass
            await ready.close()

    def ready_state(self) -> Mapping[str, object]:
        return {
            "status": "ready" if self.connected and self.assignments_received > 0 else "starting",
            "role": "node",
            "node_id": self.config.node_id,
            "parts": sorted(self.config.parts),
            "assignments_received": self.assignments_received,
            "transport_epoch": self.node.observed.transport_epoch,
        }

    def _register_envelope(self) -> MessageEnvelope:
        return self._envelope(
            kind="register_node",
            payload={
                "node_id": self.node.capabilities.node_id,
                "roles": sorted(self.node.capabilities.roles),
                "max_polyphony": self.node.capabilities.max_polyphony,
                "learned_variation": self.node.capabilities.learned_variation,
            },
        )

    def _ready_envelope(self) -> MessageEnvelope:
        if not self.node.ready_reports:
            raise RuntimeError("node has no ready report to send")
        return self._envelope(
            kind="phrase_ready",
            payload=phrase_ready_to_payload(self.node.ready_reports[-1]),
        )

    async def _send_heartbeats(
        self,
        send: Callable[[MessageEnvelope], Awaitable[None]],
    ) -> None:
        while True:
            await asyncio.sleep(0.5)
            await send(self._envelope(kind="heartbeat", payload={"node_id": self.config.node_id}))

    def _envelope(self, *, kind: str, payload: JsonObject) -> MessageEnvelope:
        self._message_counter += 1
        return MessageEnvelope(
            schema_version=1,
            message_id=f"{self.config.node_id}:{kind}:{self._message_counter}",
            sender_id=self.config.node_id,
            kind=kind,
            payload=payload,
        )


async def run_coordinator_daemon(
    config: CoordinatorRuntimeConfig,
    *,
    ready_endpoint: Endpoint,
) -> None:
    await CoordinatorRuntime(config, ready_endpoint).run_forever()


async def run_node_daemon(config: NodeRuntimeConfig, *, ready_endpoint: Endpoint) -> None:
    await NodeRuntime(config, ready_endpoint).run_forever()


def _capabilities_from_payload(payload: Mapping[str, object]) -> NodeCapabilities:
    roles = payload["roles"]
    if not isinstance(roles, list):
        raise ValueError("roles must be a list")
    return NodeCapabilities(
        node_id=str(payload["node_id"]),
        roles=frozenset(str(role) for role in roles),
        max_polyphony=_int(payload["max_polyphony"], "max_polyphony"),
        learned_variation=_bool(payload["learned_variation"], "learned_variation"),
    )


def _assignment_summary(assignment: Assignment) -> Mapping[str, object]:
    return {
        "assignment_id": assignment.assignment_id,
        "node_id": assignment.node_id,
        "part_id": assignment.part_id,
        "generation": assignment.assignment_generation,
        "transport_epoch": assignment.transport_epoch,
        "valid_from_bar": assignment.valid_from_bar,
        "valid_through_bar": assignment.valid_through_bar,
        "phrase_sequence": assignment.phrase.phrase_sequence,
    }


def _server_ssl_context(config: TransportRuntimeConfig) -> ssl.SSLContext | None:
    if config.insecure_dev_mode:
        return None
    if config.ca_cert is None or config.cert is None or config.key is None:
        raise ValueError("secure coordinator transport requires ca_cert, cert, and key")
    return build_server_ssl_context(ca_cert=config.ca_cert, cert=config.cert, key=config.key)


def _client_ssl_context(config: TransportRuntimeConfig) -> ssl.SSLContext | None:
    if config.insecure_dev_mode:
        return None
    if config.ca_cert is None:
        raise ValueError("secure node transport requires ca_cert")
    return build_client_ssl_context(ca_cert=config.ca_cert, cert=config.cert, key=config.key)


def _default_root_pitch(part: str) -> int:
    if part == "bass":
        return 36
    if part == "lead":
        return 60
    return 48


def _default_density(part: str) -> float:
    return 0.75 if part == "bass" else 1.0


def _int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _bool(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value
