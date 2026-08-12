from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import random
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class QueueOverflow(StrEnum):
    FAIL = "fail"
    DROP_NEWEST = "drop_newest"
    REPLACE_OLDEST = "replace_oldest"


class BackpressureError(RuntimeError):
    pass


@dataclass(slots=True)
class Subscriber:
    queue: asyncio.Queue[object]
    overflow: QueueOverflow


@dataclass(slots=True)
class InMemoryBus:
    """Bounded fan-out bus used by tests and the local simulator.

    The bus is transport only. Durable desired state lives in the coordinator.
    """

    default_maxsize: int = 64
    _subscribers: dict[str, list[Subscriber]] = field(default_factory=lambda: defaultdict(list))

    def subscribe(
        self,
        topic: str,
        *,
        maxsize: int | None = None,
        overflow: QueueOverflow = QueueOverflow.FAIL,
    ) -> asyncio.Queue[object]:
        queue: asyncio.Queue[object] = asyncio.Queue(maxsize=maxsize or self.default_maxsize)
        self._subscribers[topic].append(Subscriber(queue=queue, overflow=overflow))
        return queue

    async def publish(self, topic: str, message: object) -> None:
        for subscriber in tuple(self._subscribers.get(topic, ())):
            if subscriber.queue.full():
                if subscriber.overflow is QueueOverflow.DROP_NEWEST:
                    continue
                if subscriber.overflow is QueueOverflow.REPLACE_OLDEST:
                    subscriber.queue.get_nowait()
                else:
                    raise BackpressureError(f"subscriber queue for {topic!r} is full")
            await subscriber.queue.put(message)


@dataclass(frozen=True, slots=True)
class MessageEnvelope:
    schema_version: int
    message_id: str
    sender_id: str
    kind: str
    payload: dict[str, Any]
    signature: str = ""


@dataclass(frozen=True, slots=True)
class JsonMessageCodec:
    """Bounded JSON codec with optional HMAC authentication.

    This intentionally avoids pickle or object deserialization for network input.
    """

    secrets_by_sender: dict[str, bytes] = field(default_factory=dict)
    max_bytes: int = 64_000

    def encode(self, envelope: MessageEnvelope) -> bytes:
        unsigned = asdict(envelope) | {"signature": ""}
        body = self._canonical_json(unsigned)
        signature = self._signature(envelope.sender_id, body)
        signed = unsigned | {"signature": signature}
        encoded = self._canonical_json(signed)
        if len(encoded) > self.max_bytes:
            raise ValueError("encoded message exceeds configured size limit")
        return encoded

    def decode(self, raw: bytes) -> MessageEnvelope:
        if len(raw) > self.max_bytes:
            raise ValueError("message exceeds configured size limit")
        decoded = json.loads(raw.decode("utf-8"))
        if not isinstance(decoded, dict):
            raise ValueError("message must be a JSON object")
        envelope = MessageEnvelope(
            schema_version=int(decoded["schema_version"]),
            message_id=str(decoded["message_id"]),
            sender_id=str(decoded["sender_id"]),
            kind=str(decoded["kind"]),
            payload=dict(decoded["payload"]),
            signature=str(decoded.get("signature", "")),
        )
        unsigned = asdict(envelope) | {"signature": ""}
        expected = self._signature(envelope.sender_id, self._canonical_json(unsigned))
        if expected and not hmac.compare_digest(expected, envelope.signature):
            raise ValueError("message authentication failed")
        return envelope

    def _signature(self, sender_id: str, body: bytes) -> str:
        secret = self.secrets_by_sender.get(sender_id)
        if secret is None:
            return ""
        return hmac.new(secret, body, hashlib.sha256).hexdigest()

    @staticmethod
    def _canonical_json(value: dict[str, Any]) -> bytes:
        return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


@dataclass(slots=True)
class SimulatedNetwork:
    """Failure-injection transport shim for one-machine distributed tests."""

    bus: InMemoryBus
    latency_seconds: float = 0.0
    jitter_seconds: float = 0.0
    packet_loss: float = 0.0
    duplicate_probability: float = 0.0
    reorder_probability: float = 0.0
    seed: int = 1
    partitions: set[str] = field(default_factory=set)
    _pending_reordered: list[tuple[str, object]] = field(default_factory=list)

    async def publish(self, topic: str, message: object) -> None:
        if topic in self.partitions:
            return
        rng = random.Random(f"{self.seed}:{topic}:{message!r}")
        if rng.random() < self.packet_loss:
            return
        deliveries = 2 if rng.random() < self.duplicate_probability else 1
        for _ in range(deliveries):
            delay = self.latency_seconds + rng.uniform(0.0, self.jitter_seconds)
            if delay > 0:
                await asyncio.sleep(delay)
            if rng.random() < self.reorder_probability:
                self._pending_reordered.append((topic, message))
                continue
            await self.bus.publish(topic, message)
        await self.flush_reordered()

    async def flush_reordered(self) -> None:
        while self._pending_reordered:
            topic, message = self._pending_reordered.pop()
            await self.bus.publish(topic, message)
