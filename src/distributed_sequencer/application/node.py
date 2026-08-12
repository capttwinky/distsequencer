from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from distributed_sequencer.application.scheduler import Scheduler
from distributed_sequencer.application.variation import VariationEngine
from distributed_sequencer.domain.music import Phrase
from distributed_sequencer.domain.state import (
    Assignment,
    AuthoritativeSnapshot,
    NodeCapabilities,
    NodeObservedState,
    PhraseReady,
    is_part_authorized,
)
from distributed_sequencer.infrastructure.messaging import InMemoryBus


@dataclass(slots=True)
class SequencerNode:
    capabilities: NodeCapabilities
    bus: InMemoryBus
    variation: VariationEngine
    scheduler: Scheduler
    phrase_buffer: asyncio.Queue[Phrase] = field(default_factory=lambda: asyncio.Queue(maxsize=4))
    last_assignment: Assignment | None = None
    observed: NodeObservedState = field(init=False)
    processed_message_ids: set[str] = field(default_factory=set)
    ready_reports: list[PhraseReady] = field(default_factory=list)
    local_replay_count: int = 0
    _assignment_queue: asyncio.Queue[object] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._assignment_queue = self.bus.subscribe(self.assignment_topic)
        self.observed = NodeObservedState(node_id=self.capabilities.node_id)

    async def receive_once(self) -> Phrase:
        while True:
            message = await self._assignment_queue.get()
            phrase = await self.handle_message(message)
            if phrase is not None:
                return phrase

    async def handle_message(self, message: object) -> Phrase | None:
        if isinstance(message, AuthoritativeSnapshot):
            await self.reconcile(message)
            return None
        if not isinstance(message, Assignment):
            raise TypeError("node received unsupported assignment message")
        return await self.accept_assignment(message)

    async def accept_assignment(self, assignment: Assignment) -> Phrase | None:
        message_id = assignment.message_id
        assert message_id is not None
        if message_id in self.processed_message_ids:
            self.observed.duplicate_message_drops += 1
            return None
        if self.observed.transport_epoch is not None:
            if assignment.transport_epoch < self.observed.transport_epoch:
                self.observed.stale_message_drops += 1
                return None
            if assignment.transport_epoch > self.observed.transport_epoch:
                self._adopt_epoch(assignment.transport_epoch)
        else:
            self.observed.transport_epoch = assignment.transport_epoch

        if assignment.node_id != self.capabilities.node_id:
            self.observed.stale_message_drops += 1
            return None
        assert assignment.part_id is not None
        accepted_generation = self.observed.active_assignment_generation.get(assignment.part_id, 0)
        if assignment.assignment_generation <= accepted_generation:
            self.observed.duplicate_message_drops += 1
            self.processed_message_ids.add(message_id)
            return None

        self.processed_message_ids.add(message_id)
        self.last_assignment = assignment
        self.observed.current_bar = max(self.observed.current_bar, assignment.valid_from_bar)
        phrase = await self.variation.vary(
            assignment.phrase,
            assignment.policy,
            generation=assignment.assignment_generation,
        )
        await self._buffer_phrase(phrase)
        self.observed.active_assignment_generation[assignment.part_id] = (
            assignment.assignment_generation
        )
        self.observed.policy_version = assignment.policy.policy_version
        self._report_ready(assignment, phrase)
        return phrase

    async def play_once(self, *, current_bar: int | None = None) -> Phrase:
        if self.last_assignment is not None:
            bar = self.observed.current_bar if current_bar is None else current_bar
            if not is_part_authorized(
                assignment=self.last_assignment,
                node_id=self.capabilities.node_id,
                transport_epoch=self.observed.transport_epoch,
                current_bar=bar,
            ):
                self.observed.lease_expirations += 1
                raise RuntimeError("node is not authorized to perform this part at the current bar")
            self.observed.current_bar = bar
        if self.phrase_buffer.empty():
            self.observed.buffer_underruns += 1
        phrase = await self.phrase_buffer.get()
        await self.scheduler.play(phrase)
        return phrase

    async def replay_locally(self, *, current_bar: int | None = None) -> Phrase:
        """Offline fallback: vary the last valid canonical phrase again."""
        if self.last_assignment is None:
            raise RuntimeError("node has no cached canonical phrase")
        bar = self.observed.current_bar if current_bar is None else current_bar
        if not is_part_authorized(
            assignment=self.last_assignment,
            node_id=self.capabilities.node_id,
            transport_epoch=self.observed.transport_epoch,
            current_bar=bar,
        ):
            self.observed.lease_expirations += 1
            raise RuntimeError("lease expired; node must relinquish exclusive performance")
        self.local_replay_count += 1
        phrase = await self.variation.vary(
            self.last_assignment.phrase,
            self.last_assignment.policy,
            generation=self.last_assignment.assignment_generation + self.local_replay_count,
        )
        await self._buffer_phrase(phrase)
        self._report_ready(self.last_assignment, phrase)
        return phrase

    async def reconcile(self, snapshot: AuthoritativeSnapshot) -> None:
        if (
            self.observed.transport_epoch is not None
            and snapshot.transport_epoch < self.observed.transport_epoch
        ):
            self.observed.stale_message_drops += 1
            return
        if snapshot.transport_epoch != self.observed.transport_epoch:
            self._adopt_epoch(snapshot.transport_epoch)
        self.observed.current_bar = snapshot.current_bar
        for assignment in snapshot.assignments:
            await self.accept_assignment(assignment)

    @property
    def assignment_topic(self) -> str:
        return f"node.{self.capabilities.node_id}.assignment"

    def _adopt_epoch(self, epoch: int) -> None:
        self.observed.transport_epoch = epoch
        self.observed.active_assignment_generation.clear()
        self.processed_message_ids.clear()
        self.last_assignment = None
        while not self.phrase_buffer.empty():
            self.phrase_buffer.get_nowait()

    async def _buffer_phrase(self, phrase: Phrase) -> None:
        if self.phrase_buffer.full():
            self.phrase_buffer.get_nowait()
        await self.phrase_buffer.put(phrase)
        self.observed.buffered_through_bar = max(
            self.observed.buffered_through_bar,
            self.observed.current_bar + phrase.bars,
        )

    def _report_ready(self, assignment: Assignment, phrase: Phrase) -> None:
        assert assignment.part_id is not None
        ready = PhraseReady(
            node_id=self.capabilities.node_id,
            part_id=assignment.part_id,
            phrase_sequence=phrase.phrase_sequence,
            assignment_generation=assignment.assignment_generation,
            ready_through_bar=self.observed.current_bar + phrase.bars,
            transport_epoch=assignment.transport_epoch,
        )
        self.ready_reports.append(ready)
