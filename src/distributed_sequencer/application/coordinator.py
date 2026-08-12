from __future__ import annotations

from dataclasses import dataclass, field

from distributed_sequencer.application.composition import CompositionEngine
from distributed_sequencer.domain.state import (
    Assignment,
    AuthoritativeSnapshot,
    CompositionContext,
    NodeCapabilities,
    PartLease,
    PhraseReady,
    TransportState,
    VariationPolicy,
)
from distributed_sequencer.infrastructure.messaging import InMemoryBus


@dataclass(slots=True)
class Coordinator:
    composition: CompositionEngine
    bus: InMemoryBus
    nodes: dict[str, NodeCapabilities] = field(default_factory=dict)
    transport_epoch: int = 1
    transport_state: TransportState = TransportState.STOPPED
    tempo_bpm: float = 120.0
    current_bar: int = 0
    lease_bars: int = 16
    generation_by_part: dict[str, int] = field(default_factory=dict)
    desired_assignments: dict[str, Assignment] = field(default_factory=dict)
    readiness: dict[tuple[str, str], PhraseReady] = field(default_factory=dict)

    @property
    def generation(self) -> int:
        return max(self.generation_by_part.values(), default=0)

    def register(self, capabilities: NodeCapabilities) -> None:
        self.nodes[capabilities.node_id] = capabilities

    def mark_current_bar(self, bar: int) -> None:
        if bar < 0:
            raise ValueError("bar must be non-negative")
        self.current_bar = bar

    def start_transport(self) -> None:
        self.transport_state = TransportState.PLAYING

    def restart_transport(self) -> int:
        self.transport_epoch += 1
        self.transport_state = TransportState.STOPPED
        self.desired_assignments.clear()
        self.generation_by_part.clear()
        self.readiness.clear()
        return self.transport_epoch

    def allocate(self, role: str) -> NodeCapabilities:
        compatible = [node for node in self.nodes.values() if role in node.roles]
        if not compatible:
            raise RuntimeError(f"no node can perform role {role!r}")
        return sorted(compatible, key=lambda node: node.node_id)[0]

    def can_reassign(self, part_id: str, *, at_bar: int | None = None) -> bool:
        current = self.desired_assignments.get(part_id)
        if current is None:
            return True
        assert current.lease is not None
        bar = self.current_bar if at_bar is None else at_bar
        return not current.lease.exclusive or bar > current.lease.valid_through_bar

    async def compose_and_assign(
        self,
        context: CompositionContext,
        policy: VariationPolicy,
        *,
        node_id: str | None = None,
    ) -> Assignment:
        phrase = await self.composition.compose(context)
        node = self.nodes[node_id] if node_id is not None else self.allocate(context.role)
        if context.role not in node.roles:
            raise RuntimeError(f"node {node.node_id!r} cannot perform role {context.role!r}")
        existing = self.desired_assignments.get(context.role)
        if (
            existing is not None
            and existing.node_id != node.node_id
            and not self.can_reassign(context.role)
        ):
            raise RuntimeError(
                f"cannot reassign exclusive part {context.role!r} before lease expiration"
            )
        generation = self.generation_by_part.get(context.role, 0) + 1
        self.generation_by_part[context.role] = generation
        valid_from = self.current_bar + 1
        lease = PartLease(
            transport_epoch=self.transport_epoch,
            part_id=context.role,
            node_id=node.node_id,
            assignment_generation=generation,
            valid_from_bar=valid_from,
            valid_through_bar=valid_from + self.lease_bars,
        )
        assignment = Assignment(
            node_id=node.node_id,
            phrase=phrase,
            policy=policy,
            assignment_generation=generation,
            transport_epoch=self.transport_epoch,
            part_id=context.role,
            assignment_id=f"{self.transport_epoch}:{context.role}:{generation}",
            lease=lease,
            message_id=f"assignment:{self.transport_epoch}:{context.role}:{generation}",
        )
        self.desired_assignments[context.role] = assignment
        await self.bus.publish(f"node.{node.node_id}.assignment", assignment)
        return assignment

    def note_ready(self, readiness: PhraseReady) -> None:
        if readiness.transport_epoch != self.transport_epoch:
            return
        self.readiness[(readiness.node_id, readiness.part_id)] = readiness

    def snapshot_for(self, node_id: str) -> AuthoritativeSnapshot:
        assignments = tuple(
            assignment
            for assignment in self.desired_assignments.values()
            if assignment.node_id == node_id
        )
        return AuthoritativeSnapshot(
            transport_epoch=self.transport_epoch,
            transport_state=self.transport_state,
            tempo_bpm=self.tempo_bpm,
            current_bar=self.current_bar,
            assignments=assignments,
            message_id=f"snapshot:{self.transport_epoch}:{node_id}",
        )
