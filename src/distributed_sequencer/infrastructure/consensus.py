from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Protocol


class RaftRole(StrEnum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True, slots=True)
class RaftLogEntry:
    term: int
    index: int
    command_id: str
    command: str


@dataclass(frozen=True, slots=True)
class RequestVoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True, slots=True)
class RequestVoteResponse:
    term: int
    vote_granted: bool


@dataclass(frozen=True, slots=True)
class AppendEntriesRequest:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: tuple[RaftLogEntry, ...]
    leader_commit: int


@dataclass(frozen=True, slots=True)
class AppendEntriesResponse:
    term: int
    success: bool
    match_index: int


@dataclass(frozen=True, slots=True)
class RaftPersistentState:
    current_term: int = 0
    voted_for: str | None = None
    log: tuple[RaftLogEntry, ...] = ()


class RaftStorage(Protocol):
    def load(self) -> RaftPersistentState: ...

    def save(self, state: RaftPersistentState) -> None: ...


@dataclass(slots=True)
class MemoryRaftStorage:
    state: RaftPersistentState = RaftPersistentState()

    def load(self) -> RaftPersistentState:
        return self.state

    def save(self, state: RaftPersistentState) -> None:
        self.state = state


@dataclass(frozen=True, slots=True)
class JsonRaftStorage:
    path: Path

    def load(self) -> RaftPersistentState:
        if not self.path.exists():
            return RaftPersistentState()
        decoded = json.loads(self.path.read_text(encoding="utf-8"))
        return RaftPersistentState(
            current_term=int(decoded["current_term"]),
            voted_for=decoded["voted_for"],
            log=tuple(RaftLogEntry(**entry) for entry in decoded["log"]),
        )

    def save(self, state: RaftPersistentState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(asdict(state), sort_keys=True), encoding="utf-8")


@dataclass(slots=True)
class RaftNode:
    node_id: str
    peer_ids: tuple[str, ...]
    storage: RaftStorage
    role: RaftRole = RaftRole.FOLLOWER
    leader_id: str | None = None
    commit_index: int = 0
    last_applied: int = 0
    _state: RaftPersistentState = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._state = self.storage.load()

    @property
    def current_term(self) -> int:
        return self._state.current_term

    @property
    def voted_for(self) -> str | None:
        return self._state.voted_for

    @property
    def log(self) -> tuple[RaftLogEntry, ...]:
        return self._state.log

    @property
    def last_log_index(self) -> int:
        return self.log[-1].index if self.log else 0

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def start_election(self) -> RequestVoteRequest:
        self.role = RaftRole.CANDIDATE
        self.leader_id = None
        self._persist(
            RaftPersistentState(
                current_term=self.current_term + 1,
                voted_for=self.node_id,
                log=self.log,
            )
        )
        return RequestVoteRequest(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=self.last_log_index,
            last_log_term=self.last_log_term,
        )

    def become_leader(self) -> None:
        if self.role is not RaftRole.CANDIDATE:
            raise RuntimeError("only a candidate can become leader")
        self.role = RaftRole.LEADER
        self.leader_id = self.node_id

    def handle_request_vote(self, request: RequestVoteRequest) -> RequestVoteResponse:
        if request.term < self.current_term:
            return RequestVoteResponse(term=self.current_term, vote_granted=False)
        if request.term > self.current_term:
            self._step_down(request.term)
        can_vote = self.voted_for in (None, request.candidate_id)
        up_to_date = request.last_log_term > self.last_log_term or (
            request.last_log_term == self.last_log_term
            and request.last_log_index >= self.last_log_index
        )
        granted = can_vote and up_to_date
        if granted:
            self._persist(
                RaftPersistentState(
                    current_term=self.current_term,
                    voted_for=request.candidate_id,
                    log=self.log,
                )
            )
        return RequestVoteResponse(term=self.current_term, vote_granted=granted)

    def append_local_command(self, command_id: str, command: str) -> RaftLogEntry:
        if self.role is not RaftRole.LEADER:
            raise RuntimeError("only the leader may append commands")
        entry = RaftLogEntry(
            term=self.current_term,
            index=self.last_log_index + 1,
            command_id=command_id,
            command=command,
        )
        self._persist(
            RaftPersistentState(
                current_term=self.current_term,
                voted_for=self.voted_for,
                log=(*self.log, entry),
            )
        )
        return entry

    def append_entries_request(
        self,
        follower: RaftNode,
        *,
        entries: tuple[RaftLogEntry, ...],
    ) -> AppendEntriesRequest:
        prev_index = entries[0].index - 1 if entries else follower.last_log_index
        return AppendEntriesRequest(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_index,
            prev_log_term=self.term_at(prev_index),
            entries=entries,
            leader_commit=self.commit_index,
        )

    def handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        if request.term < self.current_term:
            return AppendEntriesResponse(
                term=self.current_term,
                success=False,
                match_index=self.last_log_index,
            )
        if request.term > self.current_term or self.role is not RaftRole.FOLLOWER:
            self._step_down(request.term)
        self.leader_id = request.leader_id
        if self.term_at(request.prev_log_index) != request.prev_log_term:
            return AppendEntriesResponse(
                term=self.current_term,
                success=False,
                match_index=min(self.last_log_index, request.prev_log_index - 1),
            )
        log = list(self.log[: request.prev_log_index])
        for entry in request.entries:
            existing_term = self.term_at(entry.index)
            if existing_term != 0 and existing_term != entry.term:
                log = log[: entry.index - 1]
            if entry.index > len(log):
                log.append(entry)
        self._persist(
            RaftPersistentState(
                current_term=self.current_term,
                voted_for=self.voted_for,
                log=tuple(log),
            )
        )
        if request.leader_commit > self.commit_index:
            self.commit_index = min(request.leader_commit, self.last_log_index)
        return AppendEntriesResponse(
            term=self.current_term,
            success=True,
            match_index=self.last_log_index,
        )

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        if index < 0 or index > len(self.log):
            return -1
        return self.log[index - 1].term

    def mark_committed(self, index: int) -> None:
        self.commit_index = max(self.commit_index, min(index, self.last_log_index))

    def apply_committed(self) -> tuple[RaftLogEntry, ...]:
        entries = self.log[self.last_applied : self.commit_index]
        self.last_applied = self.commit_index
        return entries

    def _step_down(self, term: int) -> None:
        self.role = RaftRole.FOLLOWER
        self.leader_id = None
        self._persist(RaftPersistentState(current_term=term, voted_for=None, log=self.log))

    def _persist(self, state: RaftPersistentState) -> None:
        self.storage.save(state)
        self._state = state


@dataclass(slots=True)
class RaftCluster:
    nodes: dict[str, RaftNode]
    leader_id: str | None = None

    @classmethod
    def create(
        cls,
        member_ids: tuple[str, ...],
        *,
        storages: dict[str, RaftStorage] | None = None,
    ) -> RaftCluster:
        if len(member_ids) < 3:
            raise ValueError("Raft requires at least three members for HA")
        configured_storages = storages or {}
        unknown_storage = set(configured_storages) - set(member_ids)
        if unknown_storage:
            raise ValueError(f"storage configured for unknown members: {sorted(unknown_storage)}")
        nodes = {
            member_id: RaftNode(
                node_id=member_id,
                peer_ids=tuple(peer for peer in member_ids if peer != member_id),
                storage=configured_storages.get(member_id, MemoryRaftStorage()),
            )
            for member_id in member_ids
        }
        return cls(nodes=nodes)

    @property
    def quorum_size(self) -> int:
        return len(self.nodes) // 2 + 1

    def elect(self, candidate_id: str, *, available_members: set[str] | None = None) -> RaftNode:
        available = available_members or set(self.nodes)
        if candidate_id not in available:
            raise RuntimeError("candidate is unavailable")
        candidate = self.nodes[candidate_id]
        request = candidate.start_election()
        votes = 1
        for member_id in sorted(available - {candidate_id}):
            response = self.nodes[member_id].handle_request_vote(request)
            if response.term > candidate.current_term:
                candidate._step_down(response.term)
                raise RuntimeError("candidate observed newer term")
            if response.vote_granted:
                votes += 1
        if votes < self.quorum_size:
            raise RuntimeError("candidate did not receive quorum")
        for member_id, node in self.nodes.items():
            if member_id != candidate_id and member_id in available:
                node.role = RaftRole.FOLLOWER
        candidate.become_leader()
        self.leader_id = candidate_id
        return candidate

    def append_command_from(
        self,
        member_id: str,
        command_id: str,
        command: str,
        *,
        available_members: set[str] | None = None,
    ) -> RaftLogEntry:
        if self.leader_id is None:
            raise RuntimeError("no leader elected")
        if member_id != self.leader_id:
            raise RuntimeError(f"member {member_id!r} is not the elected leader")
        available = available_members or set(self.nodes)
        if self.leader_id not in available:
            raise RuntimeError("leader is unavailable")
        leader = self.nodes[self.leader_id]
        entry = leader.append_local_command(command_id, command)
        acknowledgements = 1
        for member_id in sorted(available - {self.leader_id}):
            if self._replicate_to(leader, self.nodes[member_id]):
                acknowledgements += 1
        if acknowledgements < self.quorum_size:
            raise RuntimeError("entry was not committed by quorum")
        leader.mark_committed(entry.index)
        for member_id in sorted(available - {self.leader_id}):
            self._replicate_to(leader, self.nodes[member_id])
        return entry

    def append_command(
        self,
        command_id: str,
        command: str,
        *,
        available_members: set[str] | None = None,
    ) -> RaftLogEntry:
        if self.leader_id is None:
            raise RuntimeError("no leader elected")
        return self.append_command_from(
            self.leader_id,
            command_id,
            command,
            available_members=available_members,
        )

    def committed_entries(self, node_id: str) -> tuple[RaftLogEntry, ...]:
        return self.nodes[node_id].apply_committed()

    def recover_commit_index(self) -> int:
        """Recover the highest durable, quorum-replicated index after process restart."""

        max_index = max((node.last_log_index for node in self.nodes.values()), default=0)
        recovered = 0
        for index in range(1, max_index + 1):
            entry_counts: dict[RaftLogEntry, int] = {}
            for node in self.nodes.values():
                if node.last_log_index < index:
                    continue
                entry = node.log[index - 1]
                entry_counts[entry] = entry_counts.get(entry, 0) + 1
            if not any(count >= self.quorum_size for count in entry_counts.values()):
                break
            recovered = index
        for node in self.nodes.values():
            node.mark_committed(recovered)
        return recovered

    def _replicate_to(self, leader: RaftNode, follower: RaftNode) -> bool:
        next_index = follower.last_log_index + 1
        while next_index >= 1:
            entries = tuple(entry for entry in leader.log if entry.index >= next_index)
            request = leader.append_entries_request(follower, entries=entries)
            response = follower.handle_append_entries(request)
            if response.term > leader.current_term:
                leader._step_down(response.term)
                return False
            if response.success:
                return True
            next_index = max(1, response.match_index + 1)
        return False
