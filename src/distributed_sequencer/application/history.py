from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path

from distributed_sequencer.domain.music import MusicalEvent, Phrase
from distributed_sequencer.domain.state import CompositionState


@dataclass(frozen=True, slots=True)
class CompositionHistoryRecord:
    composition_id: str
    part_id: str
    phrase_sequence: int
    phrase_revision: int
    phrase_id: str
    state_revision: int


class CompositionHistoryStore:
    """SQLite-backed persistent composition history."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS phrases (
                    composition_id TEXT NOT NULL,
                    part_id TEXT NOT NULL,
                    phrase_sequence INTEGER NOT NULL,
                    phrase_revision INTEGER NOT NULL,
                    phrase_id TEXT NOT NULL,
                    state_revision INTEGER NOT NULL,
                    state_json TEXT NOT NULL,
                    phrase_json TEXT NOT NULL,
                    PRIMARY KEY (composition_id, part_id, phrase_sequence, phrase_revision)
                )
                """
            )

    def record_phrase(self, state: CompositionState, phrase: Phrase) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO phrases (
                    composition_id, part_id, phrase_sequence, phrase_revision,
                    phrase_id, state_revision, state_json, phrase_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    state.composition_id,
                    phrase.role,
                    phrase.phrase_sequence,
                    phrase.phrase_revision,
                    phrase.phrase_id,
                    state.revision,
                    json.dumps(asdict(state), sort_keys=True),
                    _phrase_to_json(phrase),
                ),
            )

    def list_records(self, composition_id: str) -> tuple[CompositionHistoryRecord, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT composition_id, part_id, phrase_sequence, phrase_revision,
                       phrase_id, state_revision
                FROM phrases
                WHERE composition_id = ?
                ORDER BY part_id, phrase_sequence, phrase_revision
                """,
                (composition_id,),
            ).fetchall()
        return tuple(CompositionHistoryRecord(*row) for row in rows)

    def load_phrase(
        self,
        *,
        composition_id: str,
        part_id: str,
        phrase_sequence: int,
        phrase_revision: int,
    ) -> Phrase | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT phrase_json
                FROM phrases
                WHERE composition_id = ?
                  AND part_id = ?
                  AND phrase_sequence = ?
                  AND phrase_revision = ?
                """,
                (composition_id, part_id, phrase_sequence, phrase_revision),
            ).fetchone()
        if row is None:
            return None
        return _phrase_from_json(str(row[0]))

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)


def _phrase_to_json(phrase: Phrase) -> str:
    return json.dumps(asdict(phrase), sort_keys=True)


def _phrase_from_json(raw: str) -> Phrase:
    decoded = json.loads(raw)
    events = tuple(MusicalEvent(**event) for event in decoded["events"])
    return Phrase(
        phrase_id=str(decoded["phrase_id"]),
        role=str(decoded["role"]),
        events=events,
        phrase_revision=int(decoded["phrase_revision"]),
        phrase_sequence=int(decoded["phrase_sequence"]),
        bars=int(decoded["bars"]),
        beats_per_bar=int(decoded["beats_per_bar"]),
        ticks_per_beat=int(decoded["ticks_per_beat"]),
    )
