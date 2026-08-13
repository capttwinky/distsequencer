from __future__ import annotations

from distributed_sequencer.domain.music import MusicalEvent, Phrase
from distributed_sequencer.domain.state import Assignment, PartLease, PhraseReady, VariationPolicy


def assignment_to_payload(assignment: Assignment) -> dict[str, object]:
    assert assignment.lease is not None
    return {
        "node_id": assignment.node_id,
        "assignment_generation": assignment.assignment_generation,
        "transport_epoch": assignment.transport_epoch,
        "part_id": assignment.part_id,
        "assignment_id": assignment.assignment_id,
        "message_id": assignment.message_id,
        "phrase": phrase_to_payload(assignment.phrase),
        "policy": variation_policy_to_payload(assignment.policy),
        "lease": part_lease_to_payload(assignment.lease),
    }


def assignment_from_payload(payload: dict[str, object]) -> Assignment:
    policy = _dict(payload["policy"], "policy")
    return Assignment(
        node_id=str(payload["node_id"]),
        phrase=phrase_from_payload(_dict(payload["phrase"], "phrase")),
        policy=VariationPolicy(
            policy_version=_int(policy["policy_version"], "policy_version"),
            timing_jitter_ticks=_int(policy["timing_jitter_ticks"], "timing_jitter_ticks"),
            velocity_jitter=_int(policy["velocity_jitter"], "velocity_jitter"),
            omission_probability=_float(policy["omission_probability"], "omission_probability"),
            pitch_shift_semitones=_int(policy["pitch_shift_semitones"], "pitch_shift_semitones"),
            rhythmic_freedom=_float(policy["rhythmic_freedom"], "rhythmic_freedom"),
            pitch_freedom=_float(policy["pitch_freedom"], "pitch_freedom"),
            density_variance=_float(policy["density_variance"], "density_variance"),
            fill_probability=_float(policy["fill_probability"], "fill_probability"),
        ),
        assignment_generation=_int(
            payload["assignment_generation"],
            "assignment_generation",
        ),
        transport_epoch=_int(payload["transport_epoch"], "transport_epoch"),
        part_id=str(payload["part_id"]),
        assignment_id=str(payload["assignment_id"]),
        lease=part_lease_from_payload(_dict(payload["lease"], "lease")),
        message_id=str(payload["message_id"]),
    )


def phrase_ready_to_payload(ready: PhraseReady) -> dict[str, object]:
    return {
        "node_id": ready.node_id,
        "part_id": ready.part_id,
        "phrase_sequence": ready.phrase_sequence,
        "assignment_generation": ready.assignment_generation,
        "ready_through_bar": ready.ready_through_bar,
        "transport_epoch": ready.transport_epoch,
    }


def phrase_ready_from_payload(payload: dict[str, object]) -> PhraseReady:
    return PhraseReady(
        node_id=str(payload["node_id"]),
        part_id=str(payload["part_id"]),
        phrase_sequence=_int(payload["phrase_sequence"], "phrase_sequence"),
        assignment_generation=_int(payload["assignment_generation"], "assignment_generation"),
        ready_through_bar=_int(payload["ready_through_bar"], "ready_through_bar"),
        transport_epoch=_int(payload["transport_epoch"], "transport_epoch"),
    )


def phrase_to_payload(phrase: Phrase) -> dict[str, object]:
    return {
        "phrase_id": phrase.phrase_id,
        "role": phrase.role,
        "events": [
            {
                "onset_tick": event.onset_tick,
                "pitch": event.pitch,
                "duration_ticks": event.duration_ticks,
                "velocity": event.velocity,
                "channel": event.channel,
            }
            for event in phrase.events
        ],
        "phrase_revision": phrase.phrase_revision,
        "phrase_sequence": phrase.phrase_sequence,
        "bars": phrase.bars,
        "beats_per_bar": phrase.beats_per_bar,
        "ticks_per_beat": phrase.ticks_per_beat,
    }


def phrase_from_payload(payload: dict[str, object]) -> Phrase:
    events = payload["events"]
    if not isinstance(events, list):
        raise ValueError("phrase.events must be a list")
    return Phrase(
        phrase_id=str(payload["phrase_id"]),
        role=str(payload["role"]),
        events=tuple(
            MusicalEvent(
                onset_tick=_int(_dict(event, "event")["onset_tick"], "onset_tick"),
                pitch=_int(_dict(event, "event")["pitch"], "pitch"),
                duration_ticks=_int(_dict(event, "event")["duration_ticks"], "duration_ticks"),
                velocity=_int(_dict(event, "event")["velocity"], "velocity"),
                channel=_int(_dict(event, "event")["channel"], "channel"),
            )
            for event in events
        ),
        phrase_revision=_int(payload["phrase_revision"], "phrase_revision"),
        phrase_sequence=_int(payload["phrase_sequence"], "phrase_sequence"),
        bars=_int(payload["bars"], "bars"),
        beats_per_bar=_int(payload["beats_per_bar"], "beats_per_bar"),
        ticks_per_beat=_int(payload["ticks_per_beat"], "ticks_per_beat"),
    )


def variation_policy_to_payload(policy: VariationPolicy) -> dict[str, object]:
    return {
        "policy_version": policy.policy_version,
        "timing_jitter_ticks": policy.timing_jitter_ticks,
        "velocity_jitter": policy.velocity_jitter,
        "omission_probability": policy.omission_probability,
        "pitch_shift_semitones": policy.pitch_shift_semitones,
        "rhythmic_freedom": policy.rhythmic_freedom,
        "pitch_freedom": policy.pitch_freedom,
        "density_variance": policy.density_variance,
        "fill_probability": policy.fill_probability,
    }


def part_lease_to_payload(lease: PartLease) -> dict[str, object]:
    return {
        "transport_epoch": lease.transport_epoch,
        "part_id": lease.part_id,
        "node_id": lease.node_id,
        "assignment_generation": lease.assignment_generation,
        "valid_from_bar": lease.valid_from_bar,
        "valid_through_bar": lease.valid_through_bar,
        "exclusive": lease.exclusive,
    }


def part_lease_from_payload(payload: dict[str, object]) -> PartLease:
    return PartLease(
        transport_epoch=_int(payload["transport_epoch"], "transport_epoch"),
        part_id=str(payload["part_id"]),
        node_id=str(payload["node_id"]),
        assignment_generation=_int(payload["assignment_generation"], "assignment_generation"),
        valid_from_bar=_int(payload["valid_from_bar"], "valid_from_bar"),
        valid_through_bar=_int(payload["valid_through_bar"], "valid_through_bar"),
        exclusive=_bool(payload["exclusive"], "exclusive"),
    )


def _dict(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def _int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _float(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{name} must be numeric")
    return float(value)


def _bool(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value
