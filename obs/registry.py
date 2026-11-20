"""Load and validate the per-source registry."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

CADENCE_MINUTES = {"5m": 5, "15m": 15, "30m": 30, "1h": 60, "6h": 360, "12h": 720, "1d": 1440}


@dataclass(frozen=True)
class Source:
    name: str
    domain: str
    owner: str
    system: str
    cadence: str
    freshness_target_minutes: int
    severity: str
    downstream: tuple[str, ...] = ()
    restating: bool = False
    partial_load_ratio: float = 0.60
    anomaly_mad_threshold: float = 4.0
    grace_minutes: int = 15

    @property
    def cadence_minutes(self) -> int:
        return CADENCE_MINUTES[self.cadence]

    @property
    def deadline_minutes(self) -> int:
        """Freshness target plus grace. A load older than this is late."""
        return self.freshness_target_minutes + self.grace_minutes


@dataclass
class Registry:
    sources: list[Source] = field(default_factory=list)

    def __iter__(self):
        return iter(self.sources)

    def __len__(self):
        return len(self.sources)

    def get(self, name: str) -> Source | None:
        return next((s for s in self.sources if s.name == name), None)

    @property
    def names(self) -> set[str]:
        return {s.name for s in self.sources}


VALID_SEVERITY = {"critical", "high", "medium", "low"}


def load(path: str | Path = "registry/sources.yml") -> Registry:
    raw = yaml.safe_load(Path(path).read_text())
    defaults = raw.get("defaults", {}) or {}
    sources: list[Source] = []
    for entry in raw.get("sources", []) or []:
        merged = {**defaults, **entry}
        merged["downstream"] = tuple(merged.get("downstream", ()) or ())
        sources.append(Source(**merged))
    validate(sources)
    return Registry(sources)


def validate(sources: list[Source]) -> None:
    """Fail loudly on a registry that would let a source go unwatched."""
    seen: set[str] = set()
    for s in sources:
        if s.name in seen:
            raise ValueError(f"duplicate source in registry: {s.name}")
        seen.add(s.name)
        if s.cadence not in CADENCE_MINUTES:
            raise ValueError(f"{s.name}: unknown cadence {s.cadence!r}")
        if s.severity not in VALID_SEVERITY:
            raise ValueError(f"{s.name}: unknown severity {s.severity!r}")
        if not s.owner:
            raise ValueError(f"{s.name}: every source needs an owner")
        if s.freshness_target_minutes < s.cadence_minutes:
            raise ValueError(
                f"{s.name}: freshness target ({s.freshness_target_minutes}m) is tighter "
                f"than its cadence ({s.cadence_minutes}m) and can never pass"
            )


def audit(registry: Registry, observed_sources: set[str]) -> list[str]:
    """Sources landing in the warehouse that nobody has claimed."""
    return sorted(observed_sources - registry.names)
