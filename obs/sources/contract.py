"""Source contracts: the shape a source promises, and what to do when it moves.

A contract is the agreement between an upstream system and the warehouse. It
names the fields the downstream models depend on, their types, and which of
them may never be null. Checking a payload against the contract before it
lands turns a silent column rename into a loud failure at ingestion time,
which is the only place it is cheap to fix.

Drift is sorted into three kinds, because they do not deserve the same
response:

  added      a field nobody has claimed yet. Safe. Recorded so the owner can
             decide whether to model it.
  missing    a declared field is gone. Breaks downstream models; fatal when
             the field is required.
  retyped    a declared field changed type. The quiet one that corrupts marts
             without failing a load, so it is always fatal.

Nothing here is Jira-specific: onboarding the next enterprise application
means declaring its contract, not writing another drift checker.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

JSON_TYPES: dict[str, tuple[type, ...]] = {
    "string": (str,),
    "int": (int,),
    "float": (int, float),
    "bool": (bool,),
    "timestamp": (str,),          # ISO-8601 in transit, cast on load
    "array": (list, tuple),
    "object": (dict,),
}


@dataclass(frozen=True)
class Field:
    name: str
    type: str
    required: bool = False

    def accepts(self, value: Any) -> bool:
        if value is None:
            return not self.required
        expected = JSON_TYPES.get(self.type)
        if expected is None:
            raise ValueError(f"{self.name}: unknown contract type {self.type!r}")
        if self.type != "bool" and isinstance(value, bool):
            return False          # bool is an int subclass; don't let it pass as one
        return isinstance(value, expected)


@dataclass
class DriftReport:
    source: str
    added: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    retyped: list[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not (self.added or self.missing or self.retyped)

    def fatal(self, contract: "SourceContract") -> list[str]:
        """Drift that must stop the load rather than be recorded and passed on."""
        required = {f.name for f in contract.fields if f.required}
        return sorted([m for m in self.missing if m in required] + self.retyped)

    def summary(self) -> str:
        if self.clean:
            return f"{self.source}: payload matches contract"
        parts = []
        for label, names in (("added", self.added), ("missing", self.missing),
                             ("retyped", self.retyped)):
            if names:
                parts.append(f"{label}={','.join(sorted(names))}")
        return f"{self.source}: " + " ".join(parts)


class ContractViolation(RuntimeError):
    """Raised when drift would corrupt downstream models if it landed."""


@dataclass(frozen=True)
class SourceContract:
    """The declared shape of one source, plus the keys that make a load safe."""

    source: str
    fields: tuple[Field, ...]
    primary_key: str
    updated_at: str
    passthrough_prefixes: tuple[str, ...] = ()

    @property
    def declared(self) -> dict[str, Field]:
        return {f.name: f for f in self.fields}

    def _claimed(self, name: str) -> bool:
        return name in self.declared or name.startswith(self.passthrough_prefixes)

    def inspect(self, records: Iterable[Mapping[str, Any]]) -> DriftReport:
        """Compare a batch against the contract without mutating it."""
        records = list(records)
        report = DriftReport(self.source)
        seen: set[str] = set()
        retyped: set[str] = set()

        for row in records:
            for name, value in row.items():
                seen.add(name)
                declared = self.declared.get(name)
                if declared is None:
                    if not self._claimed(name):
                        report.added.append(name)
                elif not declared.accepts(value):
                    retyped.add(name)

        report.added = sorted(set(report.added))
        report.retyped = sorted(retyped)
        if records:
            report.missing = sorted(set(self.declared) - seen)
        return report

    def enforce(self, records: Iterable[Mapping[str, Any]]) -> DriftReport:
        """Inspect, then refuse the load if the drift is the corrupting kind."""
        records = list(records)
        report = self.inspect(records)
        fatal = report.fatal(self)
        if fatal:
            raise ContractViolation(
                f"{self.source}: refusing to land {len(records)} rows — "
                f"contract broken on {', '.join(fatal)}. "
                f"Update the contract deliberately, then backfill."
            )
        return report
