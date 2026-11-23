"""Ingestion connectors for non-platform (enterprise application) sources.

Platform data arrives through CDC and vendor APIs that the warehouse already
knows how to land. Enterprise applications do not: each one has its own auth,
its own pagination, its own idea of a timestamp, and administrators who add
fields without telling anyone. Everything in this package exists to make those
sources land on the same terms as the rest of the registry — a declared
contract, an incremental watermark, and a load event the freshness checks can
evaluate without caring where the rows came from.
"""
from .contract import SourceContract, DriftReport, Field

__all__ = ["SourceContract", "DriftReport", "Field"]
