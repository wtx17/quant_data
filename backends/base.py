"""Storage backend protocol."""

from datetime import date
from typing import Protocol

import pyarrow as pa

from ..models import DataQuery, DatasetDefinition, RegisteredDataset


class DataBackend(Protocol):
    """Protocol implemented by storage backends.

    Notes
    -----
    A backend validates definitions in :meth:`prepare`, executes normalized
    scans in :meth:`scan`, provides sanitized provenance in
    :meth:`fingerprint`, and releases resources in :meth:`close`.
    """

    def prepare(self, spec: DatasetDefinition) -> RegisteredDataset:
        """Validate a definition and create backend-owned prepared state.

        Parameters
        ----------
        spec
            Dataset definition assigned to this backend.

        Returns
        -------
        RegisteredDataset
            Normalized specification, Arrow schema, method-level contract,
            source descriptor, and optional adjustment policy.
        """

        ...

    def scan(self, dataset: RegisteredDataset, query: DataQuery) -> pa.Table:
        """Execute a normalized query and return an Arrow long table.

        Parameters
        ----------
        dataset
            Prepared dataset returned by :meth:`prepare`.
        query
            Validated field, time, and instrument request.

        Returns
        -------
        pyarrow.Table
            Projected long table containing both key columns.
        """

        ...

    def fingerprint(self, dataset: RegisteredDataset) -> dict[str, object]:
        """Return sanitized source provenance for a query audit.

        Parameters
        ----------
        dataset
            Prepared dataset returned by :meth:`prepare`.

        Returns
        -------
        dict[str, object]
            JSON-serializable source metadata without credentials.
        """

        ...

    def close(self) -> None:
        """Release cached clients or other persistent resources."""

        ...


class TushareSemanticBackend(Protocol):
    """Additional operations implemented by Tushare-shaped data backends."""

    def panel_kind(self, dataset: RegisteredDataset) -> str:
        """Return the logical panel kind for a prepared dataset."""

        ...

    def scan_disclosure_events(self, dataset: RegisteredDataset, query: DataQuery) -> pa.Table:
        """Return disclosure events needed to construct a PIT panel."""

        ...

    def trade_calendar(self, dataset: RegisteredDataset, query: DataQuery) -> list[date]:
        """Return trading sessions required by a panel query."""

        ...

    def pit_panel_semantics(self, dataset: RegisteredDataset) -> tuple[str, str, tuple[str, ...]]:
        """Return disclosure, period, and revision-order columns."""

        ...

    def scan_membership_panel(self, dataset: RegisteredDataset, query: DataQuery) -> pa.Table:
        """Expand effective-dated membership rows over a trading calendar."""

        ...

    def normalize_snapshot_query(
        self,
        dataset: RegisteredDataset,
        query: DataQuery,
    ) -> DataQuery:
        """Apply and validate snapshot boundaries for a query."""

        ...
