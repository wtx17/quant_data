"""Logical Tushare dataset catalogs and remote route descriptions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Mapping, TypeAlias

import pyarrow as pa

from ..exceptions import DatasetRegistrationError
from .tushare_schemas import TUSHARE_SCHEMAS

RouteScope: TypeAlias = Literal["instruments", "whole_market", "both"]
RequestKind: TypeAlias = Literal["date_range", "trade_date", "membership_status"]


@dataclass(frozen=True, slots=True)
class TushareApiRoute:
    """Describe one remote API route for a logical Tushare dataset.

    Parameters
    ----------
    api_name
        Remote API method name.
    scope
        Which query universes the route serves: ``"instruments"`` for
        explicit instrument lists, ``"whole_market"`` for unbounded queries,
        ``"both"`` for either.
    request
        Request shape implemented by one of the three explicit fetch
        functions: a closed remote date range, one request per open trading
        date, or paired current/history membership requests.
    instrument_param
        API parameter carrying one instrument identifier.
    start_param, end_param
        API parameters bounding a ``date_range`` request.
    date_param
        API parameter selecting one trading date.
    max_rows
        Row count at which a single response is considered possibly
        truncated, or ``None`` when the API pages without loss.
    status_param, status_values
        Membership-status parameter and its fetchable values.
    """

    api_name: str
    scope: RouteScope
    request: RequestKind
    instrument_param: str = "ts_code"
    start_param: str = "start_date"
    end_param: str = "end_date"
    date_param: str = "trade_date"
    max_rows: int | None = None
    status_param: str = "is_new"
    status_values: tuple[str, ...] = ("Y", "N")


@dataclass(frozen=True, slots=True)
class DisclosureSemantics:
    """Describe report-period rows disclosed and revised over time."""

    period_column: str
    disclosure_column: str
    identity_columns: tuple[str, ...]
    revision_order: tuple[str, ...]
    source_order: tuple[str, ...]
    panel_time_column: str = "trade_date"
    panel_frequency: str = "d"


@dataclass(frozen=True, slots=True)
class MembershipSemantics:
    """Describe effective-dated membership intervals."""

    interval_start_column: str
    interval_end_column: str
    identity_columns: tuple[str, ...]
    source_order: tuple[str, ...]
    source_time_column: str = "in_date"
    panel_time_column: str = "date"
    panel_frequency: str = "d"


@dataclass(frozen=True, slots=True)
class ObservationSemantics:
    """Describe unique time/instrument observations that can be pivoted."""

    source_time_column: str
    identity_columns: tuple[str, ...]
    source_order: tuple[str, ...]
    panel_time_column: str = "trade_date"
    panel_frequency: str = "d"


TushareSemantics: TypeAlias = DisclosureSemantics | MembershipSemantics | ObservationSemantics


@dataclass(frozen=True, slots=True)
class TushareDatasetCatalog:
    """Describe one logical dataset, its schema, semantics, and API routes."""

    name: str
    schema: pa.Schema
    semantics: TushareSemantics
    routes: tuple[TushareApiRoute, ...]
    instrument_column: str = "ts_code"


def build_tushare_catalogs(
    schemas: Mapping[str, pa.Schema],
) -> dict[str, TushareDatasetCatalog]:
    """Build the immutable logical catalog from version-controlled Arrow schemas."""

    def financial(
        name: str,
        *,
        disclosure_column: str,
        identity_columns: tuple[str, ...],
        revision_order: tuple[str, ...],
    ) -> TushareDatasetCatalog:
        semantics = DisclosureSemantics(
            period_column="end_date",
            disclosure_column=disclosure_column,
            identity_columns=identity_columns,
            revision_order=revision_order,
            source_order=_unique(
                (
                    "end_date",
                    "ts_code",
                    disclosure_column,
                    *identity_columns,
                )
            ),
        )
        return TushareDatasetCatalog(
            name=name,
            schema=schemas[name],
            semantics=semantics,
            routes=(
                TushareApiRoute(
                    api_name=name,
                    scope="instruments",
                    request="date_range",
                ),
                TushareApiRoute(
                    api_name=f"{name}_vip",
                    scope="whole_market",
                    request="date_range",
                ),
            ),
        )

    observation_semantics = ObservationSemantics(
        source_time_column="trade_date",
        identity_columns=(),
        source_order=("trade_date", "ts_code"),
    )
    catalogs = {
        "daily_basic": TushareDatasetCatalog(
            name="daily_basic",
            schema=schemas["daily_basic"],
            semantics=observation_semantics,
            routes=(
                TushareApiRoute(
                    api_name="daily_basic",
                    scope="both",
                    request="trade_date",
                    max_rows=6000,
                ),
            ),
        ),
        "income": financial(
            "income",
            disclosure_column="f_ann_date",
            identity_columns=(
                "ann_date",
                "f_ann_date",
                "report_type",
                "comp_type",
                "end_type",
                "update_flag",
            ),
            revision_order=("f_ann_date", "ann_date", "update_flag"),
        ),
        "balancesheet": financial(
            "balancesheet",
            disclosure_column="f_ann_date",
            identity_columns=(
                "ann_date",
                "f_ann_date",
                "report_type",
                "comp_type",
                "end_type",
                "update_flag",
            ),
            revision_order=("f_ann_date", "ann_date", "update_flag"),
        ),
        "cashflow": financial(
            "cashflow",
            disclosure_column="f_ann_date",
            identity_columns=(
                "ann_date",
                "f_ann_date",
                "report_type",
                "comp_type",
                "end_type",
                "update_flag",
            ),
            revision_order=("f_ann_date", "ann_date", "update_flag"),
        ),
        "fina_indicator": financial(
            "fina_indicator",
            disclosure_column="ann_date",
            identity_columns=("ann_date", "update_flag"),
            revision_order=("ann_date", "update_flag"),
        ),
        "express": financial(
            "express",
            disclosure_column="ann_date",
            identity_columns=("ann_date", "is_audit"),
            revision_order=("ann_date",),
        ),
        "forecast": financial(
            "forecast",
            disclosure_column="ann_date",
            identity_columns=("ann_date", "first_ann_date", "type"),
            revision_order=("ann_date", "first_ann_date"),
        ),
    }

    holder_semantics = DisclosureSemantics(
        period_column="end_date",
        disclosure_column="ann_date",
        identity_columns=("ann_date",),
        revision_order=("ann_date",),
        source_order=("end_date", "ts_code", "ann_date"),
    )
    catalogs["stk_holdernumber"] = TushareDatasetCatalog(
        name="stk_holdernumber",
        schema=schemas["stk_holdernumber"],
        semantics=holder_semantics,
        routes=(
            TushareApiRoute(
                api_name="stk_holdernumber",
                scope="both",
                request="date_range",
            ),
        ),
    )

    membership_schema = schemas["industry_member"]
    membership_semantics = MembershipSemantics(
        interval_start_column="in_date",
        interval_end_column="out_date",
        identity_columns=("l1_code", "l2_code", "l3_code", "out_date", "is_new"),
        source_order=("in_date", "ts_code", "out_date", "l3_code", "is_new"),
    )
    for name in ("ci_index_member", "index_member_all"):
        catalogs[name] = TushareDatasetCatalog(
            name=name,
            schema=membership_schema,
            semantics=membership_semantics,
            routes=(
                TushareApiRoute(
                    api_name=name,
                    scope="both",
                    request="membership_status",
                ),
            ),
        )

    return catalogs


def _unique(values: tuple[str, ...]) -> tuple[str, ...]:
    result: list[str] = []
    for value in values:
        if value not in result:
            result.append(value)
    return tuple(result)


TUSHARE_DATASETS = build_tushare_catalogs(TUSHARE_SCHEMAS)


def catalog_for(dataset_name: str) -> TushareDatasetCatalog:
    """Return the logical catalog for one supported dataset name.

    Raises
    ------
    DatasetRegistrationError
        If the name is not part of the static catalog.
    """

    catalog = TUSHARE_DATASETS.get(dataset_name)
    if catalog is None:
        supported = ", ".join(sorted(TUSHARE_DATASETS))
        raise DatasetRegistrationError(
            f"Unsupported Tushare dataset {dataset_name!r}; supported datasets: {supported}"
        )
    return catalog


__all__ = [
    "DisclosureSemantics",
    "MembershipSemantics",
    "ObservationSemantics",
    "TushareApiRoute",
    "TushareDatasetCatalog",
    "TUSHARE_DATASETS",
    "build_tushare_catalogs",
]
