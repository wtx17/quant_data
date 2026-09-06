"""Initialize a DataClient with the project-supported datasets."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

if __package__:
    from . import (
        ClickHouseConfig,
        DataClient,
        DatasetRegistrationError,
    )
else:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from quant_data import (
        ClickHouseConfig,
        DataClient,
        DatasetRegistrationError,
    )

DEFAULT_CLICKHOUSE_CONNECTION = "minghu"

# One default ClickHouse panel configuration per dataset, consumed directly by
# ``register_clickhouse`` and the catalog generator. Keys mirror the keyword
# arguments of :meth:`quant_data.client.DataClient.register_clickhouse`.
CLICKHOUSE_PANEL_DEFS: tuple[dict[str, Any], ...] = (
    dict(
        name="minghu_daily",
        table="stock_base.daily",
        time_column="date",
        partition_column=None,
        order_columns=(),
        frequency="1d",
    ),
    dict(
        name="minghu_index_daily",
        table="index_base.daily",
        time_column="date",
        partition_column=None,
        order_columns=(),
        frequency="1d",
    ),
    dict(
        name="minghu_m1",
        table="stock_base.m1",
        time_column="date_time",
        partition_column="date",
        order_columns=("date_time", "code"),
        frequency="1min",
    ),
    dict(
        name="zb_cj_flow_min",
        table="zhangruiqi.zb_cj_flow_min",
        time_column="date_time",
        partition_column="date",
        order_columns=("date_time", "code"),
        frequency="1min",
    ),
)

if __package__:
    from .backends.tushare_catalog import TUSHARE_DATASETS
else:
    from quant_data.backends.tushare_catalog import TUSHARE_DATASETS

TUSHARE_DATASET_NAMES = tuple(TUSHARE_DATASETS)


def _register_clickhouse_defaults(client: DataClient, connection: str) -> None:
    """Register the default ClickHouse panels on one shared connection."""

    for definition in CLICKHOUSE_PANEL_DEFS:
        client.register_clickhouse(
            definition["name"],
            connection=connection,
            table=definition["table"],
            time_column=definition["time_column"],
            partition_column=definition["partition_column"],
            order_columns=definition["order_columns"],
            frequency=definition["frequency"],
        )


def registered_dataset_names() -> tuple[str, ...]:
    """Return dataset names registered by
    :func:`quant_data.initialize.initialize_data_client`.

    Parameters
    ----------
    Returns
    -------
    tuple[str, ...]
        ClickHouse names followed by Tushare names in registration order.

    Notes
    -----
    The result is derived from local registration records and requires no
    credentials or remote service access.
    """
    names = [str(definition["name"]) for definition in CLICKHOUSE_PANEL_DEFS]
    names.append("membership_events")
    names.extend(TUSHARE_DATASET_NAMES)
    return tuple(names)


def initialize_data_client(
    *,
    audit_dir: str | Path | None = None,
    register_clickhouse: bool = True,
    register_tushare: bool = True,
    clickhouse_connection: str = DEFAULT_CLICKHOUSE_CONNECTION,
    clickhouse_host: str | None = None,
    clickhouse_port: int | None = None,
    clickhouse_username: str | None = None,
    clickhouse_password: str | None = None,
    clickhouse_password_env: str | None = None,
    clickhouse_secure: bool | None = None,
    tushare_data_dir: str | Path | None = None,
) -> DataClient:
    """Register default ClickHouse tables and local Tushare archives.

    Tushare registration requires ``tushare_data_dir`` or
    ``QUANT_DATA_TUSHARE_DATA_DIR``. There is no remote fallback. The named
    ClickHouse connection also supplies trading dates to PIT/industry panels,
    even when ``register_clickhouse=False`` skips the market dataset names.
    Connections are opened lazily; manifest validation is local.
    """
    data_dir = tushare_data_dir or _first_env("QUANT_DATA_TUSHARE_DATA_DIR")
    if register_tushare and data_dir is None:
        raise DatasetRegistrationError(
            "Local Tushare registration requires tushare_data_dir or "
            "QUANT_DATA_TUSHARE_DATA_DIR; use register_tushare=False to omit it"
        )
    client = DataClient(
        audit_dir=audit_dir
        if audit_dir is not None
        else _first_env("QUANT_DATA_AUDIT_DIR") or ".quant_data/audit"
    )
    if register_clickhouse or register_tushare:
        client.add_clickhouse_connection(
            clickhouse_connection,
            ClickHouseConfig(
                host=clickhouse_host
                or _first_env("QUANT_DATA_CLICKHOUSE_HOST", "MINGHU_CLICKHOUSE_HOST")
                or "chdb.tradegdb.com",
                port=clickhouse_port
                if clickhouse_port is not None
                else _env_int(("QUANT_DATA_CLICKHOUSE_PORT", "MINGHU_CLICKHOUSE_PORT"), 8123),
                username=clickhouse_username
                or _first_env("QUANT_DATA_CLICKHOUSE_USERNAME", "MINGHU_CLICKHOUSE_USERNAME"),
                password=clickhouse_password or _first_env("QUANT_DATA_CLICKHOUSE_PASSWORD"),
                password_env=clickhouse_password_env
                or _first_env("QUANT_DATA_CLICKHOUSE_PASSWORD_ENV")
                or "MINGHU_CLICKHOUSE_PASSWORD",
                secure=clickhouse_secure
                if clickhouse_secure is not None
                else _env_bool(("QUANT_DATA_CLICKHOUSE_SECURE", "MINGHU_CLICKHOUSE_SECURE"), False),
            ),
        )
    if register_clickhouse:
        _register_clickhouse_defaults(client, clickhouse_connection)
        client.register_builtin("membership_events", connection=clickhouse_connection)
    if register_tushare:
        assert data_dir is not None
        for name in TUSHARE_DATASET_NAMES:
            client.register_tushare(
                name, data_dir=data_dir, calendar_connection=clickhouse_connection
            )
    return client


def initialize(**kwargs: Any) -> DataClient:
    """Call :func:`quant_data.initialize.initialize_data_client` with
    the same keyword arguments.

    Parameters
    ----------
    **kwargs
        Keyword arguments accepted by
        :func:`quant_data.initialize.initialize_data_client`.

    Returns
    -------
    DataClient
        Configured data client.
    """
    return initialize_data_client(**kwargs)


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def _env_int(names: tuple[str, ...], default: int) -> int:
    value = _first_env(*names)
    return int(value) if value is not None else default


def _env_bool(names: tuple[str, ...], default: bool) -> bool:
    value = _first_env(*names)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def main() -> None:
    """Initialize the default client and print registered dataset names."""

    client = initialize_data_client()
    try:
        names = registered_dataset_names()
        print(f"Registered {len(names)} datasets:")
        for name in names:
            print(f"- {name}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
