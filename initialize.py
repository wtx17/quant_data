"""Initialize a DataClient with the project-supported datasets."""

from __future__ import annotations

import os
import sys
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path
from typing import Any

if __package__:
    from . import (
        ClickHouseConfig,
        DataClient,
        DatasetRegistrationError,
        TushareConfig,
    )
else:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from quant_data import (
        ClickHouseConfig,
        DataClient,
        DatasetRegistrationError,
        TushareConfig,
    )

DEFAULT_CLICKHOUSE_CONNECTION = "minghu"
DEFAULT_TUSHARE_CONNECTION = "tushare"


@dataclass(frozen=True, slots=True)
class ClickHouseRegistration:
    """Describe one default ClickHouse panel registration."""

    name: str
    connection: str
    table: str
    time_column: str
    partition_column: str | None = None
    order_columns: tuple[str, ...] = ()
    frequency: str | None = None


_CLICKHOUSE_PANEL_DEFS = (
    dict(
        name="minghu_daily",
        table="stock_base.daily",
        time_column="date",
        frequency="1d",
    ),
    dict(
        name="minghu_index_daily",
        table="index_base.daily",
        time_column="date",
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

TUSHARE_DATASET_NAMES = (
    "daily_basic",
    "income",
    "balancesheet",
    "cashflow",
    "fina_indicator",
    "express",
    "forecast",
    "stk_holdernumber",
    "ci_index_member",
    "index_member_all",
)


def clickhouse_registrations(
    connection: str = DEFAULT_CLICKHOUSE_CONNECTION,
) -> tuple[ClickHouseRegistration, ...]:
    """Return the project-standard ClickHouse panel registrations.

    Parameters
    ----------
    connection
        Connection profile referenced by every returned registration.

    Returns
    -------
    tuple[ClickHouseRegistration, ...]
        Registrations for the built-in Minghu daily, index daily, and minute
        tables.

    Notes
    -----
    This function only creates immutable records. It does not create a
    ClickHouse client, read credentials, or access a remote table.
    """
    return tuple(
        ClickHouseRegistration(connection=connection, **definition)
        for definition in _CLICKHOUSE_PANEL_DEFS
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
    names = [registration.name for registration in clickhouse_registrations()]
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
    tushare_connection: str = DEFAULT_TUSHARE_CONNECTION,
    tushare_data_dir: str | Path | None = None,
    tushare_remote_datasets: Collection[str] | None = None,
    tushare_token: str | None = None,
    tushare_token_env: str | None = None,
) -> DataClient:
    """Create a client and register the project-supported datasets.

    Parameters
    ----------
    audit_dir
        Audit output directory. When omitted,
        ``QUANT_DATA_AUDIT_DIR`` or ``.quant_data/audit`` is used.
    register_clickhouse
        Configure ClickHouse and register the built-in Minghu datasets.
    register_tushare
        Configure Tushare and register its catalog-backed datasets.
    clickhouse_connection
        ClickHouse profile name referenced by generated registrations.
    clickhouse_host
        Server hostname. Environment variables and the project default are
        consulted when omitted.
    clickhouse_port
        Server port. Environment variables and port 8123 are used when omitted.
    clickhouse_username
        Optional login name, with project environment-variable fallbacks.
    clickhouse_password
        Optional direct password value.
    clickhouse_password_env
        Environment variable from which the password is read on first access.
    clickhouse_secure
        Whether to enable TLS. Environment variables are used when omitted.
    tushare_connection
        Tushare profile name referenced by generated registrations.
    tushare_data_dir
        Optional root of a manifest-backed Tushare Parquet archive. When set,
        every Tushare dataset reads local files unless selected by
        ``tushare_remote_datasets``. When omitted, all Tushare datasets use the
        remote backend.
    tushare_remote_datasets
        Logical dataset names that should use the remote Tushare backend while
        the remaining datasets read from ``tushare_data_dir``. When omitted
        with a configured archive, every Tushare dataset is local.
    tushare_token
        Optional direct Tushare token.
    tushare_token_env
        Environment variable from which the token is read.

    Returns
    -------
    DataClient
        Configured client with the requested default datasets registered.

    Raises
    ------
    DatasetRegistrationError
        If a connection or generated dataset registration is invalid.

    Notes
    -----
    Built-in registrations use local catalogs. Neither ClickHouse nor Tushare
    opens a remote connection or resolves credentials until a query needs it.
    Importing this module and calling the registration helpers are
    side-effect free.
    """
    remote_tushare_datasets = (
        _resolve_tushare_remote_datasets(
            tushare_data_dir,
            tushare_remote_datasets,
        )
        if register_tushare
        else frozenset()
    )
    client = DataClient(
        audit_dir=audit_dir
        if audit_dir is not None
        else _first_env("QUANT_DATA_AUDIT_DIR") or ".quant_data/audit"
    )
    if register_clickhouse:
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
                or _first_env(
                    "QUANT_DATA_CLICKHOUSE_USERNAME",
                    "MINGHU_CLICKHOUSE_USERNAME",
                ),
                password=clickhouse_password or _first_env("QUANT_DATA_CLICKHOUSE_PASSWORD"),
                password_env=clickhouse_password_env
                or _first_env("QUANT_DATA_CLICKHOUSE_PASSWORD_ENV")
                or "MINGHU_CLICKHOUSE_PASSWORD",
                secure=clickhouse_secure
                if clickhouse_secure is not None
                else _env_bool(("QUANT_DATA_CLICKHOUSE_SECURE", "MINGHU_CLICKHOUSE_SECURE"), False),
            ),
        )
        for registration in clickhouse_registrations(clickhouse_connection):
            client.register_clickhouse(
                registration.name,
                connection=registration.connection,
                table=registration.table,
                time_column=registration.time_column,
                partition_column=registration.partition_column,
                order_columns=registration.order_columns,
                frequency=registration.frequency,
            )
        client.register_builtin("membership_events", connection=clickhouse_connection)

    if register_tushare:
        client.add_tushare_connection(
            tushare_connection,
            TushareConfig(
                token=tushare_token or _first_env("QUANT_DATA_TUSHARE_TOKEN"),
                token_env=tushare_token_env
                or _first_env("QUANT_DATA_TUSHARE_TOKEN_ENV")
                or "TUSHARE_TOKEN",
            ),
        )
        for dataset_name in TUSHARE_DATASET_NAMES:
            if dataset_name in remote_tushare_datasets:
                client.register_tushare(
                    dataset_name,
                    connection=tushare_connection,
                )
            else:
                if tushare_data_dir is None:
                    raise DatasetRegistrationError(
                        "Tushare local registration requires tushare_data_dir"
                    )
                client.register_tushare(
                    dataset_name,
                    data_dir=tushare_data_dir,
                    calendar_connection=tushare_connection,
                )

    return client


def _resolve_tushare_remote_datasets(
    data_dir: str | Path | None,
    remote_datasets: Collection[str] | None,
) -> frozenset[str]:
    if remote_datasets is None:
        return frozenset() if data_dir is not None else frozenset(TUSHARE_DATASET_NAMES)
    if isinstance(remote_datasets, str):
        raise DatasetRegistrationError(
            "tushare_remote_datasets must be a collection of dataset names, not a string"
        )

    values = tuple(remote_datasets)
    invalid = [repr(name) for name in values if not isinstance(name, str) or not name.strip()]
    if invalid:
        raise DatasetRegistrationError(
            f"tushare_remote_datasets must contain only non-empty strings: {invalid}"
        )
    selected = frozenset(values)
    unsupported = sorted(selected.difference(TUSHARE_DATASET_NAMES))
    if unsupported:
        raise DatasetRegistrationError(
            f"Unsupported Tushare remote datasets: {unsupported}; "
            f"supported datasets: {list(TUSHARE_DATASET_NAMES)}"
        )
    if data_dir is None and selected != frozenset(TUSHARE_DATASET_NAMES):
        raise DatasetRegistrationError(
            "tushare_data_dir is required unless tushare_remote_datasets "
            "selects every Tushare dataset"
        )
    return selected


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
