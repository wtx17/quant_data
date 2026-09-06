"""Dataset registration factories and bound panel handlers.

Each factory validates one registration completely and returns a
:class:`quant_data.models.Dataset` whose ``read_panel`` closure already binds
the resolved source, semantics, and reader functions. The submodules split by
responsibility: the shared ordinary-observation tail, one registration module
per source family, and the registration validation helpers.
"""

from .builtin import builtin_dataset
from .clickhouse import clickhouse_dataset
from .parquet import parquet_dataset
from .tushare import tushare_dataset

__all__ = [
    "builtin_dataset",
    "clickhouse_dataset",
    "parquet_dataset",
    "tushare_dataset",
]
