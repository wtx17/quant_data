#!/usr/bin/env python3
"""Generate the default dataset catalog from initialization specs and local schemas."""

from __future__ import annotations

import argparse
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
NOTES_PATH = ROOT / "tools" / "dataset_descriptions.toml"
OUTPUT_PATH = ROOT / "DATASETS.md"
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from quant_data.backends.clickhouse_catalog import MINGHU_TABLE_COLUMN_TYPES  # noqa: E402
from quant_data.backends.tushare_catalog import (  # noqa: E402
    DisclosureSemantics,
    MembershipSemantics,
    ObservationSemantics,
    TUSHARE_DATASETS,
)
from quant_data.initialize import (  # noqa: E402
    clickhouse_dataset_specs,
    registered_dataset_names,
    tushare_dataset_specs,
)


class CatalogError(RuntimeError):
    """Raised when code catalogs and handwritten descriptions disagree."""


@dataclass(frozen=True, slots=True)
class DatasetReference:
    """Code-derived schema and query contract for one registered dataset."""

    name: str
    fields: tuple[tuple[str, str], ...]
    source_time_column: str
    instrument_column: str
    identity_columns: tuple[str, ...]
    panel_time_column: str


@dataclass(frozen=True, slots=True)
class DatasetNotes:
    """Handwritten title, summary, and field descriptions."""

    title: str
    summary: str
    fields: dict[str, str]


def _add_reference(references: dict[str, DatasetReference], reference: DatasetReference) -> None:
    if reference.name in references:
        raise CatalogError(f"Duplicate initialized dataset: {reference.name!r}")
    field_names = {name for name, _ in reference.fields}
    required_columns = {
        reference.source_time_column,
        reference.instrument_column,
        *reference.identity_columns,
    }
    missing_columns = required_columns.difference(field_names)
    if missing_columns:
        raise CatalogError(
            f"Dataset {reference.name!r} contract columns are absent from its schema: "
            f"{sorted(missing_columns)}"
        )
    references[reference.name] = reference


def collect_references() -> tuple[DatasetReference, ...]:
    """Collect every dataset registered by ``initialize.py`` without remote I/O."""

    references: dict[str, DatasetReference] = {}

    for clickhouse_spec in clickhouse_dataset_specs():
        fields = MINGHU_TABLE_COLUMN_TYPES.get(clickhouse_spec.table)
        if fields is None:
            raise CatalogError(
                f"Initialized ClickHouse table {clickhouse_spec.table!r} "
                "has no local schema catalog"
            )
        _add_reference(
            references,
            DatasetReference(
                name=clickhouse_spec.name,
                fields=fields,
                source_time_column=clickhouse_spec.time_column,
                instrument_column=clickhouse_spec.instrument_column,
                identity_columns=(),
                panel_time_column=clickhouse_spec.time_column,
            ),
        )

    from quant_data.backends.parquet import BUILTIN_MEMBERSHIP_SCHEMA

    _add_reference(
        references,
        DatasetReference(
            name="membership_events",
            fields=tuple((f.name, str(f.type)) for f in BUILTIN_MEMBERSHIP_SCHEMA),
            source_time_column="date",
            instrument_column="code",
            identity_columns=(),
            panel_time_column="date",
        ),
    )

    for tushare_spec in tushare_dataset_specs():
        catalog_name = tushare_spec.dataset or tushare_spec.name
        catalog = TUSHARE_DATASETS.get(catalog_name)
        if catalog is None:
            raise CatalogError(
                f"Initialized Tushare dataset {catalog_name!r} has no local schema catalog"
            )
        semantics = catalog.semantics
        if isinstance(semantics, DisclosureSemantics):
            source_time_column = semantics.period_column
            panel_time_column: str = semantics.panel_time_column
        elif isinstance(semantics, MembershipSemantics):
            source_time_column = semantics.source_time_column
            panel_time_column = semantics.panel_time_column
        elif isinstance(semantics, ObservationSemantics):
            source_time_column = semantics.source_time_column
            panel_time_column = semantics.panel_time_column
        else:  # pragma: no cover - exhaustive catalog guard
            raise CatalogError(f"Unsupported semantics for dataset {tushare_spec.name!r}")
        _add_reference(
            references,
            DatasetReference(
                name=tushare_spec.name,
                fields=tuple((field.name, str(field.type)) for field in catalog.schema),
                source_time_column=source_time_column,
                instrument_column=catalog.instrument_column,
                identity_columns=semantics.identity_columns,
                panel_time_column=panel_time_column,
            ),
        )

    initialized_names = registered_dataset_names()
    missing = set(initialized_names).difference(references)
    extra = set(references).difference(initialized_names)
    if missing or extra:
        raise CatalogError(
            f"Initialized dataset mismatch: missing={sorted(missing)}, extra={sorted(extra)}"
        )
    return tuple(references[name] for name in initialized_names)


def _nonempty_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CatalogError(f"{label} must be a non-empty string")
    return value.strip()


def load_notes(
    references: tuple[DatasetReference, ...],
) -> dict[str, DatasetNotes]:
    """Load handwritten text and require complete, exact source coverage."""

    with NOTES_PATH.open("rb") as handle:
        raw: dict[str, Any] = tomllib.load(handle)
    if set(raw) != {"datasets"} or not isinstance(raw["datasets"], dict):
        raise CatalogError("dataset_descriptions.toml must only define a [datasets] table")

    datasets: dict[str, Any] = raw["datasets"]
    expected_names = {reference.name for reference in references}
    documented_names = set(datasets)
    missing_datasets = expected_names.difference(documented_names)
    extra_datasets = documented_names.difference(expected_names)
    if missing_datasets or extra_datasets:
        raise CatalogError(
            "Description dataset mismatch: "
            f"missing={sorted(missing_datasets)}, extra={sorted(extra_datasets)}"
        )

    notes: dict[str, DatasetNotes] = {}
    for reference in references:
        raw_note = datasets[reference.name]
        if not isinstance(raw_note, dict):
            raise CatalogError(f"Dataset {reference.name!r} must be a TOML table")
        unknown_keys = set(raw_note).difference({"title", "summary", "fields"})
        missing_keys = {"title", "summary", "fields"}.difference(raw_note)
        if missing_keys or unknown_keys:
            raise CatalogError(
                f"Description keys for {reference.name!r} are invalid: "
                f"missing={sorted(missing_keys)}, extra={sorted(unknown_keys)}"
            )
        raw_fields = raw_note["fields"]
        if not isinstance(raw_fields, dict):
            raise CatalogError(f"Dataset {reference.name!r} fields must be a TOML table")

        expected_fields = {name for name, _ in reference.fields}
        documented_fields = set(raw_fields)
        missing_fields = expected_fields.difference(documented_fields)
        extra_fields = documented_fields.difference(expected_fields)
        if missing_fields or extra_fields:
            raise CatalogError(
                f"Description fields for {reference.name!r} are stale: "
                f"missing={sorted(missing_fields)}, extra={sorted(extra_fields)}"
            )
        descriptions: dict[str, str] = {}
        for field_name, _ in reference.fields:
            raw_field = raw_fields[field_name]
            if not isinstance(raw_field, dict) or set(raw_field) != {"description"}:
                raise CatalogError(
                    f"Field {reference.name}.{field_name} must only define description"
                )
            descriptions[field_name] = _nonempty_text(
                raw_field["description"],
                f"Description for {reference.name}.{field_name}",
            )
        notes[reference.name] = DatasetNotes(
            title=_nonempty_text(raw_note["title"], f"Title for {reference.name}"),
            summary=_nonempty_text(raw_note["summary"], f"Summary for {reference.name}"),
            fields=descriptions,
        )
    return notes


def _escape_cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _anchor(dataset_name: str) -> str:
    return "dataset-" + dataset_name.replace("_", "-")


def _field_role(reference: DatasetReference, field_name: str) -> str:
    if field_name == reference.instrument_column:
        return "`get_panel()` 列键"
    if field_name == reference.panel_time_column:
        return "`get_panel()` 索引"
    return "`get_panel()` 可请求值"


def render_catalog(
    references: tuple[DatasetReference, ...],
    notes: dict[str, DatasetNotes],
) -> str:
    """Render the complete user-facing Markdown catalog."""

    lines = [
        "<!-- Generated by tools/generate_dataset_catalog.py; do not edit directly. -->",
        "",
        "# 默认数据集",
        "",
        "本手册列出 `initialize.py` 注册的全部数据集及其可用字段。使用者通过",
        "`get_panel()` 获取 `time × instrument` 宽表。",
        "数据集、字段、类型和键来自源码；字段说明在",
        "`tools/dataset_descriptions.toml` 中人工维护。",
        "",
        "面板索引和列键无需、也不应放入 `fields` 参数；",
        "标为“可请求值”的字段可以放入 `fields`。",
        "",
        "## 数据集索引",
        "",
        "| 数据集 | `get_panel()` |",
        "| --- | --- |",
    ]
    for reference in references:
        lines.append(f"| [`{reference.name}`](#{_anchor(reference.name)}) | 宽表 |")

    for reference in references:
        note = notes[reference.name]
        lines.extend(
            [
                "",
                f'<a id="{_anchor(reference.name)}"></a>',
                f"## `{reference.name}`：{note.title}",
                "",
                note.summary,
                "",
            ]
        )
        lines.append(
            f"- `get_panel()`：按 `{reference.panel_time_column} × "
            f"{reference.instrument_column}` 返回每个请求字段的宽表。"
        )
        lines.extend(
            [
                "",
                "| 字段 | 类型 | 使用方式 | 说明 |",
                "| --- | --- | --- | --- |",
            ]
        )
        for field_name, field_type in reference.fields:
            lines.append(
                "| "
                + " | ".join(
                    _escape_cell(value)
                    for value in (
                        f"`{field_name}`",
                        f"`{field_type}`",
                        _field_role(reference, field_name),
                        note.fields[field_name],
                    )
                )
                + " |"
            )
    lines.append("")
    return "\n".join(lines)


def expected_output() -> str:
    references = collect_references()
    return render_catalog(references, load_notes(references))


def sync(*, check: bool) -> int:
    content = expected_output()
    current = OUTPUT_PATH.read_text(encoding="utf-8") if OUTPUT_PATH.exists() else None
    if current == content:
        print("checked DATASETS.md" if check else "DATASETS.md is already current")
        return 0
    if check:
        print(
            "stale generated dataset catalog: DATASETS.md\n"
            "run: python tools/generate_dataset_catalog.py",
            file=sys.stderr,
        )
        return 1
    OUTPUT_PATH.write_text(content, encoding="utf-8")
    print("updated DATASETS.md")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail when DATASETS.md differs instead of writing it",
    )
    args = parser.parse_args()
    try:
        return sync(check=args.check)
    except (CatalogError, OSError, tomllib.TOMLDecodeError) as exc:
        print(f"dataset catalog generation failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
