from __future__ import annotations

from typing import Any, Sequence

import pyarrow as pa

from harlequin import (
    HarlequinAdapter,
    HarlequinConnection,
    HarlequinCursor,
)
from harlequin.autocomplete.completion import HarlequinCompletion
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from textual_fastdatatable.backend import AutoBackendType

from datafusion import SessionContext

_mapping = {
    pa.int64() : "##"
}

class DataFusionCursor(HarlequinCursor):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.cur = args[0]
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        return [(field.name, _mapping.get(field.type, field.type)) for field in self.cur.schema()]

    def set_limit(self, limit: int) -> DataFusionCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            if self._limit is None:
                return self.cur.to_arrow_table()
            else:
                return self.cur.limit(self._limit).to_arrow_table()
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e


def _list_schemas_in_db(catalog):
    return [(name, catalog.database(name)) for name in catalog.names()]


def _list_relations_in_schema(schema):
    return [(name, (table := schema.table(name)), table.kind) for name in schema.names()]


def _list_columns_in_relation(relation):
    return [(field.name, field.type) for field in list(relation.schema)]


def _list_databases(conn):
    names = conn.sql("select distinct table_catalog from information_schema.tables;").to_pydict()["table_catalog"]
    return [(name, conn.catalog(name)) for name in names]

class DataFusionConnection(HarlequinConnection):
    def __init__(
        self, conn_str: Sequence[str], *args: Any, init_message: str = "", **kwargs: Any
    ) -> None:
        self.init_message = init_message
        try:
            self.conn = SessionContext()
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e), title="Harlequin could not connect to your database."
            ) from e

    def execute(self, query: str) -> HarlequinCursor | None:
        try:
            cur = self.conn.sql(query)  # type: ignore
            if str(cur.logical_plan()) == "EmptyRelation":
                return None
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        else:
            if cur is not None:
                return DataFusionCursor(cur)
            else:
                return None

    def get_catalog(self) -> Catalog:
        databases = _list_databases(self.conn)
        db_items: list[CatalogItem] = []
        for db_name, db in databases:
            schemas = _list_schemas_in_db(db)
            schema_items: list[CatalogItem] = []
            for schema_name, schema in schemas:
                relations = _list_relations_in_schema(schema)
                rel_items: list[CatalogItem] = []
                for rel_name, rel, rel_type in relations:
                    cols = _list_columns_in_relation(rel)
                    col_items = [
                        CatalogItem(
                            qualified_identifier=f'"{db}"."{schema}"."{rel}"."{col}"',
                            query_name=f'"{col}"',
                            label=col,
                            type_label=_mapping.get(col_type, col_type),
                        )
                        for col, col_type in cols
                    ]
                    rel_items.append(
                        CatalogItem(
                            qualified_identifier=f'"{db}"."{schema}"."{rel}"',
                            query_name=f'"{db}"."{schema}"."{rel}"',
                            label=rel,
                            type_label=rel_type,
                            children=col_items,
                        )
                    )
                schema_items.append(
                    CatalogItem(
                        qualified_identifier=f'"{db}"."{schema}"',
                        query_name=f'"{db}"."{schema}"',
                        label=schema,
                        type_label="s",
                        children=rel_items,
                    )
                )
            db_items.append(
                CatalogItem(
                    qualified_identifier=f'"{db}"',
                    query_name=f'"{db}"',
                    label=db,
                    type_label="db",
                    children=schema_items,
                )
            )
        return Catalog(items=db_items)

    def get_completions(self) -> list[HarlequinCompletion]:
        extra_keywords = ["foo", "bar", "baz"]
        return [
            HarlequinCompletion(
                label=item, type_label="kw", value=item, priority=1000, context=None
            )
            for item in extra_keywords
        ]


class DataFusionAdapter(HarlequinAdapter):

    def __init__(self, conn_str: Sequence[str], **options: Any) -> None:
        self.conn_str = conn_str
        self.options = options

    def connect(self) -> DataFusionConnection:
        conn = DataFusionConnection(self.conn_str, self.options)
        return conn
