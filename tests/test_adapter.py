import sys

import pytest
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinQueryError
from harlequin_datafusion.adapter import DataFusionAdapter, DataFusionConnection
from textual_fastdatatable.backend import create_backend

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points


def test_plugin_discovery() -> None:
    PLUGIN_NAME = "datafusion"
    eps = entry_points(group="harlequin.adapter")
    assert eps[PLUGIN_NAME]
    adapter_cls = eps[PLUGIN_NAME].load()
    assert issubclass(adapter_cls, HarlequinAdapter)
    assert adapter_cls == DataFusionAdapter


def test_connect() -> None:
    conn = DataFusionAdapter(conn_str=tuple()).connect()
    assert isinstance(conn, HarlequinConnection)


def test_init_extra_kwargs() -> None:
    assert DataFusionAdapter(conn_str=tuple(), foo=1, bar="baz").connect()


@pytest.fixture
def connection() -> DataFusionConnection:
    return DataFusionAdapter(conn_str=tuple()).connect()


def test_get_catalog(connection: DataFusionConnection) -> None:
    catalog = connection.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items
    assert isinstance(catalog.items[0], CatalogItem)


def test_execute_ddl(connection: DataFusionConnection) -> None:
    cur = connection.execute("create table foo (a int)")
    assert cur is None


def test_execute_select(connection: DataFusionConnection) -> None:
    cur = connection.execute("select 1 as a")
    assert isinstance(cur, HarlequinCursor)
    assert cur.columns() == [("a", "##")]
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 1


def test_execute_select_from_csv(connection: DataFusionConnection) -> None:
    from pathlib import Path

    table = Path(__file__).absolute().parent / "data" / "functional_alltypes.parquet"
    create_query = (
        f"CREATE EXTERNAL TABLE alltypes STORED AS PARQUET LOCATION '{table}';"
    )
    connection.execute(create_query)

    cur = connection.execute("select * from alltypes limit 100;")
    assert isinstance(cur, HarlequinCursor)
    assert cur.columns() == [
        ("id", "##"),
        ("bool_col", "t/f"),
        ("tinyint_col", "##"),
        ("smallint_col", "##"),
        ("int_col", "##"),
        ("bigint_col", "##"),
        ("float_col", "#.#"),
        ("double_col", "#.#"),
        ("date_string_col", "s"),
        ("string_col", "s"),
        ("timestamp_col", "dt"),
        ("year", "##"),
        ("month", "##"),
    ]
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 13
    assert backend.row_count == 100


def test_set_limit(connection: DataFusionConnection) -> None:
    cur = connection.execute("select 1 as a union all select 2 union all select 3")
    assert isinstance(cur, HarlequinCursor)
    cur = cur.set_limit(2)
    assert isinstance(cur, HarlequinCursor)
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 2


def test_execute_raises_query_error(connection: DataFusionConnection) -> None:
    with pytest.raises(HarlequinQueryError):
        _ = connection.execute("selec;")
