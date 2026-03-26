from __future__ import annotations

import os
import re
import socket
import webbrowser
import csv
import io
import json
from uuid import uuid4
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from time import perf_counter
from typing import Any
from urllib.parse import parse_qsl, urlencode

import pymysql
from dotenv import load_dotenv
from flask import Flask, abort, flash, make_response, redirect, render_template, request, session, url_for
from werkzeug.exceptions import HTTPException


SYSTEM_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}
MAX_SQL_PREVIEW_ROWS = 500
SQL_HISTORY_LIMIT = 20
SQL_SNIPPETS_LIMIT = 30
MAX_IDENTIFIER_LENGTH = 128
DEFAULT_SQL_TIMEOUT_MS = 30000
MIN_SQL_TIMEOUT_MS = 1000
MAX_SQL_TIMEOUT_MS = 300000
DATA_SEARCH_MAX_QUERY_LENGTH = 120
DATA_SEARCH_MAX_TABLES = 40
DATA_SEARCH_MAX_COLUMNS_PER_TABLE = 24
DATA_SEARCH_MAX_ROWS_PER_TABLE = 5
DATA_SEARCH_MAX_MATCHED_TABLES = 20
DATA_SEARCH_MAX_TOTAL_ROWS = 100
CREATE_TABLE_ALLOWED_TYPES = {
    "INT",
    "BIGINT",
    "TINYINT",
    "TINYINT(1)",
    "DECIMAL(10,2)",
    "VARCHAR(255)",
    "TEXT",
    "LONGTEXT",
    "DATE",
    "DATETIME",
    "TIMESTAMP",
    "JSON",
}
FK_RULES = {"RESTRICT", "CASCADE", "SET NULL", "NO ACTION"}

NUMERIC_DATA_TYPES = {
    "tinyint",
    "smallint",
    "mediumint",
    "int",
    "integer",
    "bigint",
    "decimal",
    "numeric",
    "float",
    "double",
    "real",
}
TEXTAREA_DATA_TYPES = {"text", "tinytext", "mediumtext", "longtext", "json"}
DATETIME_DATA_TYPES = {"datetime", "timestamp"}
DATE_DATA_TYPES = {"date"}
TIME_DATA_TYPES = {"time"}
SIMPLE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


@dataclass
class DbMeta:
    name: str
    collation: str | None
    table_count: int
    size_mb: float
    is_system: bool


@dataclass
class TableMeta:
    name: str
    rows_count: int
    engine: str | None
    collation: str | None
    size_mb: float


@dataclass
class ColumnMeta:
    name: str
    data_type: str
    column_type: str
    is_nullable: bool
    is_primary: bool
    default: str | None
    extra: str


SKIP_VALUE = object()

load_dotenv()

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "mysql-admin-pro-dev-secret")


def is_authenticated() -> bool:
    return all(
        key in session
        for key in ("mysql_host", "mysql_port", "mysql_user", "mysql_password")
    )


def quote_identifier(name: str) -> str:
    if not name:
        raise ValueError("Identifier cannot be empty")
    return f"`{name.replace('`', '``')}`"


def safe_database_name(name: str) -> bool:
    if not name:
        return False
    if len(name) > MAX_IDENTIFIER_LENGTH:
        return False
    if "\x00" in name or "/" in name:
        return False
    return True


def is_system_database_name(name: str) -> bool:
    return name in SYSTEM_DATABASES


def safe_simple_identifier(name: str) -> bool:
    return safe_database_name(name) and bool(SIMPLE_IDENTIFIER_RE.fullmatch(name))


def normalize_create_table_columns(
    form_data: Any,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    names = [item.strip() for item in form_data.getlist("column_name")]
    types = [item.strip().upper() for item in form_data.getlist("column_type")]
    nullable_values = [item.strip() for item in form_data.getlist("column_nullable")]
    primary_values = [item.strip() for item in form_data.getlist("column_primary")]

    max_len = max(len(names), len(types), len(nullable_values), len(primary_values), 0)
    columns: list[dict[str, Any]] = []
    field_errors: dict[str, str] = {}
    used_names: set[str] = set()
    primary_count = 0

    for index in range(max_len):
        name = names[index] if index < len(names) else ""
        column_type = types[index] if index < len(types) else ""
        nullable_raw = nullable_values[index] if index < len(nullable_values) else "0"
        primary_raw = primary_values[index] if index < len(primary_values) else "0"
        nullable = nullable_raw == "1"
        is_primary = primary_raw == "1"

        if not name and not column_type:
            continue

        if not safe_simple_identifier(name):
            field_errors[f"column_name_{index}"] = "Некорректное имя колонки."

        normalized_key = name.lower()
        if normalized_key in used_names:
            field_errors[f"column_name_{index}"] = "Имена колонок должны быть уникальными."
        if normalized_key:
            used_names.add(normalized_key)

        if column_type not in CREATE_TABLE_ALLOWED_TYPES:
            field_errors[f"column_type_{index}"] = "Недопустимый тип колонки."

        if is_primary:
            primary_count += 1
            nullable = False

        columns.append(
            {
                "name": name,
                "column_type": column_type,
                "nullable": nullable,
                "is_primary": is_primary,
            }
        )

    if not columns:
        field_errors["columns"] = "Добавь хотя бы одну колонку."

    if primary_count > 0 and primary_count != 1:
        field_errors["primary"] = "Пока поддерживается только один PRIMARY KEY."

    return columns, field_errors


def build_column_definition_sql(
    *,
    column_type: str,
    nullable: bool,
    default_mode: str,
    default_value: str,
) -> tuple[str, list[Any], dict[str, str]]:
    errors: dict[str, str] = {}
    params: list[Any] = []
    sql_fragment = f"{column_type} {'NULL' if nullable else 'NOT NULL'}"

    if default_mode not in {"none", "null", "value"}:
        errors["default"] = "Некорректный режим DEFAULT."
        return sql_fragment, params, errors

    if default_mode == "null":
        if not nullable:
            errors["default"] = "DEFAULT NULL возможен только для nullable колонки."
            return sql_fragment, params, errors
        sql_fragment += " DEFAULT NULL"
    elif default_mode == "value":
        sql_fragment += " DEFAULT %s"
        params.append(default_value)

    return sql_fragment, params, errors


def build_existing_column_definition_sql(
    column: ColumnMeta,
) -> tuple[str, list[Any], str | None]:
    extra_upper = column.extra.upper()
    if (
        "GENERATED" in extra_upper
        or "ON UPDATE" in extra_upper
        or "AUTO_INCREMENT" in extra_upper
    ):
        return "", [], "Эта колонка имеет сложное определение и пока не поддерживает reorder."

    sql_fragment = (
        f"{column.column_type} {'NULL' if column.is_nullable else 'NOT NULL'}"
    )
    params: list[Any] = []
    if column.default is None:
        if column.is_nullable:
            sql_fragment += " DEFAULT NULL"
    else:
        default_text = str(column.default).upper()
        if "CURRENT_TIMESTAMP" in default_text:
            return "", [], "Колонка с DEFAULT CURRENT_TIMESTAMP пока не поддерживает reorder."
        sql_fragment += " DEFAULT %s"
        params.append(column.default)

    return sql_fragment, params, None


def is_boolean_column(column: ColumnMeta) -> bool:
    data_type = column.data_type.lower()
    column_type = column.column_type.lower()
    return data_type in {"boolean", "bool"} or column_type in {"tinyint(1)", "bit(1)"}


def parse_enum_set_options(column_type: str) -> list[str]:
    lowered = column_type.lower()
    if not (lowered.startswith("enum(") or lowered.startswith("set(")):
        return []
    return [match.replace("\\'", "'").replace("\\\\", "\\") for match in re.findall(r"'((?:[^'\\]|\\.)*)'", column_type)]


def column_input_type(column: ColumnMeta) -> str:
    data_type = column.data_type.lower()
    if is_boolean_column(column):
        return "boolean"
    if data_type in TEXTAREA_DATA_TYPES:
        return "textarea"
    if data_type in NUMERIC_DATA_TYPES:
        return "number"
    if data_type in DATETIME_DATA_TYPES:
        return "datetime-local"
    if data_type in DATE_DATA_TYPES:
        return "date"
    if data_type in TIME_DATA_TYPES:
        return "time"
    if data_type in {"enum", "set"}:
        return "select"
    return "text"


def column_select_options(column: ColumnMeta) -> list[tuple[str, str]]:
    input_type = column_input_type(column)
    if input_type == "boolean":
        return [("1", "True"), ("0", "False")]
    if input_type == "select":
        return [(option, option) for option in parse_enum_set_options(column.column_type)]
    return []


def column_number_step(column: ColumnMeta) -> str:
    return "any" if column.data_type.lower() in {"decimal", "numeric", "float", "double", "real"} else "1"


def mysql_config() -> dict[str, Any]:
    if not is_authenticated():
        raise RuntimeError("Not authenticated")

    return {
        "host": session["mysql_host"],
        "port": int(session["mysql_port"]),
        "user": session["mysql_user"],
        "password": session["mysql_password"],
        "charset": "utf8mb4",
        "autocommit": True,
        "cursorclass": pymysql.cursors.DictCursor,
    }


def mysql_connect(database: str | None = None) -> pymysql.Connection:
    cfg = mysql_config()
    if database:
        cfg["database"] = database
    return pymysql.connect(**cfg)


def format_cell(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bytes):
        preview = value[:32].hex()
        suffix = "..." if len(value) > 32 else ""
        return f"0x{preview}{suffix}"
    if isinstance(value, (datetime, date, time, Decimal)):
        return str(value)
    return str(value)


def format_uptime(total_seconds: int | None) -> str:
    if total_seconds is None:
        return "Unknown"
    return str(timedelta(seconds=total_seconds))


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def get_collations(conn: pymysql.Connection) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SHOW COLLATION")
        rows = cur.fetchall()
    collations = sorted({row["Collation"] for row in rows if row.get("Collation")})
    preferred = [
        "utf8mb4_unicode_ci",
        "utf8mb4_0900_ai_ci",
        "utf8mb4_general_ci",
        "utf8_general_ci",
        "latin1_swedish_ci",
    ]
    output: list[str] = []
    for item in preferred:
        if item in collations:
            output.append(item)
    for item in collations:
        if item not in output:
            output.append(item)
    return output


def fetch_databases(conn: pymysql.Connection) -> list[DbMeta]:
    sql = """
        SELECT
            s.schema_name AS name,
            s.default_collation_name AS collation,
            COALESCE(t.table_count, 0) AS table_count,
            ROUND(COALESCE(t.total_bytes, 0) / 1024 / 1024, 2) AS size_mb
        FROM information_schema.schemata s
        LEFT JOIN (
            SELECT
                table_schema,
                COUNT(*) AS table_count,
                COALESCE(SUM(data_length + index_length), 0) AS total_bytes
            FROM information_schema.tables
            GROUP BY table_schema
        ) t ON t.table_schema = s.schema_name
        ORDER BY s.schema_name
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    items = [
        DbMeta(
            name=row["name"],
            collation=row["collation"],
            table_count=int(row["table_count"] or 0),
            size_mb=float(row["size_mb"] or 0),
            is_system=row["name"] in SYSTEM_DATABASES,
        )
        for row in rows
    ]

    items.sort(key=lambda i: (i.is_system, i.name.lower()))
    return items


def fetch_tables(conn: pymysql.Connection, db_name: str) -> list[TableMeta]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                table_name AS name,
                COALESCE(table_rows, 0) AS rows_count,
                engine,
                table_collation AS collation,
                ROUND(COALESCE(data_length + index_length, 0) / 1024 / 1024, 2) AS size_mb
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
            """,
            (db_name,),
        )
        rows = cur.fetchall()

    return [
        TableMeta(
            name=row["name"],
            rows_count=int(row["rows_count"] or 0),
            engine=row.get("engine"),
            collation=row.get("collation"),
            size_mb=float(row["size_mb"] or 0),
        )
        for row in rows
    ]


def search_data_across_tables(
    conn: pymysql.Connection,
    db_name: str,
    table_names: list[str],
    search_text: str,
) -> tuple[list[dict[str, Any]], int, int, bool]:
    normalized = search_text.strip()
    if not normalized:
        return [], 0, 0, False

    excluded_types = {
        "binary",
        "varbinary",
        "tinyblob",
        "blob",
        "mediumblob",
        "longblob",
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection",
    }
    table_name_set = set(table_names)
    columns_by_table: dict[str, list[str]] = {}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                c.table_name AS table_name,
                c.column_name AS column_name,
                c.data_type AS data_type
            FROM information_schema.columns c
            WHERE c.table_schema = %s
            ORDER BY c.table_name, c.ordinal_position
            """,
            (db_name,),
        )
        column_rows = cur.fetchall()

        for row in column_rows:
            table_name = str(row["table_name"])
            if table_name not in table_name_set:
                continue
            data_type = str(row.get("data_type") or "").lower()
            if data_type in excluded_types:
                continue
            columns_by_table.setdefault(table_name, []).append(str(row["column_name"]))

        results: list[dict[str, Any]] = []
        scanned_tables = 0
        total_rows = 0
        limit_hit = False
        like_value = f"%{normalized}%"

        for table_name in table_names:
            searchable_columns = columns_by_table.get(table_name, [])
            if not searchable_columns:
                continue
            searchable_columns = searchable_columns[:DATA_SEARCH_MAX_COLUMNS_PER_TABLE]

            if scanned_tables >= DATA_SEARCH_MAX_TABLES:
                limit_hit = True
                break
            scanned_tables += 1

            where_sql = " OR ".join(
                f"CAST({quote_identifier(column)} AS CHAR) LIKE %s"
                for column in searchable_columns
            )
            sql = (
                f"SELECT * FROM {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                f"WHERE {where_sql} LIMIT %s"
            )
            params: list[Any] = [like_value] * len(searchable_columns)
            params.append(DATA_SEARCH_MAX_ROWS_PER_TABLE + 1)

            try:
                cur.execute(sql, params)
            except Exception:
                continue

            raw_rows = cur.fetchall()
            if not raw_rows:
                continue

            has_more = len(raw_rows) > DATA_SEARCH_MAX_ROWS_PER_TABLE
            limited_rows = raw_rows[:DATA_SEARCH_MAX_ROWS_PER_TABLE]
            result_columns = [str(column[0]) for column in cur.description or []]
            formatted_rows = [
                [format_cell(row.get(column)) for column in result_columns]
                for row in limited_rows
            ]
            results.append(
                {
                    "table_name": table_name,
                    "columns": result_columns,
                    "rows": formatted_rows,
                    "row_count": len(limited_rows),
                    "has_more": has_more,
                }
            )
            total_rows += len(limited_rows)

            if (
                len(results) >= DATA_SEARCH_MAX_MATCHED_TABLES
                or total_rows >= DATA_SEARCH_MAX_TOTAL_ROWS
            ):
                limit_hit = True
                break

    return results, scanned_tables, total_rows, limit_hit


def fetch_columns_meta(
    conn: pymysql.Connection, db_name: str, table_name: str
) -> list[ColumnMeta]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                c.column_name AS name,
                c.data_type AS data_type,
                c.column_type AS column_type,
                c.is_nullable AS is_nullable,
                c.column_default AS column_default,
                c.column_key AS column_key,
                c.extra AS extra
            FROM information_schema.columns c
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
            """,
            (db_name, table_name),
        )
        rows = cur.fetchall()

    return [
        ColumnMeta(
            name=row["name"],
            data_type=row["data_type"],
            column_type=row["column_type"],
            is_nullable=(row["is_nullable"] == "YES"),
            is_primary=(row.get("column_key") == "PRI"),
            default=row.get("column_default"),
            extra=(row.get("extra") or ""),
        )
        for row in rows
    ]


def fetch_table_indexes(
    conn: pymysql.Connection, db_name: str, table_name: str
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                s.index_name AS index_name,
                s.non_unique AS non_unique,
                s.index_type AS index_type,
                s.seq_in_index AS seq_in_index,
                s.column_name AS column_name
            FROM information_schema.statistics s
            WHERE s.table_schema = %s AND s.table_name = %s
            ORDER BY s.index_name, s.seq_in_index
            """,
            (db_name, table_name),
        )
        rows = cur.fetchall()

    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row["index_name"])
        item = grouped.setdefault(
            name,
            {
                "name": name,
                "index_type": str(row.get("index_type") or "BTREE"),
                "is_unique": int(row.get("non_unique") or 0) == 0,
                "is_primary": name == "PRIMARY",
                "columns": [],
            },
        )
        item["columns"].append(str(row["column_name"]))

    items = list(grouped.values())
    items.sort(key=lambda i: (not i["is_primary"], i["name"].lower()))
    return items


def fetch_table_foreign_keys(
    conn: pymysql.Connection, db_name: str, table_name: str
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                k.constraint_name AS constraint_name,
                k.column_name AS column_name,
                k.referenced_table_schema AS referenced_table_schema,
                k.referenced_table_name AS referenced_table_name,
                k.referenced_column_name AS referenced_column_name,
                k.ordinal_position AS ordinal_position,
                r.update_rule AS update_rule,
                r.delete_rule AS delete_rule
            FROM information_schema.key_column_usage k
            JOIN information_schema.referential_constraints r
              ON r.constraint_schema = k.constraint_schema
             AND r.constraint_name = k.constraint_name
             AND r.table_name = k.table_name
            WHERE
                k.table_schema = %s
                AND k.table_name = %s
                AND k.referenced_table_name IS NOT NULL
            ORDER BY k.constraint_name, k.ordinal_position
            """,
            (db_name, table_name),
        )
        rows = cur.fetchall()

    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row["constraint_name"])
        item = grouped.setdefault(
            name,
            {
                "name": name,
                "referenced_table_schema": str(row["referenced_table_schema"]),
                "referenced_table_name": str(row["referenced_table_name"]),
                "update_rule": str(row.get("update_rule") or "RESTRICT"),
                "delete_rule": str(row.get("delete_rule") or "RESTRICT"),
                "columns": [],
                "referenced_columns": [],
            },
        )
        item["columns"].append(str(row["column_name"]))
        item["referenced_columns"].append(str(row["referenced_column_name"]))

    items = list(grouped.values())
    items.sort(key=lambda i: i["name"].lower())
    return items


def fetch_primary_key_columns(
    conn: pymysql.Connection, db_name: str, table_name: str
) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT k.column_name AS column_name
            FROM information_schema.key_column_usage k
            WHERE
                k.table_schema = %s
                AND k.table_name = %s
                AND k.constraint_name = 'PRIMARY'
            ORDER BY k.ordinal_position
            """,
            (db_name, table_name),
        )
        rows = cur.fetchall()
    return [str(row["column_name"]) for row in rows]


def format_form_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (datetime, date, time, Decimal)):
        return str(value)
    return str(value)


def format_export_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (datetime, date, time, Decimal)):
        return str(value)
    return str(value)


def json_export_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (datetime, date, time, Decimal)):
        return str(value)
    return value


def format_input_value(column: ColumnMeta, value: Any) -> str:
    if value is None:
        return ""

    input_type = column_input_type(column)
    string_value = format_form_value(value)

    if input_type == "boolean":
        lowered = string_value.lower()
        if lowered in {"1", "true", "t", "yes", "y"}:
            return "1"
        if lowered in {"0", "false", "f", "no", "n"}:
            return "0"
        return ""

    if input_type == "datetime-local":
        if " " in string_value:
            string_value = string_value.replace(" ", "T", 1)
        return string_value

    return string_value


def normalize_form_value(raw_value: str, column: ColumnMeta, for_insert: bool) -> Any:
    if raw_value != "":
        input_type = column_input_type(column)
        if input_type == "datetime-local":
            return raw_value.replace("T", " ", 1)
        if input_type == "boolean":
            return 1 if raw_value in {"1", "true", "True", "on"} else 0
        return raw_value

    extra_upper = column.extra.upper()
    if for_insert and ("AUTO_INCREMENT" in extra_upper or column.default is not None):
        return SKIP_VALUE

    if "GENERATED" in extra_upper:
        return SKIP_VALUE

    if column.is_nullable:
        return None

    return ""


def build_pk_filters(pk_columns: list[str], values: Mapping[str, Any]) -> list[tuple[str, Any]] | None:
    filters: list[tuple[str, Any]] = []
    for column in pk_columns:
        key = f"pk_{column}"
        if key not in values:
            return None
        filters.append((column, values[key]))
    return filters


def get_sql_history() -> list[dict[str, str]]:
    raw_history = session.get("sql_history", [])
    if not isinstance(raw_history, list):
        return []

    normalized: list[dict[str, str]] = []
    for item in raw_history:
        if not isinstance(item, dict):
            continue
        query = str(item.get("query", "")).strip()
        if not query:
            continue
        normalized.append(
            {
                "db": str(item.get("db", "")).strip(),
                "query": query,
                "executed_at": str(item.get("executed_at", "")).strip(),
            }
        )

    return normalized[:SQL_HISTORY_LIMIT]


def push_sql_history(db_name: str, query_text: str) -> None:
    normalized_query = query_text.strip()
    if not normalized_query:
        return

    new_item = {
        "db": db_name,
        "query": normalized_query,
        "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    existing = [item for item in get_sql_history() if not (item["db"] == db_name and item["query"] == normalized_query)]
    session["sql_history"] = [new_item] + existing[: SQL_HISTORY_LIMIT - 1]
    session.modified = True


def get_sql_snippets() -> list[dict[str, str]]:
    raw_snippets = session.get("sql_snippets", [])
    if not isinstance(raw_snippets, list):
        return []

    normalized: list[dict[str, str]] = []
    for item in raw_snippets:
        if not isinstance(item, dict):
            continue

        snippet_id = str(item.get("id", "")).strip()
        db_name = str(item.get("db", "")).strip()
        name = str(item.get("name", "")).strip()
        query = str(item.get("query", "")).strip()
        saved_at = str(item.get("saved_at", "")).strip()
        if not snippet_id or not db_name or not name or not query:
            continue

        normalized.append(
            {
                "id": snippet_id,
                "db": db_name,
                "name": name,
                "query": query,
                "saved_at": saved_at,
            }
        )

    return normalized[:SQL_SNIPPETS_LIMIT]


def derive_sql_snippet_name(query_text: str) -> str:
    compact = " ".join(query_text.strip().split())
    if not compact:
        return "Snippet"
    if len(compact) <= 64:
        return compact
    return f"{compact[:61]}..."


def save_sql_snippet(db_name: str, snippet_name: str, query_text: str) -> str | None:
    name = snippet_name.strip() or derive_sql_snippet_name(query_text)
    if len(name) > 80:
        name = f"{name[:77]}..."

    normalized_query = query_text.strip()
    if not normalized_query:
        return None

    new_item = {
        "id": uuid4().hex,
        "db": db_name,
        "name": name,
        "query": normalized_query,
        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    existing = [
        item
        for item in get_sql_snippets()
        if not (
            item["db"] == db_name
            and item["name"].lower() == name.lower()
        )
    ]
    session["sql_snippets"] = [new_item] + existing[: SQL_SNIPPETS_LIMIT - 1]
    session.modified = True
    return new_item["id"]


def delete_sql_snippet(db_name: str, snippet_id: str) -> bool:
    normalized_id = snippet_id.strip()
    if not normalized_id:
        return False

    existing = get_sql_snippets()
    filtered = [
        item
        for item in existing
        if not (item["db"] == db_name and item["id"] == normalized_id)
    ]
    changed = len(filtered) != len(existing)
    if changed:
        session["sql_snippets"] = filtered[:SQL_SNIPPETS_LIMIT]
        session.modified = True
    return changed


def coerce_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def coerce_int_in_range(value: Any, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return default
    if parsed < min_value:
        return min_value
    if parsed > max_value:
        return max_value
    return parsed


def render_error_page(
    *,
    title: str,
    message: str,
    status_code: int = 500,
    back_url: str | None = None,
) -> tuple[str, int]:
    return (
        render_template(
            "error.html",
            error_title=title,
            error_message=message,
            back_url=back_url,
            status_code=status_code,
        ),
        status_code,
    )


def parse_table_return_state(source: Mapping[str, Any]) -> tuple[int, int, str]:
    return_page = coerce_positive_int(source.get("return_page") or source.get("page"), 1)
    return_limit = min(
        500, coerce_positive_int(source.get("return_limit") or source.get("limit"), 50)
    )
    return_tables_q = str(
        source.get("return_tables_q") or source.get("tables_q") or ""
    ).strip()
    return return_page, return_limit, return_tables_q


def normalize_query_string(raw_query: str) -> str:
    value = raw_query.strip().lstrip("?")
    if not value:
        return ""
    try:
        pairs = parse_qsl(value, keep_blank_values=True)
    except ValueError:
        return ""
    return urlencode(pairs, doseq=True)


def build_table_view_return_url(
    *,
    db_name: str,
    table_name: str,
    return_query: str,
    return_page: int,
    return_limit: int,
    return_tables_q: str,
) -> str:
    normalized_query = normalize_query_string(return_query)
    if normalized_query:
        return (
            f"{url_for('table_view', db_name=db_name, table_name=table_name)}"
            f"?{normalized_query}"
        )
    return url_for(
        "table_view",
        db_name=db_name,
        table_name=table_name,
        page=return_page,
        limit=return_limit,
        tables_q=return_tables_q,
    )


def fetch_row_by_pk(
    conn: pymysql.Connection,
    db_name: str,
    table_name: str,
    pk_filters: list[tuple[str, Any]],
) -> dict[str, Any] | None:
    where_sql = " AND ".join(f"{quote_identifier(column)} = %s" for column, _ in pk_filters)
    params = [value for _, value in pk_filters]
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM {quote_identifier(db_name)}.{quote_identifier(table_name)} "
            f"WHERE {where_sql} LIMIT 1",
            params,
        )
        return cur.fetchone()


def database_exists(conn: pymysql.Connection, db_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
            (db_name,),
        )
        return cur.fetchone() is not None


def fetch_database_collation(conn: pymysql.Connection, db_name: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT default_collation_name AS collation FROM information_schema.schemata WHERE schema_name = %s",
            (db_name,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return row.get("collation")


def table_exists(conn: pymysql.Connection, db_name: str, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (db_name, table_name),
        )
        return cur.fetchone() is not None


def get_server_version(conn: pymysql.Connection) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT VERSION() AS version")
        row = cur.fetchone()
    return row["version"] if row else "Unknown"


def get_uptime_seconds(conn: pymysql.Connection) -> int | None:
    with conn.cursor() as cur:
        cur.execute("SHOW GLOBAL STATUS LIKE 'Uptime'")
        row = cur.fetchone()
    if not row:
        return None
    value = row.get("Value")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@app.context_processor
def inject_common() -> dict[str, Any]:
    return {
        "connected": is_authenticated(),
        "mysql_host": session.get("mysql_host", "127.0.0.1"),
        "mysql_port": session.get("mysql_port", 3306),
        "mysql_user": session.get("mysql_user"),
        "server_version": session.get("server_version", "Unknown"),
        "column_input_type": column_input_type,
        "column_select_options": column_select_options,
        "column_number_step": column_number_step,
    }


@app.route("/")
def index() -> Any:
    if is_authenticated():
        return redirect(url_for("databases"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login() -> Any:
    if request.method == "GET" and is_authenticated():
        return redirect(url_for("databases"))

    if request.method == "POST":
        host = request.form.get("host", "127.0.0.1").strip() or "127.0.0.1"
        port_raw = request.form.get("port", "3306").strip() or "3306"
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        field_errors: dict[str, str] = {}

        if not host:
            field_errors["host"] = "Укажи сервер."

        if not username:
            field_errors["username"] = "Введи пользователя MySQL."

        try:
            port = int(port_raw)
            if port <= 0 or port > 65535:
                field_errors["port"] = "Порт должен быть в диапазоне 1-65535."
        except ValueError:
            field_errors["port"] = "Порт должен быть числом."

        if field_errors:
            flash("Исправь ошибки в форме.", "error")
            return render_template(
                "login.html",
                host=host,
                port=port_raw,
                username=username,
                field_errors=field_errors,
            )

        try:
            conn = pymysql.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                charset="utf8mb4",
                autocommit=True,
                cursorclass=pymysql.cursors.DictCursor,
            )
            try:
                version = get_server_version(conn)
            finally:
                conn.close()
        except Exception as exc:  # pragma: no cover - runtime-specific
            flash(f"Не удалось подключиться: {exc}", "error")
            return render_template(
                "login.html",
                host=host,
                port=port_raw,
                username=username,
                field_errors={},
            )

        session["mysql_host"] = host
        session["mysql_port"] = port
        session["mysql_user"] = username
        session["mysql_password"] = password
        session["server_version"] = version
        flash("Подключение к MySQL установлено.", "success")

        return redirect(url_for("databases"))

    return render_template(
        "login.html",
        host="127.0.0.1",
        port="3306",
        username="",
        field_errors={},
    )


@app.route("/logout", methods=["POST"])
def logout() -> Any:
    session.clear()
    return redirect(url_for("login"))


@app.route("/databases")
def databases() -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    filter_query = request.args.get("q", "").strip().lower()
    try:
        conn = mysql_connect()
        try:
            db_items = fetch_databases(conn)
            uptime = format_uptime(get_uptime_seconds(conn))
            session["server_version"] = get_server_version(conn)
        finally:
            conn.close()
    except Exception as exc:  # pragma: no cover - runtime-specific
        return render_error_page(
            title="Ошибка чтения баз данных",
            message=str(exc),
            status_code=503,
            back_url=url_for("login"),
        )

    if filter_query:
        db_items = [item for item in db_items if filter_query in item.name.lower()]

    return render_template(
        "databases.html",
        databases=db_items,
        total_databases=len(db_items),
        uptime=uptime,
        active_page="databases",
        filter_query=request.args.get("q", ""),
    )


@app.route("/databases/new", methods=["GET", "POST"])
def create_database() -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    try:
        conn = mysql_connect()
    except Exception as exc:  # pragma: no cover - runtime-specific
        return render_error_page(
            title="Ошибка подключения к MySQL",
            message=str(exc),
            status_code=503,
            back_url=url_for("login"),
        )

    try:
        db_items = fetch_databases(conn)
        collations = get_collations(conn)

        if request.method == "POST":
            database_name = request.form.get("database_name", "").strip()
            collation = request.form.get("collation", "utf8mb4_unicode_ci").strip()
            field_errors: dict[str, str] = {}

            if not safe_database_name(database_name):
                field_errors["database_name"] = "Имя БД не должно быть пустым и не может содержать '/'."

            if collation not in collations:
                field_errors["collation"] = "Выбрана недоступная collation."

            if field_errors:
                flash("Исправь ошибки в форме.", "error")
                return render_template(
                    "new_database.html",
                    databases=db_items,
                    collations=collations,
                    selected_collation=collation,
                    database_name=database_name,
                    field_errors=field_errors,
                )

            with conn.cursor() as cur:
                sql = (
                    f"CREATE DATABASE {quote_identifier(database_name)} "
                    f"COLLATE {collation}"
                )
                cur.execute(sql)

            flash(f"База данных '{database_name}' создана.", "success")
            return redirect(url_for("database_tables", db_name=database_name))

        return render_template(
            "new_database.html",
            databases=db_items,
            collations=collations,
            selected_collation="utf8mb4_unicode_ci",
            database_name="",
            field_errors={},
        )
    except Exception as exc:  # pragma: no cover - runtime-specific
        return render_error_page(
            title="Ошибка создания базы данных",
            message=str(exc),
            status_code=500,
            back_url=url_for("databases"),
        )
    finally:
        conn.close()


@app.route("/databases/<db_name>/rename", methods=["POST"])
def rename_database(db_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))
    if not safe_database_name(db_name):
        abort(404)

    filter_query = request.form.get("q", "").strip()
    new_name = request.form.get("new_name", "").strip()
    confirm_name = request.form.get("confirm_name", "").strip()

    if is_system_database_name(db_name):
        flash("Системные базы переименовывать нельзя.", "error")
        return redirect(url_for("databases", q=filter_query))
    if confirm_name != db_name:
        flash("Подтверждение не совпало с именем базы.", "error")
        return redirect(url_for("databases", q=filter_query))
    if not safe_simple_identifier(new_name):
        flash("Новое имя базы должно содержать только латиницу, цифры и _.", "error")
        return redirect(url_for("databases", q=filter_query))
    if new_name == db_name:
        flash("Новое имя совпадает с текущим.", "error")
        return redirect(url_for("databases", q=filter_query))

    try:
        conn = mysql_connect()
        try:
            if not database_exists(conn, db_name):
                flash(f"База '{db_name}' не найдена.", "error")
                return redirect(url_for("databases", q=filter_query))
            if database_exists(conn, new_name):
                flash(f"База '{new_name}' уже существует.", "error")
                return redirect(url_for("databases", q=filter_query))

            collation = fetch_database_collation(conn, db_name) or "utf8mb4_unicode_ci"
            tables = fetch_tables(conn, db_name)

            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE DATABASE {quote_identifier(new_name)} COLLATE {collation}"
                )
                for table in tables:
                    cur.execute(
                        f"RENAME TABLE {quote_identifier(db_name)}.{quote_identifier(table.name)} "
                        f"TO {quote_identifier(new_name)}.{quote_identifier(table.name)}"
                    )
                cur.execute(f"DROP DATABASE {quote_identifier(db_name)}")
        finally:
            conn.close()
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(
            f"Не удалось переименовать базу: {exc}. Проверь состояние БД вручную.",
            "error",
        )
        return redirect(url_for("databases", q=filter_query))

    flash(f"База '{db_name}' переименована в '{new_name}'.", "success")
    return redirect(url_for("database_tables", db_name=new_name))


@app.route("/databases/<db_name>/drop", methods=["POST"])
def drop_database(db_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))
    if not safe_database_name(db_name):
        abort(404)

    filter_query = request.form.get("q", "").strip()
    confirm_name = request.form.get("confirm_name", "").strip()

    if is_system_database_name(db_name):
        flash("Системные базы удалять нельзя.", "error")
        return redirect(url_for("databases", q=filter_query))
    if confirm_name != db_name:
        flash("Подтверждение не совпало с именем базы.", "error")
        return redirect(url_for("databases", q=filter_query))

    try:
        conn = mysql_connect()
        try:
            if not database_exists(conn, db_name):
                flash(f"База '{db_name}' не найдена.", "error")
                return redirect(url_for("databases", q=filter_query))
            with conn.cursor() as cur:
                cur.execute(f"DROP DATABASE {quote_identifier(db_name)}")
        finally:
            conn.close()
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Не удалось удалить базу: {exc}", "error")
        return redirect(url_for("databases", q=filter_query))

    flash(f"База '{db_name}' удалена.", "success")
    return redirect(url_for("databases", q=filter_query))


@app.route("/databases/<db_name>/tables/new", methods=["GET", "POST"])
def create_table(db_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))
    if not safe_database_name(db_name):
        abort(404)

    table_name = request.form.get("table_name", "").strip() if request.method == "POST" else ""
    table_filter = (request.form.get("q") if request.method == "POST" else request.args.get("q", "")) or ""
    table_filter = table_filter.strip()
    field_errors: dict[str, str] = {}
    columns_data: list[dict[str, Any]] = []

    try:
        conn = mysql_connect()
        try:
            if not database_exists(conn, db_name):
                abort(404)
            db_items = fetch_databases(conn)
            table_items = fetch_tables(conn, db_name)
            total_rows = sum(t.rows_count for t in table_items)
            total_size = round(sum(t.size_mb for t in table_items), 2)

            if request.method == "POST":
                columns_data, column_errors = normalize_create_table_columns(request.form)
                field_errors.update(column_errors)

                if not safe_simple_identifier(table_name):
                    field_errors["table_name"] = "Имя таблицы: только латиница, цифры и _."
                elif table_exists(conn, db_name, table_name):
                    field_errors["table_name"] = "Таблица с таким именем уже существует."

                if field_errors:
                    flash("Исправь ошибки в форме.", "error")
                else:
                    definitions: list[str] = []
                    for column in columns_data:
                        fragment = (
                            f"{quote_identifier(column['name'])} {column['column_type']}"
                        )
                        if not column["nullable"]:
                            fragment += " NOT NULL"
                        definitions.append(fragment)

                    primary_columns = [
                        quote_identifier(column["name"])
                        for column in columns_data
                        if column["is_primary"]
                    ]
                    if primary_columns:
                        definitions.append(f"PRIMARY KEY ({', '.join(primary_columns)})")

                    create_sql = (
                        f"CREATE TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                        f"({', '.join(definitions)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
                    )
                    with conn.cursor() as cur:
                        cur.execute(create_sql)

                    flash(f"Таблица '{table_name}' создана.", "success")
                    return redirect(
                        url_for(
                            "table_view",
                            db_name=db_name,
                            table_name=table_name,
                            tables_q=table_filter,
                        )
                    )
            else:
                columns_data = [
                    {
                        "name": "id",
                        "column_type": "BIGINT",
                        "nullable": False,
                        "is_primary": True,
                    },
                    {
                        "name": "created_at",
                        "column_type": "DATETIME",
                        "nullable": False,
                        "is_primary": False,
                    },
                ]
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        return render_error_page(
            title="Ошибка создания таблицы",
            message=str(exc),
            status_code=500,
            back_url=url_for("database_tables", db_name=db_name, q=table_filter),
        )

    return render_template(
        "new_table.html",
        databases=db_items if "db_items" in locals() else [],
        current_db=db_name,
        tables=table_items if "table_items" in locals() else [],
        total_tables=len(table_items) if "table_items" in locals() else 0,
        total_rows=total_rows if "total_rows" in locals() else 0,
        total_size=total_size if "total_size" in locals() else 0.0,
        table_name=table_name,
        table_filter=table_filter,
        columns_data=columns_data,
        field_errors=field_errors,
        allowed_column_types=sorted(CREATE_TABLE_ALLOWED_TYPES),
    )


@app.route("/databases/<db_name>/tables/<table_name>/rename", methods=["POST"])
def rename_table(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))
    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    filter_query = request.form.get("q", "").strip()
    new_table_name = request.form.get("new_table_name", "").strip()

    if not safe_simple_identifier(new_table_name):
        flash("Новое имя таблицы должно содержать только латиницу, цифры и _.", "error")
        return redirect(url_for("database_tables", db_name=db_name, q=filter_query))
    if new_table_name == table_name:
        flash("Новое имя совпадает с текущим.", "error")
        return redirect(url_for("database_tables", db_name=db_name, q=filter_query))

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                flash(f"Таблица '{table_name}' не найдена.", "error")
                return redirect(url_for("database_tables", db_name=db_name, q=filter_query))
            if table_exists(conn, db_name, new_table_name):
                flash(f"Таблица '{new_table_name}' уже существует.", "error")
                return redirect(url_for("database_tables", db_name=db_name, q=filter_query))
            with conn.cursor() as cur:
                cur.execute(
                    f"RENAME TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                    f"TO {quote_identifier(db_name)}.{quote_identifier(new_table_name)}"
                )
        finally:
            conn.close()
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Не удалось переименовать таблицу: {exc}", "error")
        return redirect(url_for("database_tables", db_name=db_name, q=filter_query))

    flash(f"Таблица '{table_name}' переименована в '{new_table_name}'.", "success")
    return redirect(url_for("database_tables", db_name=db_name, q=filter_query))


@app.route("/databases/<db_name>/tables/<table_name>/truncate", methods=["POST"])
def truncate_table(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))
    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    filter_query = request.form.get("q", "").strip()
    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                flash(f"Таблица '{table_name}' не найдена.", "error")
                return redirect(url_for("database_tables", db_name=db_name, q=filter_query))
            with conn.cursor() as cur:
                cur.execute(
                    f"TRUNCATE TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)}"
                )
        finally:
            conn.close()
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Не удалось очистить таблицу: {exc}", "error")
        return redirect(url_for("database_tables", db_name=db_name, q=filter_query))

    flash(f"Таблица '{table_name}' очищена.", "success")
    return redirect(url_for("database_tables", db_name=db_name, q=filter_query))


@app.route("/databases/<db_name>/tables/<table_name>/drop", methods=["POST"])
def drop_table(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))
    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    filter_query = request.form.get("q", "").strip()
    confirm_name = request.form.get("confirm_name", "").strip()
    if confirm_name != table_name:
        flash("Подтверждение удаления не совпало с именем таблицы.", "error")
        return redirect(url_for("database_tables", db_name=db_name, q=filter_query))

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                flash(f"Таблица '{table_name}' не найдена.", "error")
                return redirect(url_for("database_tables", db_name=db_name, q=filter_query))
            with conn.cursor() as cur:
                cur.execute(
                    f"DROP TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)}"
                )
        finally:
            conn.close()
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Не удалось удалить таблицу: {exc}", "error")
        return redirect(url_for("database_tables", db_name=db_name, q=filter_query))

    flash(f"Таблица '{table_name}' удалена.", "success")
    return redirect(url_for("database_tables", db_name=db_name, q=filter_query))


@app.route("/databases/<db_name>/tables/<table_name>/duplicate", methods=["POST"])
def duplicate_table(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))
    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    filter_query = request.form.get("q", "").strip()
    new_table_name = request.form.get("new_table_name", "").strip()
    duplicate_mode = request.form.get("duplicate_mode", "structure_only").strip()

    if duplicate_mode not in {"structure_only", "structure_and_data"}:
        flash("Некорректный режим дублирования.", "error")
        return redirect(url_for("database_tables", db_name=db_name, q=filter_query))
    if not safe_simple_identifier(new_table_name):
        flash("Имя новой таблицы должно содержать только латиницу, цифры и _.", "error")
        return redirect(url_for("database_tables", db_name=db_name, q=filter_query))
    if new_table_name == table_name:
        flash("Имя новой таблицы должно отличаться от исходной.", "error")
        return redirect(url_for("database_tables", db_name=db_name, q=filter_query))

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                flash(f"Таблица '{table_name}' не найдена.", "error")
                return redirect(url_for("database_tables", db_name=db_name, q=filter_query))
            if table_exists(conn, db_name, new_table_name):
                flash(f"Таблица '{new_table_name}' уже существует.", "error")
                return redirect(url_for("database_tables", db_name=db_name, q=filter_query))

            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE {quote_identifier(db_name)}.{quote_identifier(new_table_name)} "
                    f"LIKE {quote_identifier(db_name)}.{quote_identifier(table_name)}"
                )
                inserted_rows = 0
                if duplicate_mode == "structure_and_data":
                    cur.execute(
                        f"INSERT INTO {quote_identifier(db_name)}.{quote_identifier(new_table_name)} "
                        f"SELECT * FROM {quote_identifier(db_name)}.{quote_identifier(table_name)}"
                    )
                    inserted_rows = max(cur.rowcount, 0)
        finally:
            conn.close()
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Не удалось дублировать таблицу: {exc}", "error")
        return redirect(url_for("database_tables", db_name=db_name, q=filter_query))

    if duplicate_mode == "structure_and_data":
        flash(
            f"Таблица '{new_table_name}' создана (структура + данные, строк: {inserted_rows}).",
            "success",
        )
    else:
        flash(f"Таблица '{new_table_name}' создана (только структура).", "success")
    return redirect(url_for("database_tables", db_name=db_name, q=filter_query))


@app.route("/databases/<db_name>/tables")
def database_tables(db_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name):
        abort(404)

    filter_query_raw = request.args.get("q", "").strip()
    filter_query = filter_query_raw.lower()
    data_search_query_raw = request.args.get("data_q", "").strip()
    data_search_query = data_search_query_raw[:DATA_SEARCH_MAX_QUERY_LENGTH]
    data_search_query_trimmed = len(data_search_query_raw) > DATA_SEARCH_MAX_QUERY_LENGTH
    data_search_results: list[dict[str, Any]] = []
    data_search_scanned_tables = 0
    data_search_total_rows = 0
    data_search_limit_hit = False
    data_search_ms: float | None = None

    try:
        conn = mysql_connect()
        try:
            if not database_exists(conn, db_name):
                abort(404)

            db_items = fetch_databases(conn)
            table_items_all = fetch_tables(conn, db_name)
            table_items = table_items_all

            if filter_query:
                table_items = [
                    item for item in table_items if filter_query in item.name.lower()
                ]

            total_rows = sum(t.rows_count for t in table_items)
            total_size = round(sum(t.size_mb for t in table_items), 2)

            if data_search_query:
                start = perf_counter()
                (
                    data_search_results,
                    data_search_scanned_tables,
                    data_search_total_rows,
                    data_search_limit_hit,
                ) = search_data_across_tables(
                    conn,
                    db_name,
                    [table.name for table in table_items_all],
                    data_search_query,
                )
                data_search_ms = (perf_counter() - start) * 1000
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        return render_error_page(
            title="Ошибка чтения таблиц",
            message=str(exc),
            status_code=500,
            back_url=url_for("databases"),
        )

    return render_template(
        "tables.html",
        databases=db_items,
        current_db=db_name,
        tables=table_items,
        total_tables=len(table_items),
        total_rows=total_rows,
        total_size=total_size,
        filter_query=filter_query_raw,
        data_search_query=data_search_query,
        data_search_query_trimmed=data_search_query_trimmed,
        data_search_results=data_search_results,
        data_search_scanned_tables=data_search_scanned_tables,
        data_search_total_rows=data_search_total_rows,
        data_search_limit_hit=data_search_limit_hit,
        data_search_ms=round(data_search_ms, 2) if data_search_ms is not None else None,
    )


@app.route("/databases/<db_name>/sql", methods=["GET", "POST"])
def sql_console(db_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name):
        abort(404)

    query_text = "SELECT NOW();"
    export_format = ""
    if request.method == "POST":
        query_text = request.form.get("sql_query", "SELECT NOW();").strip()
        export_format = request.form.get("export_format", "").strip().lower()
        if export_format not in {"", "csv", "json"}:
            export_format = ""
    result_columns: list[str] = []
    result_rows: list[list[str]] = []
    result_count = 0
    affected_rows: int | None = None
    query_ms: float | None = None
    result_truncated = False
    sql_history = [item for item in get_sql_history() if item["db"] == db_name]
    sql_snippets = [item for item in get_sql_snippets() if item["db"] == db_name]
    timeout_source = (
        request.form.get("timeout_ms")
        if request.method == "POST"
        else request.args.get("timeout_ms", session.get("sql_timeout_ms", DEFAULT_SQL_TIMEOUT_MS))
    )
    timeout_ms = coerce_int_in_range(
        timeout_source,
        DEFAULT_SQL_TIMEOUT_MS,
        MIN_SQL_TIMEOUT_MS,
        MAX_SQL_TIMEOUT_MS,
    )
    current_table = request.values.get("table", "").strip()
    if current_table and not safe_database_name(current_table):
        current_table = ""
    return_page, return_limit, tables_q = parse_table_return_state(request.values)

    if request.method == "GET":
        history_raw = request.args.get("history")
        if history_raw is not None:
            try:
                history_index = int(history_raw)
                if 0 <= history_index < len(sql_history):
                    query_text = sql_history[history_index]["query"]
            except ValueError:
                pass

        snippet_id = request.args.get("snippet_id", "").strip()
        if snippet_id:
            selected_snippet = next(
                (item for item in sql_snippets if item["id"] == snippet_id),
                None,
            )
            if selected_snippet:
                query_text = selected_snippet["query"]

    if request.method == "POST":
        try:
            requested_timeout = int(str(timeout_source))
            if requested_timeout != timeout_ms:
                flash(
                    f"Таймаут скорректирован до {timeout_ms} ms (допустимо: {MIN_SQL_TIMEOUT_MS}-{MAX_SQL_TIMEOUT_MS}).",
                    "error",
                )
        except (TypeError, ValueError):
            flash(
                f"Таймаут должен быть числом. Использовано значение {timeout_ms} ms.",
                "error",
            )

    try:
        conn = mysql_connect()
        try:
            if not database_exists(conn, db_name):
                abort(404)

            db_items = fetch_databases(conn)
            table_items = fetch_tables(conn, db_name)

            if request.method == "POST":
                if not query_text:
                    flash("Введите SQL-запрос.", "error")
                else:
                    try:
                        start = perf_counter()
                        with conn.cursor() as cur:
                            cur.execute(f"USE {quote_identifier(db_name)}")
                            session["sql_timeout_ms"] = timeout_ms
                            session.modified = True
                            # Cross-compatible execution limits: MySQL and MariaDB.
                            try:
                                cur.execute("SET SESSION max_execution_time = %s", (timeout_ms,))
                            except Exception:
                                pass
                            try:
                                cur.execute(
                                    "SET SESSION max_statement_time = %s",
                                    (timeout_ms / 1000.0,),
                                )
                            except Exception:
                                pass
                            cur.execute(query_text)
                            query_ms = (perf_counter() - start) * 1000

                            if cur.description:
                                result_columns = [str(column[0]) for column in cur.description]
                                raw_rows = cur.fetchmany(MAX_SQL_PREVIEW_ROWS + 1)
                                limited_rows = raw_rows[:MAX_SQL_PREVIEW_ROWS]
                                result_rows = [
                                    [format_cell(row.get(column)) for column in result_columns]
                                    for row in limited_rows
                                ]
                                result_count = len(result_rows)
                                result_truncated = len(raw_rows) > MAX_SQL_PREVIEW_ROWS
                                push_sql_history(db_name, query_text)
                                sql_history = [item for item in get_sql_history() if item["db"] == db_name]

                                if export_format in {"csv", "json"}:
                                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                    if export_format == "csv":
                                        output = io.StringIO()
                                        writer = csv.writer(output)
                                        writer.writerow(result_columns)
                                        for row in limited_rows:
                                            writer.writerow(
                                                [
                                                    format_export_value(row.get(column))
                                                    for column in result_columns
                                                ]
                                            )
                                        response = make_response(output.getvalue())
                                        filename = f"sql_result_{db_name}_{timestamp}.csv"
                                        response.headers["Content-Type"] = (
                                            "text/csv; charset=utf-8"
                                        )
                                    else:
                                        export_payload = [
                                            {
                                                column: json_export_value(row.get(column))
                                                for column in result_columns
                                            }
                                            for row in limited_rows
                                        ]
                                        response = make_response(
                                            json.dumps(
                                                export_payload,
                                                ensure_ascii=False,
                                                indent=2,
                                            )
                                        )
                                        filename = f"sql_result_{db_name}_{timestamp}.json"
                                        response.headers["Content-Type"] = (
                                            "application/json; charset=utf-8"
                                        )

                                    response.headers["Content-Disposition"] = (
                                        f'attachment; filename="{filename}"'
                                    )
                                    if result_truncated:
                                        response.headers["X-Result-Truncated"] = "1"
                                    return response

                                flash("SQL-запрос выполнен.", "success")
                            else:
                                if export_format in {"csv", "json"}:
                                    flash(
                                        "Экспорт доступен только для запросов, возвращающих таблицу данных.",
                                        "error",
                                    )
                                affected_rows = max(cur.rowcount, 0)
                                push_sql_history(db_name, query_text)
                                sql_history = [item for item in get_sql_history() if item["db"] == db_name]
                                if export_format not in {"csv", "json"}:
                                    flash(
                                        f"SQL-запрос выполнен. Затронуто строк: {affected_rows}.",
                                        "success",
                                    )
                    except Exception as exc:
                        flash(f"Ошибка SQL: {exc}", "error")
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        return render_error_page(
            title="Ошибка выполнения SQL",
            message=str(exc),
            status_code=500,
            back_url=url_for("database_tables", db_name=db_name),
        )

    return render_template(
        "sql_console.html",
        databases=db_items if "db_items" in locals() else [],
        tables=table_items if "table_items" in locals() else [],
        current_db=db_name,
        sql_query=query_text,
        result_columns=result_columns,
        result_rows=result_rows,
        result_count=result_count,
        affected_rows=affected_rows,
        query_ms=round(query_ms, 2) if query_ms is not None else None,
        result_truncated=result_truncated,
        sql_history=sql_history,
        sql_snippets=sql_snippets,
        timeout_ms=timeout_ms,
        current_table=current_table,
        return_page=return_page,
        return_limit=return_limit,
        tables_q=tables_q,
    )


@app.route("/databases/<db_name>/sql/snippets/add", methods=["POST"])
def add_sql_snippet_route(db_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name):
        abort(404)

    snippet_name = request.form.get("snippet_name", "").strip()
    snippet_query = request.form.get("snippet_query", "").strip()
    current_table = request.form.get("table", "").strip()
    if current_table and not safe_database_name(current_table):
        current_table = ""
    return_page, return_limit, tables_q = parse_table_return_state(request.form)
    timeout_ms = coerce_int_in_range(
        request.form.get("timeout_ms", session.get("sql_timeout_ms", DEFAULT_SQL_TIMEOUT_MS)),
        DEFAULT_SQL_TIMEOUT_MS,
        MIN_SQL_TIMEOUT_MS,
        MAX_SQL_TIMEOUT_MS,
    )

    if not snippet_query:
        flash("Нельзя сохранить пустой SQL-сниппет.", "error")
        return redirect(
            url_for(
                "sql_console",
                db_name=db_name,
                table=current_table,
                return_page=return_page,
                return_limit=return_limit,
                tables_q=tables_q,
                timeout_ms=timeout_ms,
            )
        )

    snippet_id = save_sql_snippet(db_name, snippet_name, snippet_query)
    flash("SQL-сниппет сохранен в Favorites.", "success")
    return redirect(
        url_for(
            "sql_console",
            db_name=db_name,
            snippet_id=snippet_id,
            table=current_table,
            return_page=return_page,
            return_limit=return_limit,
            tables_q=tables_q,
            timeout_ms=timeout_ms,
        )
    )


@app.route("/databases/<db_name>/sql/snippets/<snippet_id>/delete", methods=["POST"])
def delete_sql_snippet_route(db_name: str, snippet_id: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name):
        abort(404)

    current_table = request.form.get("table", "").strip()
    if current_table and not safe_database_name(current_table):
        current_table = ""
    return_page, return_limit, tables_q = parse_table_return_state(request.form)
    timeout_ms = coerce_int_in_range(
        request.form.get("timeout_ms", session.get("sql_timeout_ms", DEFAULT_SQL_TIMEOUT_MS)),
        DEFAULT_SQL_TIMEOUT_MS,
        MIN_SQL_TIMEOUT_MS,
        MAX_SQL_TIMEOUT_MS,
    )

    if delete_sql_snippet(db_name, snippet_id):
        flash("Сниппет удален из Favorites.", "success")
    else:
        flash("Сниппет не найден или уже удален.", "error")

    return redirect(
        url_for(
            "sql_console",
            db_name=db_name,
            table=current_table,
            return_page=return_page,
            return_limit=return_limit,
            tables_q=tables_q,
            timeout_ms=timeout_ms,
        )
    )


@app.route("/databases/<db_name>/tables/<table_name>")
def table_view(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    limit_raw = request.args.get("limit", "50")
    limit = min(500, coerce_positive_int(limit_raw, 50))
    page_raw = request.args.get("page", "1")
    page = coerce_positive_int(page_raw, 1)
    offset = (page - 1) * limit
    tables_q = request.args.get("tables_q", "").strip()
    sort_col = request.args.get("sort_col", "").strip()
    sort_dir = request.args.get("sort_dir", "asc").strip().lower()
    sort_dir = "desc" if sort_dir == "desc" else "asc"
    filter_logic = request.args.get("filter_logic", "AND").strip().upper()
    filter_logic = "OR" if filter_logic == "OR" else "AND"

    filter_columns = request.args.getlist("filter_column")
    filter_ops = request.args.getlist("filter_op")
    filter_values = request.args.getlist("filter_value")
    filter_values_to = request.args.getlist("filter_value_to")
    max_filter_rows = max(
        len(filter_columns), len(filter_ops), len(filter_values), len(filter_values_to), 1
    )
    filter_rows: list[dict[str, str]] = []
    valid_filters: list[dict[str, str]] = []
    filter_operators = {
        "contains",
        "exact",
        "gt",
        "lt",
        "between",
        "is_null",
        "not_null",
    }
    filter_operator_items = [
        ("contains", "Contains"),
        ("exact", "Exact"),
        ("gt", ">"),
        ("lt", "<"),
        ("between", "Between"),
        ("is_null", "IS NULL"),
        ("not_null", "IS NOT NULL"),
    ]

    try:
        conn = mysql_connect()
        try:
            if not database_exists(conn, db_name):
                abort(404)
            if not table_exists(conn, db_name, table_name):
                flash(f"Таблица '{table_name}' не найдена.", "error")
                return redirect(url_for("database_tables", db_name=db_name, q=tables_q))

            db_items = fetch_databases(conn)
            table_items = fetch_tables(conn, db_name)
            sidebar_tables = (
                [item for item in table_items if tables_q.lower() in item.name.lower()]
                if tables_q
                else table_items
            )
            if tables_q and not any(item.name == table_name for item in sidebar_tables):
                current_table_meta = next(
                    (item for item in table_items if item.name == table_name),
                    None,
                )
                if current_table_meta:
                    sidebar_tables = [current_table_meta, *sidebar_tables]

            columns_meta = fetch_columns_meta(conn, db_name, table_name)
            primary_key_columns = [column.name for column in columns_meta if column.is_primary]
            column_names = [column.name for column in columns_meta]
            column_name_set = set(column_names)
            inline_edit_columns = [
                column
                for column in columns_meta
                if (not column.is_primary and "GENERATED" not in column.extra.upper())
            ]

            if sort_col not in column_name_set:
                sort_col = ""

            for index in range(max_filter_rows):
                row_col = filter_columns[index].strip() if index < len(filter_columns) else ""
                row_op = filter_ops[index].strip() if index < len(filter_ops) else "contains"
                row_val = filter_values[index].strip() if index < len(filter_values) else ""
                row_val_to = (
                    filter_values_to[index].strip()
                    if index < len(filter_values_to)
                    else ""
                )
                if row_op not in filter_operators:
                    row_op = "contains"

                filter_rows.append(
                    {
                        "column": row_col,
                        "op": row_op,
                        "value": row_val,
                        "value_to": row_val_to,
                    }
                )

                has_row_input = bool(row_col or row_val or row_val_to)
                if not has_row_input:
                    continue
                if row_col not in column_name_set:
                    continue
                if row_op in {"is_null", "not_null"}:
                    valid_filters.append(filter_rows[-1])
                elif row_op == "between":
                    if row_val and row_val_to:
                        valid_filters.append(filter_rows[-1])
                elif row_val:
                    valid_filters.append(filter_rows[-1])

            where_clauses: list[str] = []
            where_params: list[Any] = []
            for item in valid_filters:
                col_sql = quote_identifier(item["column"])
                op = item["op"]
                val = item["value"]
                val_to = item["value_to"]

                if op == "contains":
                    where_clauses.append(f"CAST({col_sql} AS CHAR) LIKE %s")
                    where_params.append(f"%{val}%")
                elif op == "exact":
                    if val.upper() == "NULL":
                        where_clauses.append(f"{col_sql} IS NULL")
                    else:
                        where_clauses.append(f"{col_sql} = %s")
                        where_params.append(val)
                elif op == "gt":
                    where_clauses.append(f"{col_sql} > %s")
                    where_params.append(val)
                elif op == "lt":
                    where_clauses.append(f"{col_sql} < %s")
                    where_params.append(val)
                elif op == "between":
                    where_clauses.append(f"{col_sql} BETWEEN %s AND %s")
                    where_params.extend([val, val_to])
                elif op == "is_null":
                    where_clauses.append(f"{col_sql} IS NULL")
                elif op == "not_null":
                    where_clauses.append(f"{col_sql} IS NOT NULL")

            where_sql = ""
            if where_clauses:
                where_sql = f" WHERE {f' {filter_logic} '.join(where_clauses)}"

            order_sql = ""
            if sort_col:
                order_sql = (
                    f" ORDER BY {quote_identifier(sort_col)} "
                    f"{'DESC' if sort_dir == 'desc' else 'ASC'}"
                )

            start = perf_counter()
            with conn.cursor() as cur:
                sql = (
                    f"SELECT * FROM {quote_identifier(db_name)}.{quote_identifier(table_name)}"
                    f"{where_sql}{order_sql} LIMIT %s OFFSET %s"
                )
                cur.execute(sql, [*where_params, limit + 1, offset])
                fetched_rows = cur.fetchall()
                query_ms = (perf_counter() - start) * 1000
                columns: list[str] = (
                    [str(column[0]) for column in cur.description] if cur.description else []
                )

            has_next_page = len(fetched_rows) > limit
            raw_rows = fetched_rows[:limit]

            row_items: list[dict[str, Any]] = []
            for raw_row in raw_rows:
                pk_map = {
                    column: format_form_value(raw_row.get(column))
                    for column in primary_key_columns
                }
                inline_values = {
                    column.name: format_input_value(column, raw_row.get(column.name))
                    for column in inline_edit_columns
                }
                inline_nulls = {
                    column.name: raw_row.get(column.name) is None
                    for column in inline_edit_columns
                }
                has_pk_values = bool(primary_key_columns) and all(
                    raw_row.get(column) is not None for column in primary_key_columns
                )
                row_items.append(
                    {
                        "cells": [format_cell(raw_row.get(column)) for column in columns],
                        "pk_items": [
                            {"name": column, "value": pk_map[column]}
                            for column in primary_key_columns
                        ],
                        "pk_token": (
                            json.dumps(pk_map, separators=(",", ":"), ensure_ascii=False)
                            if has_pk_values
                            else ""
                        ),
                        "inline_values": inline_values,
                        "inline_nulls": inline_nulls,
                        "has_pk_values": has_pk_values,
                    }
                )

            estimated_rows = next(
                (t.rows_count for t in table_items if t.name == table_name),
                len(raw_rows),
            )
            rows_from = offset + 1 if raw_rows else 0
            rows_to = offset + len(raw_rows)

            query_args_multi = {key: request.args.getlist(key) for key in request.args.keys()}

            def build_query(overrides: dict[str, Any]) -> str:
                params = {key: list(values) for key, values in query_args_multi.items()}
                for key, value in overrides.items():
                    if value is None:
                        params.pop(key, None)
                    else:
                        params[key] = [str(value)]
                return urlencode(params, doseq=True)

            prev_page_query = build_query({"page": max(page - 1, 1)})
            next_page_query = build_query({"page": page + 1})
            limit_50_query = build_query({"limit": 50, "page": 1})
            limit_100_query = build_query({"limit": 100, "page": 1})
            limit_500_query = build_query({"limit": 500, "page": 1})
            current_query = normalize_query_string(build_query({}))

            sort_queries: dict[str, str] = {}
            for column in columns:
                next_dir = (
                    "desc"
                    if sort_col == column and sort_dir == "asc"
                    else "asc"
                )
                sort_queries[column] = build_query(
                    {
                        "sort_col": column,
                        "sort_dir": next_dir,
                        "page": 1,
                    }
                )
            clear_sort_query = build_query(
                {
                    "sort_col": None,
                    "sort_dir": None,
                    "page": 1,
                }
            )

            query_args_without_page: list[tuple[str, str]] = []
            for key, values in query_args_multi.items():
                if key == "page":
                    continue
                for value in values:
                    query_args_without_page.append((key, value))

            bulk_update_columns = [
                column for column in columns_meta if "GENERATED" not in column.extra.upper()
            ]

            query_preview = (
                f"SELECT * FROM `{table_name}`"
                f"{' WHERE ...' if where_clauses else ''}"
                f"{f' ORDER BY `{sort_col}` {sort_dir.upper()}' if sort_col else ''} "
                f"LIMIT {limit} OFFSET {offset};"
            )
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        return render_error_page(
            title="Ошибка чтения данных таблицы",
            message=str(exc),
            status_code=500,
            back_url=url_for("database_tables", db_name=db_name),
        )

    return render_template(
        "table_view.html",
        databases=db_items,
        current_db=db_name,
        tables=table_items,
        sidebar_tables=sidebar_tables,
        current_table=table_name,
        columns=columns,
        row_items=row_items,
        limit=limit,
        page=page,
        has_prev_page=page > 1,
        has_next_page=has_next_page,
        prev_page=page - 1,
        next_page=page + 1,
        offset=offset,
        rows_from=rows_from,
        rows_to=rows_to,
        tables_q=tables_q,
        query_ms=round(query_ms, 2),
        estimated_rows=estimated_rows,
        has_primary_key=bool(primary_key_columns),
        sort_col=sort_col,
        sort_dir=sort_dir,
        sort_queries=sort_queries,
        clear_sort_query=clear_sort_query,
        filter_logic=filter_logic,
        filter_rows=filter_rows,
        column_names=column_names,
        filter_operator_items=filter_operator_items,
        prev_page_query=prev_page_query,
        next_page_query=next_page_query,
        limit_50_query=limit_50_query,
        limit_100_query=limit_100_query,
        limit_500_query=limit_500_query,
        current_query=current_query,
        query_args_without_page=query_args_without_page,
        bulk_update_columns=bulk_update_columns,
        inline_edit_columns=inline_edit_columns,
        query_preview=query_preview,
    )


@app.route("/databases/<db_name>/tables/<table_name>/structure")
def table_structure(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    tables_q = request.args.get("tables_q", "").strip()
    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                flash(f"Таблица '{table_name}' не найдена.", "error")
                return redirect(url_for("database_tables", db_name=db_name, q=tables_q))

            db_items = fetch_databases(conn)
            table_items = fetch_tables(conn, db_name)
            columns_meta = fetch_columns_meta(conn, db_name, table_name)
            table_indexes = fetch_table_indexes(conn, db_name, table_name)
            foreign_keys = fetch_table_foreign_keys(conn, db_name, table_name)
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        return render_error_page(
            title="Ошибка чтения структуры таблицы",
            message=str(exc),
            status_code=500,
            back_url=url_for("table_view", db_name=db_name, table_name=table_name, tables_q=tables_q),
        )

    return render_template(
        "table_structure.html",
        databases=db_items,
        tables=table_items,
        current_db=db_name,
        current_table=table_name,
        columns_meta=columns_meta,
        table_indexes=table_indexes,
        foreign_keys=foreign_keys,
        table_names=[table.name for table in table_items],
        tables_q=tables_q,
        allowed_column_types=sorted(CREATE_TABLE_ALLOWED_TYPES),
    )


@app.route(
    "/databases/<db_name>/tables/<table_name>/structure/columns/<column_name>/move",
    methods=["POST"],
)
def move_column(db_name: str, table_name: str, column_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    tables_q = request.form.get("tables_q", "").strip()
    direction = request.form.get("direction", "").strip().lower()
    if direction not in {"up", "down"}:
        flash("Некорректное направление перемещения.", "error")
        return redirect(
            url_for(
                "table_structure",
                db_name=db_name,
                table_name=table_name,
                tables_q=tables_q,
            )
        )

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            columns_meta = fetch_columns_meta(conn, db_name, table_name)
            names = [column.name for column in columns_meta]
            if column_name not in names:
                flash("Колонка для перемещения не найдена.", "error")
                return redirect(
                    url_for(
                        "table_structure",
                        db_name=db_name,
                        table_name=table_name,
                        tables_q=tables_q,
                    )
                )

            index = names.index(column_name)
            if direction == "up" and index == 0:
                flash("Колонка уже находится на первом месте.", "error")
                return redirect(
                    url_for(
                        "table_structure",
                        db_name=db_name,
                        table_name=table_name,
                        tables_q=tables_q,
                    )
                )
            if direction == "down" and index == len(names) - 1:
                flash("Колонка уже находится на последнем месте.", "error")
                return redirect(
                    url_for(
                        "table_structure",
                        db_name=db_name,
                        table_name=table_name,
                        tables_q=tables_q,
                    )
                )

            column_meta = next(column for column in columns_meta if column.name == column_name)
            definition_sql, params, definition_error = build_existing_column_definition_sql(
                column_meta
            )
            if definition_error:
                flash(definition_error, "error")
                return redirect(
                    url_for(
                        "table_structure",
                        db_name=db_name,
                        table_name=table_name,
                        tables_q=tables_q,
                    )
                )

            if direction == "up":
                if index == 1:
                    position_sql = " FIRST"
                else:
                    position_sql = f" AFTER {quote_identifier(names[index - 2])}"
            else:
                position_sql = f" AFTER {quote_identifier(names[index + 1])}"

            with conn.cursor() as cur:
                cur.execute(
                    f"ALTER TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                    f"MODIFY COLUMN {quote_identifier(column_name)} {definition_sql}{position_sql}",
                    params,
                )
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Не удалось изменить порядок колонок: {exc}", "error")
        return redirect(
            url_for(
                "table_structure",
                db_name=db_name,
                table_name=table_name,
                tables_q=tables_q,
            )
        )

    flash(f"Порядок колонки '{column_name}' обновлен.", "success")
    return redirect(
        url_for(
            "table_structure",
            db_name=db_name,
            table_name=table_name,
            tables_q=tables_q,
        )
    )


@app.route(
    "/databases/<db_name>/tables/<table_name>/structure/columns/add",
    methods=["POST"],
)
def add_column(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    tables_q = request.form.get("tables_q", "").strip()
    column_name = request.form.get("column_name", "").strip()
    column_type = request.form.get("column_type", "").strip().upper()
    column_nullable = request.form.get("column_nullable", "0").strip() == "1"
    default_mode = request.form.get("default_mode", "none").strip()
    default_value = request.form.get("default_value", "")
    column_after = request.form.get("column_after", "").strip()

    if not safe_simple_identifier(column_name):
        flash("Некорректное имя новой колонки.", "error")
        return redirect(
            url_for(
                "table_structure",
                db_name=db_name,
                table_name=table_name,
                tables_q=tables_q,
            )
        )
    if column_type not in CREATE_TABLE_ALLOWED_TYPES:
        flash("Недопустимый тип колонки.", "error")
        return redirect(
            url_for(
                "table_structure",
                db_name=db_name,
                table_name=table_name,
                tables_q=tables_q,
            )
        )

    definition_sql, definition_params, definition_errors = build_column_definition_sql(
        column_type=column_type,
        nullable=column_nullable,
        default_mode=default_mode,
        default_value=default_value,
    )
    if definition_errors:
        flash(definition_errors["default"], "error")
        return redirect(
            url_for(
                "table_structure",
                db_name=db_name,
                table_name=table_name,
                tables_q=tables_q,
            )
        )

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            columns_meta = fetch_columns_meta(conn, db_name, table_name)
            existing_names = {column.name for column in columns_meta}

            if column_name in existing_names:
                flash("Колонка с таким именем уже существует.", "error")
                return redirect(
                    url_for(
                        "table_structure",
                        db_name=db_name,
                        table_name=table_name,
                        tables_q=tables_q,
                    )
                )

            position_sql = ""
            if column_after:
                if column_after not in existing_names:
                    flash("Колонка AFTER не найдена.", "error")
                    return redirect(
                        url_for(
                            "table_structure",
                            db_name=db_name,
                            table_name=table_name,
                            tables_q=tables_q,
                        )
                    )
                position_sql = f" AFTER {quote_identifier(column_after)}"

            sql = (
                f"ALTER TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                f"ADD COLUMN {quote_identifier(column_name)} {definition_sql}{position_sql}"
            )
            with conn.cursor() as cur:
                cur.execute(sql, definition_params)
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Не удалось добавить колонку: {exc}", "error")
        return redirect(
            url_for(
                "table_structure",
                db_name=db_name,
                table_name=table_name,
                tables_q=tables_q,
            )
        )

    flash(f"Колонка '{column_name}' добавлена.", "success")
    return redirect(
        url_for(
            "table_structure",
            db_name=db_name,
            table_name=table_name,
            tables_q=tables_q,
        )
    )


@app.route(
    "/databases/<db_name>/tables/<table_name>/structure/indexes/add",
    methods=["POST"],
)
def add_index(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    tables_q = request.form.get("tables_q", "").strip()
    index_kind = request.form.get("index_kind", "index").strip().lower()
    index_name = request.form.get("index_name", "").strip()
    columns_raw = request.form.get("index_columns", "").strip()
    index_columns = [item.strip() for item in columns_raw.split(",") if item.strip()]

    if index_kind not in {"primary", "unique", "index", "fulltext"}:
        flash("Некорректный тип индекса.", "error")
        return redirect(
            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
        )
    if not index_columns:
        flash("Укажи хотя бы одну колонку для индекса.", "error")
        return redirect(
            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
        )
    for column in index_columns:
        if not safe_simple_identifier(column):
            flash("Некорректное имя колонки в индексе.", "error")
            return redirect(
                url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
            )

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            columns_meta = fetch_columns_meta(conn, db_name, table_name)
            existing_columns = {column.name for column in columns_meta}
            if any(column not in existing_columns for column in index_columns):
                flash("Одна или несколько колонок для индекса не найдены.", "error")
                return redirect(
                    url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
                )

            existing_indexes = fetch_table_indexes(conn, db_name, table_name)
            existing_index_names = {item["name"] for item in existing_indexes}

            columns_sql = ", ".join(quote_identifier(column) for column in index_columns)
            with conn.cursor() as cur:
                if index_kind == "primary":
                    if "PRIMARY" in existing_index_names:
                        flash("PRIMARY KEY уже существует.", "error")
                        return redirect(
                            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
                        )
                    cur.execute(
                        f"ALTER TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                        f"ADD PRIMARY KEY ({columns_sql})"
                    )
                    created_name = "PRIMARY"
                else:
                    if not index_name:
                        index_name = f"idx_{'_'.join(index_columns)}"
                    if not safe_simple_identifier(index_name):
                        flash("Некорректное имя индекса.", "error")
                        return redirect(
                            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
                        )
                    if index_name in existing_index_names:
                        flash("Индекс с таким именем уже существует.", "error")
                        return redirect(
                            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
                        )

                    if index_kind == "unique":
                        statement = "ADD UNIQUE INDEX"
                    elif index_kind == "fulltext":
                        statement = "ADD FULLTEXT INDEX"
                    else:
                        statement = "ADD INDEX"
                    cur.execute(
                        f"ALTER TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                        f"{statement} {quote_identifier(index_name)} ({columns_sql})"
                    )
                    created_name = index_name
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Не удалось создать индекс: {exc}", "error")
        return redirect(
            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
        )

    flash(f"Индекс '{created_name}' создан.", "success")
    return redirect(
        url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
    )


@app.route(
    "/databases/<db_name>/tables/<table_name>/structure/indexes/<index_name>/delete",
    methods=["POST"],
)
def delete_index(db_name: str, table_name: str, index_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    tables_q = request.form.get("tables_q", "").strip()
    confirm_name = request.form.get("confirm_name", "").strip()
    if confirm_name != index_name:
        flash("Подтверждение удаления индекса не совпало.", "error")
        return redirect(
            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
        )

    if index_name != "PRIMARY" and not safe_simple_identifier(index_name):
        flash("Некорректное имя индекса.", "error")
        return redirect(
            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
        )

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            existing_indexes = fetch_table_indexes(conn, db_name, table_name)
            existing_names = {item["name"] for item in existing_indexes}
            if index_name not in existing_names:
                flash("Индекс не найден.", "error")
                return redirect(
                    url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
                )

            with conn.cursor() as cur:
                if index_name == "PRIMARY":
                    cur.execute(
                        f"ALTER TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                        "DROP PRIMARY KEY"
                    )
                else:
                    cur.execute(
                        f"ALTER TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                        f"DROP INDEX {quote_identifier(index_name)}"
                    )
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Не удалось удалить индекс: {exc}", "error")
        return redirect(
            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
        )

    flash(f"Индекс '{index_name}' удален.", "success")
    return redirect(
        url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
    )


@app.route(
    "/databases/<db_name>/tables/<table_name>/structure/foreign-keys/add",
    methods=["POST"],
)
def add_foreign_key(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    tables_q = request.form.get("tables_q", "").strip()
    fk_name = request.form.get("fk_name", "").strip()
    column_name = request.form.get("column_name", "").strip()
    referenced_table = request.form.get("referenced_table", "").strip()
    referenced_column = request.form.get("referenced_column", "").strip()
    on_update = request.form.get("on_update", "RESTRICT").strip().upper()
    on_delete = request.form.get("on_delete", "RESTRICT").strip().upper()

    if not fk_name:
        fk_name = f"fk_{table_name}_{column_name}"
    if not all(
        safe_simple_identifier(value)
        for value in (fk_name, column_name, referenced_table, referenced_column)
    ):
        flash("Некорректные параметры FOREIGN KEY.", "error")
        return redirect(
            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
        )
    if on_update not in FK_RULES or on_delete not in FK_RULES:
        flash("Некорректные правила ON UPDATE/ON DELETE.", "error")
        return redirect(
            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
        )

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)
            if not table_exists(conn, db_name, referenced_table):
                flash("Таблица-назначение для FOREIGN KEY не найдена.", "error")
                return redirect(
                    url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
                )

            local_columns = fetch_columns_meta(conn, db_name, table_name)
            referenced_columns = fetch_columns_meta(conn, db_name, referenced_table)
            local_names = {column.name for column in local_columns}
            referenced_names = {column.name for column in referenced_columns}

            if column_name not in local_names or referenced_column not in referenced_names:
                flash("Колонки для FOREIGN KEY не найдены.", "error")
                return redirect(
                    url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
                )

            if on_delete == "SET NULL":
                local_column_meta = next(column for column in local_columns if column.name == column_name)
                if not local_column_meta.is_nullable:
                    flash("ON DELETE SET NULL требует nullable колонку.", "error")
                    return redirect(
                        url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
                    )

            existing_fk = fetch_table_foreign_keys(conn, db_name, table_name)
            if fk_name in {item["name"] for item in existing_fk}:
                flash("FOREIGN KEY с таким именем уже существует.", "error")
                return redirect(
                    url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
                )

            with conn.cursor() as cur:
                cur.execute(
                    f"ALTER TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                    f"ADD CONSTRAINT {quote_identifier(fk_name)} "
                    f"FOREIGN KEY ({quote_identifier(column_name)}) "
                    f"REFERENCES {quote_identifier(db_name)}.{quote_identifier(referenced_table)} "
                    f"({quote_identifier(referenced_column)}) "
                    f"ON UPDATE {on_update} ON DELETE {on_delete}"
                )
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Не удалось создать FOREIGN KEY: {exc}", "error")
        return redirect(
            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
        )

    flash(f"FOREIGN KEY '{fk_name}' создан.", "success")
    return redirect(
        url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
    )


@app.route(
    "/databases/<db_name>/tables/<table_name>/structure/foreign-keys/<fk_name>/delete",
    methods=["POST"],
)
def delete_foreign_key(db_name: str, table_name: str, fk_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)
    if not safe_simple_identifier(fk_name):
        flash("Некорректное имя FOREIGN KEY.", "error")
        return redirect(url_for("table_structure", db_name=db_name, table_name=table_name))

    tables_q = request.form.get("tables_q", "").strip()
    confirm_name = request.form.get("confirm_name", "").strip()
    if confirm_name != fk_name:
        flash("Подтверждение удаления FOREIGN KEY не совпало.", "error")
        return redirect(
            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
        )

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            existing_fk = fetch_table_foreign_keys(conn, db_name, table_name)
            if fk_name not in {item["name"] for item in existing_fk}:
                flash("FOREIGN KEY не найден.", "error")
                return redirect(
                    url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
                )

            with conn.cursor() as cur:
                cur.execute(
                    f"ALTER TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                    f"DROP FOREIGN KEY {quote_identifier(fk_name)}"
                )
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Не удалось удалить FOREIGN KEY: {exc}", "error")
        return redirect(
            url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
        )

    flash(f"FOREIGN KEY '{fk_name}' удален.", "success")
    return redirect(
        url_for("table_structure", db_name=db_name, table_name=table_name, tables_q=tables_q)
    )


@app.route(
    "/databases/<db_name>/tables/<table_name>/structure/columns/<column_name>/edit",
    methods=["POST"],
)
def edit_column(db_name: str, table_name: str, column_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    tables_q = request.form.get("tables_q", "").strip()
    new_name = request.form.get("new_name", "").strip()
    new_type = request.form.get("new_type", "").strip().upper()
    new_nullable = request.form.get("new_nullable", "0").strip() == "1"
    default_mode = request.form.get("default_mode", "none").strip()
    default_value = request.form.get("default_value", "")

    if not safe_simple_identifier(new_name):
        flash("Некорректное новое имя колонки.", "error")
        return redirect(
            url_for(
                "table_structure",
                db_name=db_name,
                table_name=table_name,
                tables_q=tables_q,
            )
        )
    if new_type not in CREATE_TABLE_ALLOWED_TYPES:
        flash("Недопустимый тип колонки.", "error")
        return redirect(
            url_for(
                "table_structure",
                db_name=db_name,
                table_name=table_name,
                tables_q=tables_q,
            )
        )

    definition_sql, definition_params, definition_errors = build_column_definition_sql(
        column_type=new_type,
        nullable=new_nullable,
        default_mode=default_mode,
        default_value=default_value,
    )
    if definition_errors:
        flash(definition_errors["default"], "error")
        return redirect(
            url_for(
                "table_structure",
                db_name=db_name,
                table_name=table_name,
                tables_q=tables_q,
            )
        )

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            columns_meta = fetch_columns_meta(conn, db_name, table_name)
            target = next((col for col in columns_meta if col.name == column_name), None)
            if not target:
                flash("Колонка для изменения не найдена.", "error")
                return redirect(
                    url_for(
                        "table_structure",
                        db_name=db_name,
                        table_name=table_name,
                        tables_q=tables_q,
                    )
                )

            extra_upper = target.extra.upper()
            if "AUTO_INCREMENT" in extra_upper or "GENERATED" in extra_upper:
                flash("Колонку с AUTO_INCREMENT/GENERATED пока нельзя изменить через UI.", "error")
                return redirect(
                    url_for(
                        "table_structure",
                        db_name=db_name,
                        table_name=table_name,
                        tables_q=tables_q,
                    )
                )

            existing_names = {column.name for column in columns_meta}
            if new_name != column_name and new_name in existing_names:
                flash("Колонка с таким именем уже существует.", "error")
                return redirect(
                    url_for(
                        "table_structure",
                        db_name=db_name,
                        table_name=table_name,
                        tables_q=tables_q,
                    )
                )

            sql = (
                f"ALTER TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                f"CHANGE COLUMN {quote_identifier(column_name)} {quote_identifier(new_name)} "
                f"{definition_sql}"
            )
            with conn.cursor() as cur:
                cur.execute(sql, definition_params)
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Не удалось изменить колонку: {exc}", "error")
        return redirect(
            url_for(
                "table_structure",
                db_name=db_name,
                table_name=table_name,
                tables_q=tables_q,
            )
        )

    flash(f"Колонка '{column_name}' обновлена.", "success")
    return redirect(
        url_for(
            "table_structure",
            db_name=db_name,
            table_name=table_name,
            tables_q=tables_q,
        )
    )


@app.route(
    "/databases/<db_name>/tables/<table_name>/structure/columns/<column_name>/delete",
    methods=["POST"],
)
def delete_column(db_name: str, table_name: str, column_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    tables_q = request.form.get("tables_q", "").strip()
    confirm_name = request.form.get("confirm_name", "").strip()
    if confirm_name != column_name:
        flash("Подтверждение удаления колонки не совпало.", "error")
        return redirect(
            url_for(
                "table_structure",
                db_name=db_name,
                table_name=table_name,
                tables_q=tables_q,
            )
        )

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            columns_meta = fetch_columns_meta(conn, db_name, table_name)
            target = next((col for col in columns_meta if col.name == column_name), None)
            if not target:
                flash("Колонка для удаления не найдена.", "error")
                return redirect(
                    url_for(
                        "table_structure",
                        db_name=db_name,
                        table_name=table_name,
                        tables_q=tables_q,
                    )
                )
            if len(columns_meta) <= 1:
                flash("Нельзя удалить последнюю колонку таблицы.", "error")
                return redirect(
                    url_for(
                        "table_structure",
                        db_name=db_name,
                        table_name=table_name,
                        tables_q=tables_q,
                    )
                )
            if target.is_primary:
                flash("Удаление PRIMARY KEY колонки через UI пока не поддерживается.", "error")
                return redirect(
                    url_for(
                        "table_structure",
                        db_name=db_name,
                        table_name=table_name,
                        tables_q=tables_q,
                    )
                )
            if "GENERATED" in target.extra.upper():
                flash("Удаление GENERATED колонки через UI пока не поддерживается.", "error")
                return redirect(
                    url_for(
                        "table_structure",
                        db_name=db_name,
                        table_name=table_name,
                        tables_q=tables_q,
                    )
                )

            with conn.cursor() as cur:
                cur.execute(
                    f"ALTER TABLE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                    f"DROP COLUMN {quote_identifier(column_name)}"
                )
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Не удалось удалить колонку: {exc}", "error")
        return redirect(
            url_for(
                "table_structure",
                db_name=db_name,
                table_name=table_name,
                tables_q=tables_q,
            )
        )

    flash(f"Колонка '{column_name}' удалена.", "success")
    return redirect(
        url_for(
            "table_structure",
            db_name=db_name,
            table_name=table_name,
            tables_q=tables_q,
        )
    )


@app.route("/databases/<db_name>/tables/<table_name>/rows/new", methods=["GET", "POST"])
def create_row(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    source_values = request.form if request.method == "POST" else request.args
    return_page, return_limit, return_tables_q = parse_table_return_state(source_values)
    return_query = normalize_query_string(str(source_values.get("return_query", "")))
    return_url = build_table_view_return_url(
        db_name=db_name,
        table_name=table_name,
        return_query=return_query,
        return_page=return_page,
        return_limit=return_limit,
        return_tables_q=return_tables_q,
    )
    field_errors: dict[str, str] = {}

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            db_items = fetch_databases(conn)
            table_items = fetch_tables(conn, db_name)
            columns_meta = fetch_columns_meta(conn, db_name, table_name)
            editable_columns = [
                column
                for column in columns_meta
                if "GENERATED" not in column.extra.upper()
            ]

            form_values = {
                column.name: request.form.get(column.name, "")
                for column in editable_columns
            }

            if request.method == "POST":
                insert_columns: list[str] = []
                insert_values: list[Any] = []

                for column in editable_columns:
                    value = normalize_form_value(
                        form_values[column.name], column, for_insert=True
                    )
                    if value is SKIP_VALUE:
                        continue
                    if value == "" and not column.is_nullable:
                        field_errors[column.name] = "Поле обязательно для заполнения."
                        continue
                    insert_columns.append(column.name)
                    insert_values.append(value)

                if field_errors:
                    flash("Исправь ошибки в форме.", "error")
                else:
                    with conn.cursor() as cur:
                        if insert_columns:
                            columns_sql = ", ".join(
                                quote_identifier(column) for column in insert_columns
                            )
                            placeholders = ", ".join(["%s"] * len(insert_values))
                            cur.execute(
                                f"INSERT INTO {quote_identifier(db_name)}."
                                f"{quote_identifier(table_name)} ({columns_sql}) "
                                f"VALUES ({placeholders})",
                                insert_values,
                            )
                        else:
                            cur.execute(
                                f"INSERT INTO {quote_identifier(db_name)}."
                                f"{quote_identifier(table_name)} () VALUES ()"
                            )

                    flash("Строка успешно добавлена.", "success")
                    return redirect(return_url)
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Ошибка добавления строки: {exc}", "error")

    return render_template(
        "row_form.html",
        databases=db_items if "db_items" in locals() else [],
        tables=table_items if "table_items" in locals() else [],
        current_db=db_name,
        current_table=table_name,
        mode="create",
        editable_columns=editable_columns if "editable_columns" in locals() else [],
        form_values=form_values if "form_values" in locals() else {},
        field_errors=field_errors,
        pk_filters=[],
        return_page=return_page,
        return_limit=return_limit,
        return_tables_q=return_tables_q,
        return_query=return_query,
    )


@app.route("/databases/<db_name>/tables/<table_name>/rows/edit", methods=["GET", "POST"])
def edit_row(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    source_values = request.form if request.method == "POST" else request.args
    return_page, return_limit, return_tables_q = parse_table_return_state(source_values)
    return_query = normalize_query_string(str(source_values.get("return_query", "")))
    return_url = build_table_view_return_url(
        db_name=db_name,
        table_name=table_name,
        return_query=return_query,
        return_page=return_page,
        return_limit=return_limit,
        return_tables_q=return_tables_q,
    )
    field_errors: dict[str, str] = {}

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            db_items = fetch_databases(conn)
            table_items = fetch_tables(conn, db_name)
            columns_meta = fetch_columns_meta(conn, db_name, table_name)
            primary_key_columns = [column.name for column in columns_meta if column.is_primary]

            if not primary_key_columns:
                flash("Редактирование доступно только для таблиц с PRIMARY KEY.", "error")
                return redirect(return_url)

            pk_filters = build_pk_filters(primary_key_columns, source_values)

            if not pk_filters:
                flash("Не удалось определить PRIMARY KEY для выбранной строки.", "error")
                return redirect(return_url)

            existing_row = fetch_row_by_pk(conn, db_name, table_name, pk_filters)
            if not existing_row:
                flash("Строка не найдена.", "error")
                return redirect(return_url)

            editable_columns = [
                column
                for column in columns_meta
                if not column.is_primary and "GENERATED" not in column.extra.upper()
            ]

            if request.method == "POST":
                form_values = {
                    column.name: request.form.get(column.name, "")
                    for column in editable_columns
                }
            else:
                form_values = {
                    column.name: format_input_value(column, existing_row.get(column.name))
                    for column in editable_columns
                }

            if request.method == "POST":
                set_fragments: list[str] = []
                set_values: list[Any] = []

                for column in editable_columns:
                    value = normalize_form_value(
                        form_values[column.name], column, for_insert=False
                    )
                    if value is SKIP_VALUE:
                        continue
                    if value == "" and not column.is_nullable:
                        field_errors[column.name] = "Поле обязательно для заполнения."
                        continue
                    set_fragments.append(f"{quote_identifier(column.name)} = %s")
                    set_values.append(value)

                if field_errors:
                    flash("Исправь ошибки в форме.", "error")
                elif not set_fragments:
                    flash("Нет полей для обновления.", "error")
                else:
                    where_sql = " AND ".join(
                        f"{quote_identifier(column)} = %s" for column, _ in pk_filters
                    )
                    where_values = [value for _, value in pk_filters]
                    with conn.cursor() as cur:
                        cur.execute(
                            f"UPDATE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                            f"SET {', '.join(set_fragments)} WHERE {where_sql}",
                            set_values + where_values,
                        )
                    flash("Строка успешно обновлена.", "success")
                    return redirect(return_url)
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Ошибка обновления строки: {exc}", "error")

    return render_template(
        "row_form.html",
        databases=db_items if "db_items" in locals() else [],
        tables=table_items if "table_items" in locals() else [],
        current_db=db_name,
        current_table=table_name,
        mode="edit",
        editable_columns=editable_columns if "editable_columns" in locals() else [],
        form_values=form_values if "form_values" in locals() else {},
        field_errors=field_errors,
        pk_filters=pk_filters if "pk_filters" in locals() else [],
        return_page=return_page,
        return_limit=return_limit,
        return_tables_q=return_tables_q,
        return_query=return_query,
    )


@app.route("/databases/<db_name>/tables/<table_name>/rows/delete", methods=["POST"])
def delete_row(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    return_page, return_limit, return_tables_q = parse_table_return_state(request.form)
    return_query = normalize_query_string(str(request.form.get("return_query", "")))
    return_url = build_table_view_return_url(
        db_name=db_name,
        table_name=table_name,
        return_query=return_query,
        return_page=return_page,
        return_limit=return_limit,
        return_tables_q=return_tables_q,
    )

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            pk_columns = fetch_primary_key_columns(conn, db_name, table_name)
            if not pk_columns:
                flash("Удаление доступно только для таблиц с PRIMARY KEY.", "error")
                return redirect(return_url)

            pk_filters = build_pk_filters(pk_columns, request.form)
            if not pk_filters:
                flash("Не удалось определить PRIMARY KEY для удаления.", "error")
                return redirect(return_url)

            where_sql = " AND ".join(
                f"{quote_identifier(column)} = %s" for column, _ in pk_filters
            )
            params = [value for _, value in pk_filters]

            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                    f"WHERE {where_sql} LIMIT 1",
                    params,
                )
                deleted_count = max(cur.rowcount, 0)

            if deleted_count:
                flash("Строка удалена.", "success")
            else:
                flash("Строка не найдена или уже удалена.", "error")
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Ошибка удаления строки: {exc}", "error")

    return redirect(return_url)


@app.route("/databases/<db_name>/tables/<table_name>/rows/inline-update", methods=["POST"])
def inline_update_row(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    return_page, return_limit, return_tables_q = parse_table_return_state(request.form)
    return_query = normalize_query_string(str(request.form.get("return_query", "")))
    return_url = build_table_view_return_url(
        db_name=db_name,
        table_name=table_name,
        return_query=return_query,
        return_page=return_page,
        return_limit=return_limit,
        return_tables_q=return_tables_q,
    )

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            columns_meta = fetch_columns_meta(conn, db_name, table_name)
            primary_key_columns = [column.name for column in columns_meta if column.is_primary]
            if not primary_key_columns:
                flash("Inline-редактирование доступно только для таблиц с PRIMARY KEY.", "error")
                return redirect(return_url)

            pk_filters = build_pk_filters(primary_key_columns, request.form)
            if not pk_filters:
                flash("Не удалось определить PRIMARY KEY для inline-редактирования.", "error")
                return redirect(return_url)

            editable_columns = [
                column
                for column in columns_meta
                if (not column.is_primary and "GENERATED" not in column.extra.upper())
            ]
            if not editable_columns:
                flash("В таблице нет редактируемых колонок для inline-режима.", "error")
                return redirect(return_url)

            set_fragments: list[str] = []
            set_values: list[Any] = []
            field_errors: list[str] = []

            for column in editable_columns:
                raw_value = request.form.get(f"inline_{column.name}", "")
                set_null = request.form.get(f"inline_set_null_{column.name}", "0") == "1"

                if set_null:
                    if not column.is_nullable:
                        field_errors.append(
                            f"Колонка '{column.name}' не поддерживает NULL."
                        )
                        continue
                    value = None
                else:
                    value = normalize_form_value(raw_value, column, for_insert=False)
                    if value is SKIP_VALUE:
                        continue
                    if value == "" and not column.is_nullable:
                        field_errors.append(
                            f"Поле '{column.name}' обязательно для заполнения."
                        )
                        continue

                set_fragments.append(f"{quote_identifier(column.name)} = %s")
                set_values.append(value)

            if field_errors:
                flash(field_errors[0], "error")
                return redirect(return_url)

            if not set_fragments:
                flash("Нет полей для обновления.", "error")
                return redirect(return_url)

            where_sql = " AND ".join(
                f"{quote_identifier(column)} = %s" for column, _ in pk_filters
            )
            where_values = [value for _, value in pk_filters]

            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {quote_identifier(db_name)}.{quote_identifier(table_name)} "
                    f"SET {', '.join(set_fragments)} WHERE {where_sql}",
                    [*set_values, *where_values],
                )
                updated_count = max(cur.rowcount, 0)

            if updated_count:
                flash("Строка обновлена (inline).", "success")
            else:
                flash("Изменений не было (значения уже совпадают).", "error")
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Ошибка inline-редактирования: {exc}", "error")

    return redirect(return_url)


@app.route("/databases/<db_name>/tables/<table_name>/rows/bulk", methods=["POST"])
def bulk_rows(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    return_page, return_limit, return_tables_q = parse_table_return_state(request.form)
    return_query = normalize_query_string(str(request.form.get("return_query", "")))
    return_url = build_table_view_return_url(
        db_name=db_name,
        table_name=table_name,
        return_query=return_query,
        return_page=return_page,
        return_limit=return_limit,
        return_tables_q=return_tables_q,
    )

    bulk_action = request.form.get("bulk_action", "").strip()
    selected_tokens = request.form.getlist("selected_rows")

    if bulk_action not in {"delete", "update", "export_csv", "export_json"}:
        flash("Некорректное bulk-действие.", "error")
        return redirect(return_url)

    if not selected_tokens:
        flash("Выбери хотя бы одну строку.", "error")
        return redirect(return_url)

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            pk_columns = fetch_primary_key_columns(conn, db_name, table_name)
            if not pk_columns:
                flash("Bulk-операции доступны только для таблиц с PRIMARY KEY.", "error")
                return redirect(return_url)

            columns_meta = fetch_columns_meta(conn, db_name, table_name)
            column_by_name = {column.name: column for column in columns_meta}

            selected_pk_maps: list[dict[str, str]] = []
            signatures: set[str] = set()
            for token in selected_tokens:
                token = token.strip()
                if not token:
                    continue
                try:
                    parsed = json.loads(token)
                except json.JSONDecodeError:
                    continue
                if not isinstance(parsed, dict):
                    continue
                if any(column not in parsed for column in pk_columns):
                    continue
                pk_map = {column: str(parsed[column]) for column in pk_columns}
                signature = "|".join(pk_map[column] for column in pk_columns)
                if signature in signatures:
                    continue
                signatures.add(signature)
                selected_pk_maps.append(pk_map)

            if not selected_pk_maps:
                flash("Не удалось прочитать выбранные строки.", "error")
                return redirect(return_url)

            where_groups: list[str] = []
            where_params: list[Any] = []
            for pk_map in selected_pk_maps:
                group = []
                for column in pk_columns:
                    group.append(f"{quote_identifier(column)} = %s")
                    where_params.append(pk_map[column])
                where_groups.append(f"({' AND '.join(group)})")

            where_sql = " OR ".join(where_groups)
            table_sql = f"{quote_identifier(db_name)}.{quote_identifier(table_name)}"

            if bulk_action == "delete":
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {table_sql} WHERE {where_sql}", where_params)
                    deleted_count = max(cur.rowcount, 0)
                if deleted_count:
                    flash(f"Удалено строк: {deleted_count}.", "success")
                else:
                    flash("Ни одна строка не была удалена.", "error")
                return redirect(return_url)

            if bulk_action == "update":
                update_column = request.form.get("bulk_update_column", "").strip()
                update_raw_value = request.form.get("bulk_update_value", "")
                set_null = request.form.get("bulk_set_null", "0") == "1"

                if update_column not in column_by_name:
                    flash("Колонка для массового обновления не найдена.", "error")
                    return redirect(return_url)

                target_column = column_by_name[update_column]
                if "GENERATED" in target_column.extra.upper():
                    flash("GENERATED колонку нельзя обновлять через bulk.", "error")
                    return redirect(return_url)

                if set_null:
                    if not target_column.is_nullable:
                        flash("Для этой колонки нельзя установить NULL.", "error")
                        return redirect(return_url)
                    update_value = None
                else:
                    update_value = normalize_form_value(
                        update_raw_value,
                        target_column,
                        for_insert=False,
                    )
                    if update_value is SKIP_VALUE:
                        flash("Эту колонку нельзя обновить выбранным способом.", "error")
                        return redirect(return_url)
                    if update_value == "" and not target_column.is_nullable:
                        flash("Для этой колонки значение обязательно.", "error")
                        return redirect(return_url)

                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE {table_sql} SET {quote_identifier(update_column)} = %s "
                        f"WHERE {where_sql}",
                        [update_value, *where_params],
                    )
                    updated_count = max(cur.rowcount, 0)

                if updated_count:
                    flash(f"Обновлено строк: {updated_count}.", "success")
                else:
                    flash("Изменений не было (возможно, значения уже совпадают).", "error")
                return redirect(return_url)

            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table_sql} WHERE {where_sql}", where_params)
                export_rows = cur.fetchall()
                export_columns = [str(column[0]) for column in cur.description or []]

        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Ошибка bulk-операции: {exc}", "error")
        return redirect(return_url)

    if bulk_action == "export_csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(export_columns)
        for row in export_rows:
            writer.writerow([format_export_value(row.get(column)) for column in export_columns])

        filename = f"{db_name}_{table_name}_selected.csv"
        response = make_response(output.getvalue())
        response.headers["Content-Type"] = "text/csv; charset=utf-8"
        response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        return response

    export_payload = [
        {column: json_export_value(row.get(column)) for column in export_columns}
        for row in export_rows
    ]
    filename = f"{db_name}_{table_name}_selected.json"
    response = make_response(json.dumps(export_payload, ensure_ascii=False, indent=2))
    response.headers["Content-Type"] = "application/json; charset=utf-8"
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


@app.errorhandler(404)
def not_found(_: Any) -> tuple[str, int]:
    return render_error_page(
        title="Страница не найдена",
        message="Запрошенный адрес не существует или был перемещен.",
        status_code=404,
        back_url=url_for("index"),
    )


@app.errorhandler(500)
def internal_server_error(_: Any) -> tuple[str, int]:
    return render_error_page(
        title="Внутренняя ошибка приложения",
        message="Во время обработки запроса произошла непредвиденная ошибка.",
        status_code=500,
        back_url=url_for("index"),
    )


if __name__ == "__main__":
    host = os.getenv("FLASK_RUN_HOST", "127.0.0.1")
    port = int(os.getenv("FLASK_RUN_PORT", "5001"))
    debug = os.getenv("FLASK_DEBUG", "1") == "1"
    if os.getenv("OPEN_BROWSER", "0") == "1":
        webbrowser.open(f"http://{host}:{port}/")
    app.run(host=host, port=port, debug=debug)
