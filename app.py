from __future__ import annotations

import os
import re
import socket
import webbrowser
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from time import perf_counter
from typing import Any

import pymysql
from dotenv import load_dotenv
from flask import Flask, abort, flash, redirect, render_template, request, session, url_for
from werkzeug.exceptions import HTTPException


SYSTEM_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}
MAX_SQL_PREVIEW_ROWS = 500
SQL_HISTORY_LIMIT = 20
MAX_IDENTIFIER_LENGTH = 128
DEFAULT_SQL_TIMEOUT_MS = 30000
MIN_SQL_TIMEOUT_MS = 1000
MAX_SQL_TIMEOUT_MS = 300000

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


@app.route("/databases/<db_name>/tables")
def database_tables(db_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name):
        abort(404)

    filter_query = request.args.get("q", "").strip().lower()

    try:
        conn = mysql_connect()
        try:
            if not database_exists(conn, db_name):
                abort(404)

            db_items = fetch_databases(conn)
            table_items = fetch_tables(conn, db_name)

            if filter_query:
                table_items = [
                    item for item in table_items if filter_query in item.name.lower()
                ]

            total_rows = sum(t.rows_count for t in table_items)
            total_size = round(sum(t.size_mb for t in table_items), 2)
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
        filter_query=request.args.get("q", ""),
    )


@app.route("/databases/<db_name>/sql", methods=["GET", "POST"])
def sql_console(db_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name):
        abort(404)

    query_text = "SELECT NOW();"
    if request.method == "POST":
        query_text = request.form.get("sql_query", "SELECT NOW();").strip()
    result_columns: list[str] = []
    result_rows: list[list[str]] = []
    result_count = 0
    affected_rows: int | None = None
    query_ms: float | None = None
    result_truncated = False
    sql_history = [item for item in get_sql_history() if item["db"] == db_name]
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
                                raw_rows = cur.fetchmany(MAX_SQL_PREVIEW_ROWS)
                                result_rows = [
                                    [format_cell(row.get(column)) for column in result_columns]
                                    for row in raw_rows
                                ]
                                result_count = len(result_rows)
                                result_truncated = result_count >= MAX_SQL_PREVIEW_ROWS
                                push_sql_history(db_name, query_text)
                                sql_history = [item for item in get_sql_history() if item["db"] == db_name]
                                flash("SQL-запрос выполнен.", "success")
                            else:
                                affected_rows = max(cur.rowcount, 0)
                                push_sql_history(db_name, query_text)
                                sql_history = [item for item in get_sql_history() if item["db"] == db_name]
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
        timeout_ms=timeout_ms,
        current_table=current_table,
        return_page=return_page,
        return_limit=return_limit,
        tables_q=tables_q,
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
            primary_key_columns = fetch_primary_key_columns(conn, db_name, table_name)

            start = perf_counter()
            with conn.cursor() as cur:
                sql = (
                    f"SELECT * FROM {quote_identifier(db_name)}."
                    f"{quote_identifier(table_name)} LIMIT %s OFFSET %s"
                )
                cur.execute(sql, (limit + 1, offset))
                fetched_rows = cur.fetchall()
                query_ms = (perf_counter() - start) * 1000
                columns: list[str] = (
                    [str(column[0]) for column in cur.description] if cur.description else []
                )

            has_next_page = len(fetched_rows) > limit
            raw_rows = fetched_rows[:limit]

            row_items: list[dict[str, Any]] = []
            for raw_row in raw_rows:
                row_items.append(
                    {
                        "cells": [format_cell(raw_row.get(column)) for column in columns],
                        "pk_items": [
                            {"name": column, "value": format_form_value(raw_row.get(column))}
                            for column in primary_key_columns
                        ],
                        "has_pk_values": bool(primary_key_columns)
                        and all(raw_row.get(column) is not None for column in primary_key_columns),
                    }
                )

            estimated_rows = next(
                (t.rows_count for t in table_items if t.name == table_name),
                len(raw_rows),
            )
            rows_from = offset + 1 if raw_rows else 0
            rows_to = offset + len(raw_rows)
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
    )


@app.route("/databases/<db_name>/tables/<table_name>/rows/new", methods=["GET", "POST"])
def create_row(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    source_values = request.form if request.method == "POST" else request.args
    return_page, return_limit, return_tables_q = parse_table_return_state(source_values)
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
                    return redirect(
                        url_for(
                            "table_view",
                            db_name=db_name,
                            table_name=table_name,
                            page=return_page,
                            limit=return_limit,
                            tables_q=return_tables_q,
                        )
                    )
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
    )


@app.route("/databases/<db_name>/tables/<table_name>/rows/edit", methods=["GET", "POST"])
def edit_row(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    source_values = request.form if request.method == "POST" else request.args
    return_page, return_limit, return_tables_q = parse_table_return_state(source_values)
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
                return redirect(
                    url_for(
                        "table_view",
                        db_name=db_name,
                        table_name=table_name,
                        page=return_page,
                        limit=return_limit,
                        tables_q=return_tables_q,
                    )
                )

            pk_filters = build_pk_filters(primary_key_columns, source_values)

            if not pk_filters:
                flash("Не удалось определить PRIMARY KEY для выбранной строки.", "error")
                return redirect(
                    url_for(
                        "table_view",
                        db_name=db_name,
                        table_name=table_name,
                        page=return_page,
                        limit=return_limit,
                        tables_q=return_tables_q,
                    )
                )

            existing_row = fetch_row_by_pk(conn, db_name, table_name, pk_filters)
            if not existing_row:
                flash("Строка не найдена.", "error")
                return redirect(
                    url_for(
                        "table_view",
                        db_name=db_name,
                        table_name=table_name,
                        page=return_page,
                        limit=return_limit,
                        tables_q=return_tables_q,
                    )
                )

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
                    return redirect(
                        url_for(
                            "table_view",
                            db_name=db_name,
                            table_name=table_name,
                            page=return_page,
                            limit=return_limit,
                            tables_q=return_tables_q,
                        )
                    )
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
    )


@app.route("/databases/<db_name>/tables/<table_name>/rows/delete", methods=["POST"])
def delete_row(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    return_page, return_limit, return_tables_q = parse_table_return_state(request.form)

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            pk_columns = fetch_primary_key_columns(conn, db_name, table_name)
            if not pk_columns:
                flash("Удаление доступно только для таблиц с PRIMARY KEY.", "error")
                return redirect(
                    url_for(
                        "table_view",
                        db_name=db_name,
                        table_name=table_name,
                        page=return_page,
                        limit=return_limit,
                        tables_q=return_tables_q,
                    )
                )

            pk_filters = build_pk_filters(pk_columns, request.form)
            if not pk_filters:
                flash("Не удалось определить PRIMARY KEY для удаления.", "error")
                return redirect(
                    url_for(
                        "table_view",
                        db_name=db_name,
                        table_name=table_name,
                        page=return_page,
                        limit=return_limit,
                        tables_q=return_tables_q,
                    )
                )

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

    return redirect(
        url_for(
            "table_view",
            db_name=db_name,
            table_name=table_name,
            page=return_page,
            limit=return_limit,
            tables_q=return_tables_q,
        )
    )


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
