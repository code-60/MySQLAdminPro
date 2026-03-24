from __future__ import annotations

import os
import re
import socket
import webbrowser
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
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_$]+$")


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
    return bool(IDENTIFIER_PATTERN.fullmatch(name))


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

        if not username:
            flash("Введите пользователя MySQL.", "error")
            return render_template("login.html", host=host, port=port_raw, username=username)

        try:
            port = int(port_raw)
        except ValueError:
            flash("Порт должен быть числом.", "error")
            return render_template("login.html", host=host, port=port_raw, username=username)

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
            return render_template("login.html", host=host, port=port_raw, username=username)

        session["mysql_host"] = host
        session["mysql_port"] = port
        session["mysql_user"] = username
        session["mysql_password"] = password
        session["server_version"] = version

        return redirect(url_for("databases"))

    return render_template("login.html", host="127.0.0.1", port="3306", username="")


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
        flash(f"Ошибка чтения баз данных: {exc}", "error")
        return redirect(url_for("login"))

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
        flash(f"Ошибка подключения: {exc}", "error")
        return redirect(url_for("login"))

    try:
        db_items = fetch_databases(conn)
        collations = get_collations(conn)

        if request.method == "POST":
            database_name = request.form.get("database_name", "").strip()
            collation = request.form.get("collation", "utf8mb4_unicode_ci").strip()

            if not safe_database_name(database_name):
                flash(
                    "Имя БД может содержать только буквы, цифры, '_' и '$'.",
                    "error",
                )
                return render_template(
                    "new_database.html",
                    databases=db_items,
                    collations=collations,
                    selected_collation=collation,
                    database_name=database_name,
                )

            if collation not in collations:
                flash("Выбрана недоступная collation.", "error")
                return render_template(
                    "new_database.html",
                    databases=db_items,
                    collations=collations,
                    selected_collation=collation,
                    database_name=database_name,
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
        )
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Ошибка при создании БД: {exc}", "error")
        return redirect(url_for("databases"))
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
        flash(f"Ошибка чтения таблиц: {exc}", "error")
        return redirect(url_for("databases"))

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


@app.route("/databases/<db_name>/tables/<table_name>")
def table_view(db_name: str, table_name: str) -> Any:
    if not is_authenticated():
        return redirect(url_for("login"))

    if not safe_database_name(db_name) or not safe_database_name(table_name):
        abort(404)

    limit_raw = request.args.get("limit", "50")
    try:
        limit = max(1, min(500, int(limit_raw)))
    except ValueError:
        limit = 50

    try:
        conn = mysql_connect()
        try:
            if not table_exists(conn, db_name, table_name):
                abort(404)

            db_items = fetch_databases(conn)
            table_items = fetch_tables(conn, db_name)

            start = perf_counter()
            with conn.cursor() as cur:
                sql = (
                    f"SELECT * FROM {quote_identifier(db_name)}."
                    f"{quote_identifier(table_name)} LIMIT {limit}"
                )
                cur.execute(sql)
                rows = cur.fetchall()
                query_ms = (perf_counter() - start) * 1000

            columns: list[str] = list(rows[0].keys()) if rows else []
            row_values: list[list[str]] = [
                [format_cell(row.get(col)) for col in columns] for row in rows
            ]

            estimated_rows = next(
                (t.rows_count for t in table_items if t.name == table_name),
                len(rows),
            )
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - runtime-specific
        flash(f"Ошибка чтения данных таблицы: {exc}", "error")
        return redirect(url_for("database_tables", db_name=db_name))

    return render_template(
        "table_view.html",
        databases=db_items,
        current_db=db_name,
        tables=table_items,
        current_table=table_name,
        columns=columns,
        rows=row_values,
        limit=limit,
        query_ms=round(query_ms, 2),
        estimated_rows=estimated_rows,
    )


@app.errorhandler(404)
def not_found(_: Any) -> tuple[str, int]:
    return "Not Found", 404


if __name__ == "__main__":
    host = os.getenv("FLASK_RUN_HOST", "127.0.0.1")
    port = int(os.getenv("FLASK_RUN_PORT", "5001"))
    debug = os.getenv("FLASK_DEBUG", "1") == "1"
    if os.getenv("OPEN_BROWSER", "0") == "1":
        webbrowser.open(f"http://{host}:{port}/")
    app.run(host=host, port=port, debug=debug)
