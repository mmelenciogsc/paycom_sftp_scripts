"""
Utility functions for the Fabric Data Inspector.

This module collects helper functions used by the Gradio application.
Functions include Azure SQL authentication helpers, database object discovery
and inspection, row streaming generators, CSV and MongoDB export routines,
diagnostics summaries and formatting helpers.

The goal of separating these helpers from the UI is to encourage clean
coding practices: the logic can be tested independently and reused in
different contexts (e.g. CLI tools, scheduled jobs or other UIs).
"""

from __future__ import annotations

import os
import time
import csv
import struct
import json
from typing import Dict, Any, List, Tuple, Optional, Iterable, Set

import datetime as _dt
import decimal as _dec
import re
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import pyodbc
from azure.identity import (
    DefaultAzureCredential,
    AzureCliCredential,
    ManagedIdentityCredential,
    EnvironmentCredential,
)
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

import gradio as gr
import pandas as pd

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
# The API key used to authenticate requests from the UI.  Change this value
# before deploying the app to production.  Both the UI and the API endpoints
# require the caller to provide this key.
API_KEY: str = "demoadmin"

# When listing databases from a Fabric SQL endpoint we need a sensible default
# for the initial selection.  This value mirrors the default used by the UI
# (see app.py) but can be overridden via an environment variable.  If the
# specified default does not exist on the target server then the first
# available database will be selected automatically.
DEFAULT_DB_FALLBACK: str = os.environ.get("FABRIC_DEFAULT_DB", "dw-silver")

# -----------------------------------------------------------------------------
# Database discovery helpers
# -----------------------------------------------------------------------------
def list_databases(server: str, fallback_db: Optional[str] = None) -> List[str]:
    """
    Return a list of accessible database names on the given SQL/Fabric server.

    This helper attempts to query ``sys.databases`` to enumerate available
    databases.  Because cross‑database views are typically only accessible
    from the ``master`` catalog (and may not be available in user
    databases), the function tries several candidate databases in order:

    1. The user‑provided ``fallback_db`` (if any).
    2. The ``master`` database, which normally exposes all database names.
    3. The configured ``DEFAULT_DB_FALLBACK`` (e.g. ``dw-silver``).

    The first successful query that returns at least one user database
    (database_id > 4) is used.  If all attempts fail, the fallback database
    itself is returned to ensure the UI still has a valid selection.
    """
    # Build a list of candidate databases to attempt enumeration from
    candidates = []
    # Prefer querying the 'master' catalog first, as it typically exposes
    # all databases on the server.  Try the user‑selected fallback only
    # after master, and include the default fallback last.
    candidates.append("master")
    if fallback_db and fallback_db not in candidates:
        candidates.append(fallback_db)
    if DEFAULT_DB_FALLBACK and DEFAULT_DB_FALLBACK not in candidates:
        candidates.append(DEFAULT_DB_FALLBACK)
    # Try each candidate until we successfully enumerate databases
    for db_name in candidates:
        try:
            with open_sql_conn(server, db_name) as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name"
                )
                rows = cur.fetchall() or []
                dbs = [r[0] for r in rows]
                # Accept the result if it appears to list more than just itself or if this
                # is the master catalog.  In user databases, sys.databases often only
                # returns the current database, so we continue to try other candidates
                # in that case.
                if dbs:
                    # Normalise candidate name for comparison
                    c = (db_name or "").lower()
                    if c == "master" or len(dbs) > 1:
                        return dbs
                    # If the fallback was used and returned only itself, try next
                    continue
        except Exception as ex:
            # Print diagnostic information for troubleshooting
            print(f"[DEBUG] list_databases: failed to enumerate from {db_name}: {ex}")
            continue
    # If no candidates succeeded, return the fallback_db or default so the UI has a valid value
    return [fallback_db or DEFAULT_DB_FALLBACK]


def list_databases_action(api_key: str, server: str, selected_dbs: List[str]):
    """
    Gradio event handler to refresh the database list based on the given server.

    This function authenticates the API key, enumerates databases via
    `list_databases()`, and returns an updated `CheckboxGroup` component along
    with the full list of choices for state tracking.  Any previously
    selected databases that still exist will remain selected; otherwise the
    default fallback (``DEFAULT_DB_FALLBACK`` if present, else the first
    discovered database) is selected.
    """
    _require_key(api_key)
    # Use the first selected database or the default fallback to enumerate others
    # Derive a fallback database from the current selection if it looks like a list
    fallback_db = None
    # Gradio may pass a list of strings, a single string or a component instance here
    if isinstance(selected_dbs, (list, tuple)):
        fallback_db = selected_dbs[0] if selected_dbs else None
    elif isinstance(selected_dbs, str):
        fallback_db = selected_dbs
    # Otherwise leave fallback_db as None to default to DEFAULT_DB_FALLBACK
    dbs = list_databases(server, fallback_db=fallback_db)
    # Preserve existing selections where possible
    selected = [d for d in (selected_dbs or []) if d in dbs]
    if not selected:
        # If no previous selection is valid choose the fallback if present
        if DEFAULT_DB_FALLBACK in dbs:
            selected = [DEFAULT_DB_FALLBACK]
        elif dbs:
            selected = [dbs[0]]
        else:
            selected = []
    return gr.CheckboxGroup(choices=dbs, value=selected), dbs

# -----------------------------------------------------------------------------
# Azure SQL authentication helpers
# -----------------------------------------------------------------------------
def get_azure_token_attr() -> Dict[int, bytes]:
    """
    Obtain an Azure AD access token for SQL Database/Fabric.

    The function tries several credential sources in order (Azure CLI,
    Managed Identity, environment variables and Default Credentials).  If
    authentication succeeds, the token is encoded into the format required
    by the ODBC driver.  If no credential source works, a RuntimeError is
    raised explaining how to authenticate.
    """
    scope = "https://database.windows.net/.default"
    for Cred in (AzureCliCredential, ManagedIdentityCredential, EnvironmentCredential, DefaultAzureCredential):
        try:
            cred = Cred()
            token = cred.get_token(scope).token
            token_bytes = token.encode("UTF-16-LE")
            packed = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
            return {1256: packed}  # SQL_COPT_SS_ACCESS_TOKEN
        except Exception:
            continue
    raise RuntimeError(
        "Azure AD auth failed: no usable credential source found. "
        "Run 'az login' or configure environment variables / managed identity."
    )

def newest_sql_driver(default: str = "ODBC Driver 18 for SQL Server") -> str:
    """
    Return the name of the newest installed ODBC driver for SQL Server.

    If no SQL Server driver is installed, returns the provided default string.
    """
    try:
        drivers = [d for d in pyodbc.drivers() if d.endswith("for SQL Server")]
        return sorted(drivers)[-1] if drivers else default
    except Exception:
        return default

def open_sql_conn(server: str, database: str, driver: Optional[str] = None) -> pyodbc.Connection:
    """
    Open a SQL connection using Azure AD token authentication.

    The driver parameter may be specified explicitly; otherwise the newest
    available driver is used.  Encryption and trust server certificate are
    always enabled.  The access token is supplied via connection attributes.
    """
    drv = driver or newest_sql_driver()
    conn_str = (
        f"DRIVER={{{drv}}};SERVER={server};PORT=1433;DATABASE={database};"
        "Encrypt=yes;TrustServerCertificate=yes"
    )
    attrs_before = get_azure_token_attr()
    return pyodbc.connect(conn_str, attrs_before=attrs_before, autocommit=True)

# -----------------------------------------------------------------------------
# Helpers for object parsing & diagnostics
# -----------------------------------------------------------------------------
def _split_qualified(name: str) -> Tuple[str, str]:
    """
    Split a fully qualified table or view name into (schema, object).

    If no dot is present, defaults to the `dbo` schema.
    """
    if "." in name:
        s, o = name.split(".", 1)
        return s, o
    return "dbo", name

def db_count_rows(conn: pyodbc.Connection, table: str) -> Optional[int]:
    """
    Count the number of rows in the given table using COUNT_BIG().

    Returns None if the query fails (e.g. insufficient permissions).
    """
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT_BIG(*) FROM {table}")
        return int(cur.fetchone()[0])
    except Exception:
        return None

def db_object_info(conn: pyodbc.Connection, table: str) -> Dict[str, Any]:
    """
    Inspect a database object (table, view or synonym) and return a
    dictionary containing metadata such as the type description, whether it is
    a synonym, the base object for synonyms, the definition for views and
    whether the view contains a WHERE, DISTINCT or TOP clause.  If the
    object has row‑level security (RLS) policies, they are returned as a
    list.
    """
    schema, obj = _split_qualified(table)
    info: Dict[str, Any] = {
        "input": table,
        "schema": schema,
        "object": obj,
        "type_desc": None,
        "is_synonym": False,
        "synonym_base_object": None,
        "view_definition": None,
        "view_has_where_or_distinct_or_top": None,
        "rls_policies": [],
    }
    cur = conn.cursor()

    # Determine object type (table/view/synonym)
    try:
        cur.execute(
            """
            SELECT o.type_desc
            FROM sys.objects o
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = ? AND o.name = ?
            """,
            (schema, obj),
        )
        row = cur.fetchone()
        if row:
            info["type_desc"] = row[0]
    except Exception:
        pass

    # If synonym, find base object
    try:
        cur.execute(
            """
            SELECT sy.base_object_name
            FROM sys.synonyms sy
            JOIN sys.schemas s ON s.schema_id = sy.schema_id
            WHERE s.name = ? AND sy.name = ?
            """,
            (schema, obj),
        )
        row = cur.fetchone()
        if row:
            info["is_synonym"] = True
            info["synonym_base_object"] = row[0]
    except Exception:
        pass

    # For views, fetch definition and detect WHERE/DISTINCT/TOP
    if info["type_desc"] and "VIEW" in info["type_desc"]:
        try:
            cur.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?))", (f"{schema}.{obj}",))
            row = cur.fetchone()
            if row and row[0]:
                view_def = row[0]
                info["view_definition"] = view_def
                info["view_has_where_or_distinct_or_top"] = bool(
                    re.search(r"\bWHERE\b|\bDISTINCT\b|\bTOP\s*\(", view_def, flags=re.IGNORECASE)
                )
        except Exception:
            pass

    # Row level security policies
    try:
        cur.execute(
            """
            SELECT sp.name AS policy_name, sp.is_enabled,
                   pr.predicate_type_desc,
                   OBJECT_SCHEMA_NAME(pr.target_object_id) AS target_schema,
                   OBJECT_NAME(pr.target_object_id)        AS target_table,
                   pr.predicate_definition
            FROM sys.security_policies sp
            JOIN sys.security_predicates pr ON sp.object_id = pr.object_id
            WHERE pr.target_object_id = OBJECT_ID(?)
            """,
            (f"{schema}.{obj}",),
        )
        rows = cur.fetchall() or []
        info["rls_policies"] = [
            {
                "policy_name": r[0],
                "is_enabled": bool(r[1]),
                "predicate_type": r[2],
                "target_schema": r[3],
                "target_table": r[4],
                "predicate_definition": r[5],
            }
            for r in rows
        ]
    except Exception:
        pass

    return info

def list_objects(
    conn: pyodbc.Connection,
    name_filter: Optional[str],
    include_views: bool,
    include_synonyms: bool,
    max_items: Optional[int],
) -> List[Dict[str, Any]]:
    """
    List user tables, views and synonyms in the database.

    If `name_filter` is provided, only objects whose schema name, object name
    or fully qualified name contain the filter text are returned.  Views and
    synonyms can be included or excluded.  If `max_items` is a positive
    integer, the result set is limited using `TOP (n)`; if `None` or `<= 0` no
    cap is applied (potentially returning all objects).  The result is sorted
    by schema and object name.
    """
    parts = []
    # Base query for user tables
    parts.append(
        "SELECT s.name AS schema_name, t.name AS object_name, 'USER_TABLE' AS type_desc, "
        "       s.name + '.' + t.name AS qualified_name, CAST(NULL AS NVARCHAR(512)) AS synonym_base_object "
        "FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id"
    )
    # Include views if requested
    if include_views:
        parts.append(
            "SELECT s.name, v.name, 'VIEW', s.name + '.' + v.name, CAST(NULL AS NVARCHAR(512)) "
            "FROM sys.views v JOIN sys.schemas s ON s.schema_id = v.schema_id"
        )
    # Include synonyms if requested
    if include_synonyms:
        parts.append(
            "SELECT s.name, sy.name, 'SYNONYM', s.name + '.' + sy.name, sy.base_object_name "
            "FROM sys.synonyms sy JOIN sys.schemas s ON s.schema_id = sy.schema_id"
        )

    unioned = "\nUNION ALL\n".join(parts)

    params = []
    where_clause = ""
    if name_filter and name_filter.strip():
        like = f"%{name_filter.strip()}%"
        where_clause = " WHERE ? IS NULL OR o.object_name LIKE ? OR o.schema_name LIKE ? OR o.qualified_name LIKE ?"
        params.extend([name_filter.strip(), like, like, like])
    else:
        where_clause = " WHERE ? IS NULL"
        params.append(None)

    # Build query with optional TOP clause
    if max_items is not None and int(max_items) > 0:
        query = f"""
            SELECT TOP (?) o.schema_name, o.object_name, o.type_desc, o.qualified_name, o.synonym_base_object
            FROM ({unioned}) o
            {where_clause}
            ORDER BY o.schema_name, o.object_name
        """
        params = [int(max_items)] + params
    else:
        query = f"""
            SELECT o.schema_name, o.object_name, o.type_desc, o.qualified_name, o.synonym_base_object
            FROM ({unioned}) o
            {where_clause}
            ORDER BY o.schema_name, o.object_name
        """

    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall() or []
    out = []
    for r in rows:
        out.append({
            "schema": r[0],
            "object": r[1],
            "type_desc": r[2],
            "qualified_name": r[3],
            "synonym_base_object": r[4],
        })
    return out

# -----------------------------------------------------------------------------
# Row streaming and sanitisation helpers
# -----------------------------------------------------------------------------
def stream_rows(conn: pyodbc.Connection, table: str, limit_rows: int, arraysize: int) -> Tuple[List[str], Iterable[List[Dict[str, Any]]]]:
    """
    Stream rows from a table in batches using fetchmany(), enforcing limits client‑side.

    Returns a tuple containing the column names and a generator that yields
    batches of dictionaries (one dictionary per row). Each batch contains at
    most `arraysize` rows. The generator stops when either the table contains
    no more rows or the `limit_rows` cap is reached.

    Important: We intentionally avoid using "TOP (n)" in the SQL statement and
    enforce the cap in Python. Some environments (including certain Fabric
    configurations) may apply implicit caps when TOP is present, which can
    prematurely truncate results (e.g., at 10,000 rows). Fetching the full
    result set and slicing client‑side avoids that issue while maintaining
    predictable batching and memory usage.
    """
    cur = conn.cursor()
    # Clear any session row limits and avoid server-side TOP; enforce in client loop
    try:
        cur.execute("SET NOCOUNT ON; SET ROWCOUNT 0;")
    except Exception:
        # Some drivers may not allow compound statement; attempt separately
        try:
            cur.execute("SET NOCOUNT ON;")
            cur.execute("SET ROWCOUNT 0;")
        except Exception:
            pass
    cur.execute(f"SELECT * FROM {table}")
    cols = [c[0] for c in cur.description]
    size = max(1, int(arraysize))
    cur.arraysize = size

    # Default to conservative per-row fetch to avoid environment-imposed caps.
    # Set FDI_FAST_FETCH=1 to enable batched fetchmany mode.
    per_row_mode = not (str(os.environ.get("FDI_FAST_FETCH", "")).lower() in ("1", "true", "yes"))

    def gen() -> Iterable[List[Dict[str, Any]]]:
        remaining = int(limit_rows)
        if per_row_mode:
            # Conservative path: fetch one row at a time and yield in small batches
            buf = []
            while remaining > 0:
                row = cur.fetchone()
                if not row:
                    break
                buf.append(dict(zip(cols, row)))
                remaining -= 1
                if len(buf) >= size:
                    yield buf
                    buf = []
            if buf:
                yield buf
            return
        # Default fast path using fetchmany()
        while remaining > 0:
            rows = cur.fetchmany(size)
            if not rows:
                break
            if len(rows) > remaining:
                rows = rows[:remaining]
            batch_dicts = [dict(zip(cols, row)) for row in rows]
            yield batch_dicts
            remaining -= len(rows)

    return cols, gen()

def _sanitize_key(k: Any) -> str:
    """
    Sanitize a dictionary key for MongoDB: replace leading '$' and dots.
    """
    k = str(k)
    if k.startswith("$"):
        k = "_" + k[1:]
    if "." in k:
        k = k.replace(".", "_")
    return k

def _to_bson_safe(v: Any) -> Any:
    """
    Convert Python values into BSON‑safe values.  Dates are converted to
    datetimes, decimals to floats/strings, and complex types (set, tuple,
    list, dict) are converted recursively.  Byte sequences are preserved.
    """
    if isinstance(v, _dt.date) and not isinstance(v, _dt.datetime):
        return _dt.datetime.combine(v, _dt.time.min)
    if isinstance(v, _dec.Decimal):
        try:
            return float(v)
        except Exception:
            return str(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v)
    if isinstance(v, set):
        return list(v)
    if isinstance(v, tuple):
        return [_to_bson_safe(x) for x in v]
    if isinstance(v, list):
        return [_to_bson_safe(x) for x in v]
    if isinstance(v, dict):
        return {_sanitize_key(k): _to_bson_safe(val) for k, val in v.items()}
    return v

def sanitize_batch(batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Apply `_sanitize_key` and `_to_bson_safe` to each document in a batch.
    """
    return [
        { _sanitize_key(k): _to_bson_safe(v) for k, v in doc.items() }
        for doc in batch
    ]

def _timestamp() -> str:
    """
    Generate a safe timestamp (YYYYMMDD_HHMMSS) for use in filenames.
    """
    return _dt.datetime.now().strftime("%Y%m%d_%H%M%S")

def _csv_root_dir(csv_root: str) -> str:
    """
    Normalise a CSV root path: if it points at a file, use its parent
    directory; otherwise return the expanded path unchanged.  If the root
    points at a .csv file, the directory in which the file resides is used
    instead.
    """
    # Resolve relative paths against the repository root (directory of this file)
    base = os.path.dirname(os.path.abspath(__file__))
    path_in = csv_root or "."
    path = os.path.expanduser(path_in)
    if not os.path.isabs(path):
        path = os.path.abspath(os.path.join(base, path))
    if path.lower().endswith(".csv"):
        parent = os.path.dirname(path) or "."
        return parent
    abs_path = os.path.abspath(path)
    if os.path.exists(abs_path) and not os.path.isdir(abs_path):
        parent = os.path.dirname(abs_path) or "."
        return parent
    return path

def _resolve_csv_path(csv_root: str, database: str, table: str) -> str:
    """
    Construct a CSV output path for a table. The file is placed under
    `<csv_root>/<database_sanitized>/<object>_<timestamp>.csv`, creating directories
    as necessary. Names are sanitized for filesystem safety.
    """
    schema, obj = _split_qualified(table)
    root = _csv_root_dir(csv_root)
    # Sanitize parts for filesystem
    def _sanitize_filename(name: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]", "_", str(name or ""))
    dir_path = os.path.join(root, _sanitize_filename(database))
    os.makedirs(dir_path, exist_ok=True)
    fname = f"{_sanitize_filename(obj)}_{_timestamp()}.csv"
    return os.path.join(dir_path, fname)

def _mongo_safe_coll_name(full_name: str) -> str:
    """
    Derive a MongoDB collection name from a database-qualified object name.

    Input may be just "schema.table" or "database.schema.table"; all dots are
    converted to underscores so names from different databases remain distinct.
    """
    return str(full_name or "").replace('.', '_')

def sanitize_mongo_uri(uri: str) -> str:
    """
    Fix common misspellings and strip problematic options in a MongoDB URI.

    Currently corrects 'serverSelectionTmeoutMS' → 'serverSelectionTimeoutMS'.
    Keeps other options as-is.
    """
    try:
        if not uri or '?' not in uri:
            return uri
        scheme, netloc, path, query, frag = urlsplit(uri)
        params = parse_qsl(query, keep_blank_values=True)
        new_params = []
        for k, v in params:
            kl = k.lower()
            if kl == 'serverselectiontmeoutms':
                # Correct the misspelling
                new_params.append(('serverSelectionTimeoutMS', v))
            else:
                new_params.append((k, v))
        new_query = urlencode(new_params)
        return urlunsplit((scheme, netloc, path, new_query, frag))
    except Exception:
        # On any parsing error, return the original URI
        return uri

def _detect_latest_column(conn, table: str) -> Optional[str]:
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT c.name, t.name AS typ
            FROM sys.columns c
            JOIN sys.types t ON t.user_type_id=c.user_type_id
            WHERE c.object_id = OBJECT_ID(?)
            """,
            (table,),
        )
        rows = cur.fetchall() or []
        cand = []
        for name, typ in rows:
            n = (name or "").lower()
            t = (typ or "").lower()
            if t in ("date","datetime","datetime2","smalldatetime","timestamp","datetimeoffset"):
                score = 0
                for key in ("create","created","insert","updated","update","time","date","ts"):
                    if key in n:
                        score += 1
                cand.append((score, name))
        cand.sort(reverse=True)
        return cand[0][1] if cand else None
    except Exception:
        return None


def _parse_dt(s: str) -> Optional[_dt.datetime]:
    try:
        return _dt.datetime.fromisoformat(s)
    except Exception:
        try:
            return _dt.datetime.strptime(s, "%Y-%m-%d")
        except Exception:
            return None


def benchmark_sql_to_csv(
    server: str,
    database: str,
    table: str,
    limit_rows: int,
    sql_arraysize: int,
    csv_root: str,
    progress: Optional[gr.Progress] = None,
    multi: bool = False,
    latest_auto: bool = False,
    order_by_cols: Optional[str] = None,
    order_dir: str = "DESC",
    since_text: Optional[str] = None,
    until_text: Optional[str] = None,
    last_n_days: int = 0,
) -> Dict[str, Any]:
    """
    Export rows from a SQL table to a CSV file and report performance metrics.

    Opens a connection to SQL, counts available rows, streams up to
    `limit_rows` rows in batches of `sql_arraysize`, writes them to a CSV file
    under `csv_root` and returns a dictionary with elapsed time, throughput,
    output path and file size.  If a progress object is provided the export
    updates it during processing.
    """
    t0 = time.perf_counter()
    rows_written = 0
    error_message: Optional[str] = None
    available: Optional[int] = None
    file_path = None
    try:
        # Resolve output path and ensure directories exist (inside try for robust error reporting)
        file_path = os.path.abspath(_resolve_csv_path(csv_root, database, table))
        with open_sql_conn(server, database) as conn:
            # Count available rows once at the start
            available = db_count_rows(conn, table)
            # Determine effective target rows for progress/summary
            try:
                lim = int(limit_rows)
            except Exception:
                lim = 0
            target_rows = lim if lim > 0 else (available or 0)
            if available is not None and lim > 0:
                target_rows = min(lim, int(available))
            # Build filters and ordering
            ob_cols: List[str] = []
            if order_by_cols and str(order_by_cols).strip():
                ob_cols = [c.strip() for c in str(order_by_cols).split(',') if c.strip()]
            elif latest_auto:
                try:
                    ob = _detect_latest_column(conn, table)
                    if ob:
                        ob_cols = [ob]
                except Exception:
                    ob_cols = []
            where_clause = ""
            params: List[Any] = []
            date_col = ob_cols[0] if ob_cols else None
            if latest_auto and not date_col:
                try:
                    date_col = _detect_latest_column(conn, table)
                except Exception:
                    date_col = None
            # Compute since/until
            since_dt = None
            until_dt = None
            try:
                if last_n_days and int(last_n_days) > 0:
                    since_dt = _dt.datetime.now() - _dt.timedelta(days=int(last_n_days))
                if since_text and str(since_text).strip():
                    p = _parse_dt(str(since_text).strip())
                    if p:
                        since_dt = p
                if until_text and str(until_text).strip():
                    p = _parse_dt(str(until_text).strip())
                    if p:
                        until_dt = p
            except Exception:
                pass
            if date_col and (since_dt or until_dt):
                parts = []
                if since_dt:
                    parts.append(f"[{date_col}] >= ?")
                    params.append(since_dt)
                if until_dt:
                    parts.append(f"[{date_col}] < ?")
                    params.append(until_dt)
                if parts:
                    where_clause = " WHERE " + " AND ".join(parts)
            # Build SELECT with optional filters & ordering; still stream client-side
            order_sql = ""
            if ob_cols:
                dir_sql = "DESC" if str(order_dir).upper() == "DESC" else "ASC"
                order_sql = " ORDER BY " + ", ".join([f"[{c}] {dir_sql}" for c in ob_cols])
            top_sql = f"TOP {target_rows} " if target_rows > 0 else ""
            sel_sql = f"SELECT {top_sql}* FROM {table}{where_clause}{order_sql}"
            first = True
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                cur = conn.cursor()
                try:
                    cur.execute("SET NOCOUNT ON; SET ROWCOUNT 0;")
                except Exception:
                    pass
                cur.arraysize = max(1, int(sql_arraysize))
                cur.execute(sel_sql, params)
                cols = [c[0] for c in cur.description]
                writer = csv.DictWriter(f, fieldnames=cols)
                while True:
                    batch_rows = cur.fetchmany(cur.arraysize)
                    if not batch_rows:
                        break
                    # Enforce limit client-side
                    if target_rows and rows_written + len(batch_rows) > target_rows:
                        batch_rows = batch_rows[: max(0, target_rows - rows_written)]
                    if first:
                        writer.writeheader()
                        first = False
                    try:
                        writer.writerows([dict(zip(cols, r)) for r in batch_rows])
                        rows_written += len(batch_rows)
                    except Exception as ex:
                        error_message = (error_message or str(ex))
                        for row in [dict(zip(cols, r)) for r in batch_rows]:
                            try:
                                writer.writerow(row)
                                rows_written += 1
                            except Exception as ex2:
                                error_message = (error_message or str(ex2))
                    if progress:
                        tot = int(target_rows or 0)
                        frac = (rows_written / max(1, tot)) if tot > 0 else 0.0
                        progress(
                            frac,
                            desc=f"[{table}] CSV: {rows_written:,}/{tot:,}",
                            count=int(rows_written),
                            total=tot,
                        )
    except Exception as exc:
        # Capture any exception during path resolution, connection, streaming or writing
        error_message = str(exc)
    # Compute elapsed time and file size regardless of success
    elapsed = time.perf_counter() - t0
    size_bytes = os.path.getsize(file_path) if (file_path and os.path.exists(file_path)) else 0
    result: Dict[str, Any] = {
        "strategy": "SQL -> CSV",
        "available_rows": available,
        "rows_target": int(target_rows if 'target_rows' in locals() else int(limit_rows)),
        "rows_actual": int(rows_written),
        "elapsed_sec": round(elapsed, 3),
        "throughput_rows_per_sec": round(rows_written / elapsed, 2) if elapsed > 0 else None,
        "output": file_path,
        "file_size_bytes": int(size_bytes),
        "table": table,
    }
    # Only report error if nothing was written; otherwise treat issues as non-fatal
    if rows_written == 0 and error_message:
        result["error"] = error_message
    return result

def benchmark_sql_to_mongo(
    server: str,
    database: str,
    table: str,
    limit_rows: int,
    sql_arraysize: int,
    mongo_uri: str,
    mongo_db: str,
    mongo_coll_ignored: str,
    mongo_batch: int,
    drop_before: bool,
    write_concern_w: int,
    progress: Optional[gr.Progress] = None,
    multi: bool = False,
) -> Dict[str, Any]:
    """
    Export rows from a SQL table into a MongoDB collection and report metrics.

    Connects to MongoDB using the provided URI and database name.  The target
    collection name is derived from the fully qualified table name by
    replacing dots with underscores.  If `drop_before` is True the existing
    collection is dropped before inserting documents.  Rows are streamed from
    SQL in batches of `sql_arraysize`, sanitised for MongoDB and inserted in
    batches of size `mongo_batch`.  The returned dictionary includes
    throughput and other run statistics.  A progress object can be used to
    update the UI.
    """
    t0 = time.perf_counter()
    rows_inserted = 0
    error_message: Optional[str] = None
    available: Optional[int] = None
    coll = None
    coll_name = _mongo_safe_coll_name(f"{database}.{table}")

    # Global slice size: 500K rows per connection-slice
    GLOBAL_SLICE = 500_000
    # Clamp insert batch size to a safe window (avoid huge single inserts)
    try:
        insert_batch = int(mongo_batch)
    except Exception:
        insert_batch = 10000
    insert_batch = max(1000, min(10000, insert_batch))

    # Helpers for ordering and resume
    def _discover_order_cols(conn) -> Tuple[List[str], str]:
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT c.name, ic.key_ordinal
                FROM sys.indexes i
                JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
                JOIN sys.columns c ON c.object_id=i.object_id AND c.column_id=ic.column_id
                WHERE i.object_id = OBJECT_ID(?) AND i.is_primary_key=1
                ORDER BY ic.key_ordinal
                """,
                (table,),
            )
            rows = cur.fetchall() or []
            if rows:
                return [r[0] for r in rows], "keyset"
        except Exception:
            pass
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT TOP 1 i.index_id FROM sys.indexes i WHERE i.object_id=OBJECT_ID(?) AND i.is_unique=1 AND i.is_primary_key=0 ORDER BY i.index_id",
                (table,),
            )
            r = cur.fetchone()
            if r:
                idx = int(r[0])
                cur.execute(
                    """
                    SELECT c.name, ic.key_ordinal
                    FROM sys.index_columns ic
                    JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
                    WHERE ic.object_id=OBJECT_ID(?) AND ic.index_id=?
                    ORDER BY ic.key_ordinal
                    """,
                    (table, idx),
                )
                cols = [x[0] for x in (cur.fetchall() or [])]
                if cols:
                    return cols, "keyset"
        except Exception:
            pass
        # Fallback to ROW_NUMBER
        return [], "rownumber"

    def _order_by_sql(cols: List[str]) -> str:
        if not cols:
            return " ORDER BY (SELECT 1)"
        return " ORDER BY " + ", ".join([f"[{c}] ASC" for c in cols])

    def _build_keyset_where(cols: List[str], last_vals: List[Any]) -> Tuple[str, List[Any]]:
        if not cols or not last_vals:
            return "", []
        n = len(cols)
        pieces = []
        params: List[Any] = []
        for j in range(n):
            parts = []
            for k in range(j):
                parts.append(f"[{cols[k]}] = ?")
                params.append(last_vals[k])
            parts.append(f"[{cols[j]}] > ?")
            params.append(last_vals[j])
            pieces.append("(" + " AND ".join(parts) + ")")
        return " WHERE " + " OR ".join(pieces), params

    try:
        # Open Mongo client and collection inside try so connection issues are captured
        client = MongoClient(
            sanitize_mongo_uri(mongo_uri),
            uuidRepresentation="standard",
            connectTimeoutMS=15_000,
            serverSelectionTimeoutMS=15_000,
        )
        dbm = client[mongo_db]
        coll = dbm.get_collection(coll_name).with_options(write_concern=WriteConcern(w=int(write_concern_w)))
        if drop_before:
            coll.drop()

        # Determine available rows and ordering
        try:
            with open_sql_conn(server, database) as conn0:
                available = db_count_rows(conn0, table)
                order_cols, strat = _discover_order_cols(conn0)
        except Exception:
            order_cols, strat = [], "rownumber"

        remaining = int(limit_rows) if int(limit_rows) > 0 else None
        last_vals: List[Any] = []
        last_rn = 0

        while True:
            # Determine slice size (<= 500k and <= remaining if set)
            if remaining is None:
                slice_target = GLOBAL_SLICE
            else:
                slice_target = min(GLOBAL_SLICE, max(0, int(remaining)))
                if slice_target <= 0:
                    break

            fetched = 0
            with open_sql_conn(server, database) as conn2:
                cur = conn2.cursor()
                try:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
                except Exception:
                    pass
                if strat == "keyset" and order_cols:
                    wsql, params = _build_keyset_where(order_cols, last_vals)
                    sql = f"SELECT * FROM {table}{wsql}{_order_by_sql(order_cols)}"
                    cur.execute(sql, params)
                else:
                    ob = _order_by_sql(order_cols)
                    sql = f"SELECT * FROM (SELECT ROW_NUMBER() OVER ({ob}) AS rn, * FROM {table}) q WHERE q.rn > ? ORDER BY q.rn"
                    cur.execute(sql, (int(last_rn),))

                size = max(1, int(sql_arraysize))
                cur.arraysize = size
                cols = None
                last_row = None
                while True:
                    rows = cur.fetchmany(size)
                    if not rows:
                        break
                    if cols is None:
                        cols = [c[0] for c in cur.description]
                    # Trim to slice target
                    if fetched + len(rows) > slice_target:
                        rows = rows[: (slice_target - fetched)]
                    docs = [dict(zip(cols, r)) for r in rows]
                    if docs:
                        last_row = docs[-1]
                        sdocs = sanitize_batch(docs)
                        for i in range(0, len(sdocs), insert_batch):
                            t_chunk = time.perf_counter()
                            try:
                                coll.insert_many(sdocs[i : i + insert_batch], ordered=False)
                                rows_inserted += len(sdocs[i : i + insert_batch])
                            except Exception as ex:
                                error_message = (error_message or str(ex))
                                # Fallback to binary-split retry
                                chunk = sdocs[i : i + insert_batch]
                                if len(chunk) == 1:
                                    pass
                                else:
                                    mid = len(chunk) // 2
                                    try:
                                        coll.insert_many(chunk[:mid], ordered=False)
                                        rows_inserted += len(chunk[:mid])
                                    except Exception as ex2:
                                        error_message = (error_message or str(ex2))
                                    try:
                                        coll.insert_many(chunk[mid:], ordered=False)
                                        rows_inserted += len(chunk[mid:])
                                    except Exception as ex3:
                                        error_message = (error_message or str(ex3))
                            # Progress after each chunk insert
                            if progress and int(limit_rows) > 0:
                                dt_ms = int((time.perf_counter() - t_chunk) * 1000)
                                frac = min(1.0, rows_inserted / max(1, int(limit_rows)))
                                progress(
                                    frac,
                                    desc=f"[{table}] MongoDB: {rows_inserted:,}/{int(limit_rows):,}",
                                    count=int(rows_inserted),
                                    total=int(limit_rows),
                                    chunk=int(min(insert_batch, len(sdocs) - i)),
                                    dt_ms=dt_ms,
                                )
                        fetched += len(docs)
                        # per-chunk progress already reported
                    if fetched >= slice_target:
                        break
                if strat == "keyset" and order_cols and last_row:
                    last_vals = [last_row.get(c) for c in order_cols]
                else:
                    last_rn += fetched

            # Stop conditions
            if fetched < slice_target:
                break  # source exhausted
            if remaining is not None:
                remaining -= fetched
                if remaining <= 0:
                    break

        if progress and int(limit_rows) > 0:
            progress(1.0, desc=f"[{table}] MongoDB: {rows_inserted:,}/{int(limit_rows):,}", count=int(rows_inserted), total=int(limit_rows))
    except Exception as exc:
        # Capture exceptions during connection, paging, or insert
        error_message = str(exc)
    # Compute elapsed time regardless of success
    elapsed = time.perf_counter() - t0
    result: Dict[str, Any] = {
        "strategy": "SQL -> MongoDB",
        "available_rows": available,
        "rows_target": int(limit_rows),        "rows_actual": int(rows_inserted),
        "elapsed_sec": round(elapsed, 3),
        "throughput_rows_per_sec": round(rows_inserted / elapsed, 2) if elapsed > 0 else None,
        "output": f"{mongo_uri}/{mongo_db}.{coll_name}",
        "file_size_bytes": None,
        "table": table,
    }
    # Only report error if nothing was written; otherwise treat issues as non-fatal
    if rows_inserted == 0 and error_message:
        result["error"] = error_message
    return result

# -----------------------------------------------------------------------------
# Security helper for Gradio API
# -----------------------------------------------------------------------------
def _require_key(api_key: str) -> None:
    """
    Raise a Gradio error if the provided API key does not match the configured key.
    """
    if not api_key or api_key.strip() != API_KEY:
        raise gr.Error("Unauthorized: invalid API key")

# -----------------------------------------------------------------------------
# Summary formatting + pretty JSON helper (screen‑reader friendly)
# -----------------------------------------------------------------------------
def _fmt_int(x: Any) -> str:
    return f"{x:,}" if isinstance(x, int) else str(x)

def _fmt_num(x: Any) -> str:
    if x is None:
        return "—"
    try:
        return f"{x:,.2f}"
    except Exception:
        return str(x)

def summarize(results: List[Dict[str, Any]], header: str = "Summary") -> str:
    """
    Build a Markdown summary from a list of result dictionaries.

    The summary highlights the number of rows processed, elapsed time and
    throughput for each strategy.  When multiple results are provided the
    fastest strategy is emphasised at the end.
    """
    if not results:
        return "No results."
    lines = [f"## {header}"]
    for r in (results or []):
        try:
            strategy = r.get("strategy", "?")
            table = r.get("table", "")
            rows_actual = r.get("rows_actual", 0)
            rows_target = r.get("rows_target", 0)
            elapsed = r.get("elapsed_sec", None)
            tput = r.get("throughput_rows_per_sec", None)
            output = r.get("output", "")
            fsize = r.get("file_size_bytes")
            avail = r.get("available_rows")
            avail_txt = f" (available: {_fmt_int(avail)})" if avail is not None else ""
            err_txt = f", error: {r['error']}" if r.get("error") else ""
            line = (
                f"- **{strategy}** on `{table}` — "
                f"rows: {_fmt_int(rows_actual)}/{_fmt_int(rows_target)}{avail_txt}, "
                f"time: {_fmt_num(elapsed)} s, "
                f"throughput: {_fmt_num(tput)} rows/s, "
                f"output: `{output}`"
            )
            if fsize:
                line += f", file size: {_fmt_int(fsize)} bytes"
            line += err_txt
            lines.append(line)
        except Exception as ex:
            lines.append(f"- [summary build error: {ex} for item: {r}]")
    if results and len(results) >= 2:
        best = max(results, key=lambda r: (r.get("throughput_rows_per_sec") or 0))
        lines.append(
            f"\n**Faster overall:** {best['strategy']} "
            f"({_fmt_num(best['throughput_rows_per_sec'])} rows/s)."
        )
    # If nothing was added beyond the header, provide a helpful placeholder
    if len(lines) == 1:
        lines.append("- No results to summarize. Check connection, credentials, and settings.")
    # Note: removed the final Raw Data Preview header here because the UI now
    # provides a dedicated preview area. Displaying another header in the
    # summary causes duplicate "Raw Data Preview" headings in the web UI. The
    # preview content will be shown separately via the returned raw JSON from
    # the calling function.
    return "\n".join(lines)

def pretty_json(obj: Any) -> str:
    """
    Serialize Python objects into indented JSON, handling dates, ObjectIds and decimals.
    """
    def _default(o: Any) -> Any:
        if isinstance(o, (_dt.datetime, _dt.date, _dt.time)):
            return o.isoformat()
        try:
            import bson  # optional
            if isinstance(o, bson.ObjectId):
                return str(o)
        except Exception:
            pass
        if isinstance(o, _dec.Decimal):
            try:
                return float(o)
            except Exception:
                return str(o)
        return str(o)
    return json.dumps(obj, indent=2, ensure_ascii=False, default=_default)

def _preview_json_limited(objs: Any, max_objects: int = 30, max_lines: int = 10) -> str:
    """
    Produce a truncated pretty‑printed JSON preview for a list of objects.

    This helper limits the preview to at most ``max_objects`` items from the input
    sequence, and includes only the first ``max_lines`` lines from each item's
    JSON representation.  Use this to avoid overwhelming the UI with very
    large datasets while still showing a representative sample of the
    structure.  The default has been increased to allow more objects and
    lines to be displayed (30 objects, 10 lines per object).

    Args:
        objs: A list of objects or any serialisable item.  If the input is not
            a list, ``pretty_json`` is used directly.
        max_objects: Maximum number of objects to include from the list.
        max_lines: Maximum number of lines to include per object.

    Returns:
        A newline‑separated string containing the truncated JSON previews.
    """
    # When the input is not a list, fall back to pretty_json
    if not isinstance(objs, list):
        return pretty_json(objs)
    lines = []
    for obj in objs[:max_objects]:
        try:
            text = pretty_json(obj)
        except Exception:
            text = pretty_json(str(obj))
        obj_lines = text.splitlines()
        truncated = obj_lines[: max_lines]
        lines.extend(truncated)
    return "\n".join(lines)

# New helper: generate a concise preview for export/diagnostics results
def generate_data_preview(previews: List[Dict[str, Any]], max_objects: int = 30, max_entries: int = 10) -> str:
    """
    Produce a JSON preview of exported rows with limits on the number of objects
    and number of rows per object.

    Each element in ``previews`` is expected to be a dictionary containing at
    least the keys ``database``, ``table`` and ``rows`` (a list of row
    dictionaries).  This helper trims the list to at most ``max_objects``
    objects and, within each object, truncates the ``rows`` list to at most
    ``max_entries`` items.  If an object's ``rows`` list is empty, the
    special marker "[...]" is used to indicate the absence of data.  This
    behaviour is designed to satisfy the UI requirement that the Raw Data
    Preview area shows up to 10 entries per object and, when no data is
    available, displays a placeholder message instead of raising errors.

    The returned string is the pretty‑printed JSON representation of the
    truncated list.

    Args:
        previews: A list of preview dictionaries generated by the export or
            diagnostics functions.
        max_objects: The maximum number of objects (tables) to include in
            the preview.  Additional objects beyond this limit are dropped.
        max_entries: The maximum number of row entries per object.

    Returns:
        A string containing pretty‑printed JSON of the truncated preview.
    """
    if not isinstance(previews, list):
        # If previews is not a list, fall back to default JSON representation
        return pretty_json(previews)
    trimmed = []
    for p in previews[:max_objects]:
        # Ensure we work with a shallow copy to avoid modifying the original
        item: Dict[str, Any] = {k: v for k, v in p.items() if k != "rows"}
        rows = p.get("rows", [])
        if rows:
            # Truncate rows list to the maximum number of entries
            item["rows"] = rows[: max_entries]
        else:
            # When there is no data, include a placeholder indicator
            item["rows"] = ["[...]"]
        trimmed.append(item)
    return pretty_json(trimmed)

# New helper to build a raw data preview with object annotations.
def format_raw_data_preview(previews: List[Dict[str, Any]], max_objects: int = 30, max_entries: int = 10) -> str:
    """
    Produce a human‑readable preview for exported data grouped by object.

    Each preview entry is expected to contain at least the keys ``database``,
    ``table`` and ``rows``.  This helper iterates over up to ``max_objects``
    entries and constructs a textual representation with the object name
    prefixed by a comment line.  Up to ``max_entries`` rows per object are
    included.  If an object has no rows, a ``"[...]"`` placeholder is used.

    The output is not strictly valid JSON because comment lines are included,
    but it provides a concise, copyable representation of the actual data
    returned by the exports.  It also avoids any index errors by checking
    list lengths explicitly.

    Args:
        previews: List of preview dictionaries from run_csv_selected,
            run_mongo_selected or run_both_selected.
        max_objects: Maximum number of objects to include in the output.
        max_entries: Maximum number of row entries per object.

    Returns:
        A string consisting of comment lines and pretty‑printed row
        dictionaries, separated by newlines.
    """
    if not isinstance(previews, list):
        return pretty_json(previews)
    lines = []
    # Limit to the first ``max_objects`` preview entries
    # Sort objects so that those with the most rows appear first.  This helps
    # ensure that empty objects (with no data) don't crowd the beginning of
    # the preview, which could be mistaken for an entirely empty result.
    sorted_previews = sorted(previews[:max_objects], key=lambda p: len(p.get("rows", []) or []), reverse=True)
    for p in sorted_previews:
        # Compose a label for the object.  Prefer the encoded db|table when available.
        db = p.get("database", "")
        tbl = p.get("table", "")
        object_label = f"{db}|{tbl}" if db else tbl
        # Comment line for the object
        lines.append(f"# {object_label}")
        rows = p.get("rows", []) or []
        if rows:
            # Include up to ``max_entries`` rows
            for row in rows[:max_entries]:
                try:
                    # Use a compact one‑line JSON representation for each row.
                    row_json = json.dumps(row, ensure_ascii=False)
                except Exception:
                    row_json = json.dumps(str(row), ensure_ascii=False)
                lines.append(row_json)
        else:
            # No data available; indicate emptiness
            lines.append("[...]")
        # Add a blank line after each object's rows for readability
        lines.append("")
    # Remove the trailing blank line for a cleaner output
    if lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines)

# -----------------------------------------------------------------------------
# Data preview helper (for raw JSON area on run actions)
# -----------------------------------------------------------------------------
def fetch_preview_rows(server: str, database: str, table: str, preview_rows: int, arraysize: int) -> List[Dict[str, Any]]:
    """
    Fetch a limited number of rows from a table for preview purposes.

    The function opens a SQL connection, sets the fetch batch size (`arraysize`)
    and returns up to `preview_rows` rows as a list of dictionaries.
    """
    preview_rows = max(0, int(preview_rows or 0))
    if preview_rows == 0:
        return []
    with open_sql_conn(server, database) as conn:
        cur = conn.cursor()
        cur.arraysize = max(1, int(arraysize))
        # Clear any prior row limits and issue a bounded preview query
        try:
            cur.execute("SET NOCOUNT ON; SET ROWCOUNT 0;")
        except Exception:
            try:
                cur.execute("SET NOCOUNT ON;")
                cur.execute("SET ROWCOUNT 0;")
            except Exception:
                pass
        cur.execute(f"SELECT TOP {preview_rows} * FROM {table}")
        cols = [c[0] for c in cur.description]
        out = []
        remaining = preview_rows
        while remaining > 0:
            batch = cur.fetchmany(min(cur.arraysize, remaining))
            if not batch:
                break
            for row in batch:
                out.append({cols[i]: row[i] for i in range(len(cols))})
            remaining -= len(batch)
        return out

# -----------------------------------------------------------------------------
# Multi‑table actions (diagnose, run CSV/Mongo/Both)
# -----------------------------------------------------------------------------
def _resolve_tables(selected: List[str], fallback_table: str) -> List[str]:
    """
    Determine the list of tables to operate on based on the selected checkboxes
    and an optional fallback table text.  Raises a Gradio error if no
    selection is available.
    """
    tables = selected or ([fallback_table] if fallback_table else [])
    if not tables:
        raise gr.Error("Select at least one table from discovery or provide a table in the text field.")
    return tables

def diagnose_selected(api_key: str, server: str, databases: List[str], selected_tables: List[str], table_text: str):
    """
    Diagnose selected objects across one or more databases.

    When tables are selected via the discovery checkboxes their values are
    expected to include a database prefix separated by a pipe ("|") as
    produced by list_objects_action() (e.g. "mydb|dbo.table"). For
    fallback manual entry (table_text) a single database must be selected.

    The function iterates over the resolved table identifiers, opens a fresh
    connection for each database and gathers metadata via db_object_info()
    together with the row count via db_count_rows(). A Markdown
    summary and pretty-printed JSON diagnostics are returned.
    """
    _require_key(api_key)
    # Normalize the databases input to a list
    if isinstance(databases, str):
        db_list = [databases]
    else:
        db_list = databases or []

    # Determine which table identifiers to operate on
    table_ids: List[str] = []
    if selected_tables:
        table_ids = list(selected_tables)
    elif table_text and table_text.strip():
        # Manual override requires exactly one database to be selected
        if len(db_list) != 1:
            raise gr.Error("Manual table entry requires exactly one selected database.")
        table_ids = [f"{db_list[0]}|{table_text.strip()}"]
    else:
        raise gr.Error("Select at least one object from discovery or provide a table in the text field.")

    all_infos: List[Dict[str, Any]] = []
    previews: List[Dict[str, Any]] = []
    for tid in table_ids:
        # Expect format "db|schema.table" or just "schema.table" if no prefix
        if "|" in tid:
            db, tbl = tid.split("|", 1)
        else:
            # If no prefix provided fallback to single selected database
            if len(db_list) == 1:
                db, tbl = db_list[0], tid
            else:
                raise gr.Error("Ambiguous table identifier: no database prefix and multiple databases selected.")
        try:
            with open_sql_conn(server, db) as conn:
                info = db_object_info(conn, tbl)
                info["available_rows"] = db_count_rows(conn, tbl)
                info["database"] = db
                info["qualified_name_db"] = tid
                all_infos.append(info)
                # Fetch up to 10 preview rows for the object for display in the raw data preview
                try:
                    preview_rows = fetch_preview_rows(server, db, tbl, 10, 10000)
                except Exception:
                    preview_rows = []
                previews.append({
                    "database": db,
                    "table": tbl,
                    "rows": preview_rows,
                    "rows_returned": len(preview_rows),
                })
        except Exception as ex:
            # Capture connection errors as part of the diagnostics
            all_infos.append({
                "input": tbl,
                "schema": None,
                "object": tbl,
                "type_desc": None,
                "is_synonym": False,
                "synonym_base_object": None,
                "view_definition": None,
                "view_has_where_or_distinct_or_top": None,
                "rls_policies": [],
                "available_rows": None,
                "database": db,
                "qualified_name_db": tid,
                "error": str(ex),
            })
            previews.append({
                "database": db,
                "table": tbl,
                "rows": [],
                "rows_returned": 0,
                "error": str(ex),
            })

    # Build human summary
    lines = ["## Diagnose (selected)"]
    for i in all_infos:
        td = i.get("type_desc") or "UNKNOWN"
        obj_repr = f"{i.get('schema')}.{i.get('object')}" if i.get('schema') else i.get('object')
        lines.append(f"- `{i.get('database')}|{obj_repr}` — **{td}**; available: {_fmt_int(i.get('available_rows'))}")
        if i.get("is_synonym"):
            lines.append(f"  - Synonym → `{i.get('synonym_base_object')}`")
        if i.get("view_has_where_or_distinct_or_top") is True:
            lines.append("  - View has WHERE/DISTINCT/TOP")
        if i.get("rls_policies"):
            lines.append(f"  - RLS policies: {len(i['rls_policies'])}")
        if i.get("error"):
            lines.append(f"  - Error: {i['error']}")
    # For diagnosis we now show sample row data in the raw preview area, grouped by object.
    raw_preview = format_raw_data_preview(previews, max_objects=30, max_entries=10)
    return "\n".join(lines), raw_preview

def run_csv_selected(api_key: str, server: str, databases: List[str], selected_tables: List[str], table_text: str, rows: int, sql_arraysize: int, csv_root: str, preview_rows: int):
    """
    Run CSV export for one or more objects across one or more databases.

    Objects selected via the discovery checkboxes carry a database prefix
    separated by a pipe character (``db|schema.table``).  Manual fallback
    input requires exactly one selected database.  Each (database, table)
    combination is exported separately and the results are accumulated.
    """
    _require_key(api_key)
    # Normalize the databases input to a list
    if isinstance(databases, str):
        db_list = [databases]
    else:
        db_list = databases or []
    table_ids = []
    if selected_tables:
        table_ids = list(selected_tables)
    elif table_text and table_text.strip():
        # Manual override requires exactly one database
        if len(db_list) != 1:
            raise gr.Error("Manual table entry requires exactly one selected database.")
        table_ids = [f"{db_list[0]}|{table_text.strip()}"]
    else:
        raise gr.Error("Select at least one object from discovery or provide a table in the text field.")
    # Parse the rows parameter robustly.  Gradio's Number input may return
    # integers, floats or strings with comma separators.  To honour large
    # limits (e.g. "9,000,000"), normalise the value before converting to
    # an integer.  Default to 0 if parsing fails.
    try:
        rows_limit = int(float(str(rows).replace(',', '')))
    except Exception:
       rows_limit = int(rows) if isinstance(rows, int) else 0
    results = []
    previews = []
    for tid in table_ids:
        if "|" in tid:
            db, tbl = tid.split("|", 1)
        else:
            if len(db_list) == 1:
                db, tbl = db_list[0], tid
            else:
                raise gr.Error("Ambiguous table identifier: no database prefix and multiple databases selected.")
        # lways capture a preview *after* running the export.  Some users
        # reported that fetching the preview before the export resulted in
        # empty results, whereas the diagnostic preview showed rows.  To
        # ensure consistency with the Diagnose behaviour, we now fetch the
        # preview after the export completes.  We still guard against
        # exceptions so that the overall run continues for other tables.
        preview_data = []
        error_msg: Optional[str] = None
        try:
            # The benchmark function returns a result dictionary even on partial
            # failures (with an "error" key when appropriate).  Wrap in dict() to
            # avoid attribute errors if a namedtuple is returned.
            res_dict = dict(benchmark_sql_to_csv(
                server,
                db,
                tbl,
                rows_limit,
                int(sql_arraysize),
                csv_root,
                progress=gr.Progress(track_tqdm=False),
                multi=len(table_ids) > 1,
            ))
            # Prefix the table with its database for consistent display
            res_dict["table"] = f"{db}|{tbl}"
            # Capture any error message for the preview entry
            error_msg = res_dict.get("error")
            results.append(res_dict)
        except Exception as ex:
            # On unexpected exceptions (e.g. connection failure), record a minimal
            # error result.  rows_actual remains 0 in this scenario.
            error_msg = str(ex)
            results.append({
                "strategy": "SQL -> CSV",
                "table": f"{db}|{tbl}",
                "available_rows": None,
                "rows_target": rows_limit,
                "rows_actual": 0,
                "elapsed_sec": 0,
                "throughput_rows_per_sec": None,
                "output": None,
                "file_size_bytes": 0,
                "error": error_msg,
            })
        # Always fetch a preview after export.  Use the provided preview_rows if
        # set and greater than zero, otherwise default to 10.  The error
        # message captured above (if any) is attached tothe preview entry.
        num_preview = int(preview_rows) if (preview_rows and int(preview_rows) > 0) else 10
        try:
            preview_data = fetch_preview_rows(server, db, tbl, num_preview, int(sql_arraysize))
        except Exception:
            preview_data = []
        entry = {
            "database": db,
            "table": tbl,
            "rows": preview_data if preview_data else [],
            "rows_returned": len(preview_data) if preview_data else 0,
        }
        if error_msg:
            entry["error"] = error_msg
        previews.append(entry)
    # Build the raw data preview: group by object and include up to 10 rows per object
    raw_preview = format_raw_data_preview(previews, max_objects=30, max_entries=10)
    return summarize(results), raw_preview

def run_mongo_selected(
    api_key: str,
    server: str,
    databases: List[str],
    selected_tables: List[str],
    table_text: str,
    rows: int,
    sql_arraysize: int,
    mongo_uri: str,
    mongo_db: str,
    mongo_coll_ignored: str,
    mongo_batch: int,
    drop_before: bool,
    write_concern_w: int,
    preview_rows: int,
):
    """
    Run MongoDB export for one or more objects across one or more databases and return summary and preview.

    Objects selected via the discovery checkboxes must include a database prefix.
    Manual fallback input requires exactly one selected database.  The target
    MongoDB database name (``mongo_db``) is reused for all tables; the base
    collection parameter is ignored and derived from the table name.  Each
    (database, table) combination is exported separately.
    """
    _require_key(api_key)
    # Normalize database list
    db_list = [databases] if isinstance(databases, str) else (databases or [])
    # Determine table identifiers
    if selected_tables:
        table_ids = list(selected_tables)
    elif table_text and table_text.strip():
        if len(db_list) != 1:
            raise gr.Error("Manual table entry requires exactly one selected database.")
        table_ids = [f"{db_list[0]}|{table_text.strip()}"]
    else:
        raise gr.Error("Select at least one object from discovery or provide a table in the text field.")
    # Parse rows limit robustly
    try:
        rows_limit = int(float(str(rows).replace(',', '')))
    except Exception:
        rows_limit = int(rows) if isinstance(rows, int) else 0

    results: List[Dict[str, Any]] = []
    previews: List[Dict[str, Any]] = []
    prev_fast = os.environ.get("FDI_FAST_FETCH")
    try:
        for tid in table_ids:
            if "|" in tid:
                db, tbl = tid.split("|", 1)
            else:
                if len(db_list) == 1:
                    db, tbl = db_list[0], tid
                else:
                    raise gr.Error("Ambiguous table identifier: no database prefix and multiple databases selected.")
            error_msg: Optional[str] = None
            try:
                os.environ["FDI_FAST_FETCH"] = "1"
                arraysize_eff = int(rows_limit) if (rows_limit and rows_limit > 0) else int(sql_arraysize)
                res_dict = dict(
                    benchmark_sql_to_mongo(
                        server,
                        db,
                        tbl,
                        rows_limit,
                        int(arraysize_eff),
                        mongo_uri,
                        mongo_db,
                        mongo_coll_ignored,
                        int(mongo_batch),
                        bool(drop_before),
                        int(write_concern_w),
                        progress=gr.Progress(track_tqdm=False),
                        multi=len(table_ids) > 1,
                    )
                )
                res_dict["table"] = f"{db}|{tbl}"
                error_msg = res_dict.get("error")
                results.append(res_dict)
            except Exception as ex:
                error_msg = str(ex)
                results.append(
                    {
                        "strategy": "SQL -> MongoDB",
                        "table": f"{db}|{tbl}",
                        "available_rows": None,
                        "rows_target": rows_limit,
                        "rows_actual": 0,
                        "elapsed_sec": 0,
                        "throughput_rows_per_sec": None,
                        "output": None,
                        "file_size_bytes": 0,
                        "error": error_msg,
                    }
                )
            # Preview after export
            num_preview = int(preview_rows) if (preview_rows and int(preview_rows) > 0) else 10
            try:
                preview_data = fetch_preview_rows(server, db, tbl, num_preview, int(sql_arraysize))
            except Exception:
                preview_data = []
            entry: Dict[str, Any] = {
                "database": db,
                "table": tbl,
                "rows": preview_data if preview_data else [],
                "rows_returned": len(preview_data) if preview_data else 0,
            }
            if error_msg:
                entry["error"] = error_msg
            previews.append(entry)
    finally:
        # Restore previous fetch mode if altered
        try:
            if prev_fast is None:
                os.environ.pop("FDI_FAST_FETCH", None)
            else:
                os.environ["FDI_FAST_FETCH"] = prev_fast
        except Exception:
            pass
    # Build preview and summary
    raw_preview = format_raw_data_preview(previews, max_objects=30, max_entries=10)
    return summarize(results), raw_preview

def run_both_selected(api_key: str, server: str, databases: List[str], selected_tables: List[str], table_text: str, rows: int, sql_arraysize: int, csv_root: str, mongo_uri: str, mongo_db: str, mongo_coll_ignored: str, mongo_batch: int, drop_before: bool, write_concern_w: int, preview_rows: int):
    """
    Run both CSV and MongoDB exports for one or more objects across one or more databases and return summary and preview.

    Objects selected via the discovery checkboxes must include a database prefix.
    Manual fallback input requires exactly one selected database.  Each
    (database, table) combination is exported to both CSV and MongoDB.
    """
    _require_key(api_key)
    if isinstance(databases, str):
        db_list = [databases]
    else:
        db_list = databases or []
    # Determine table identifiers
    table_ids = []
    if selected_tables:
        table_ids = list(selected_tables)
    elif table_text and table_text.strip():
        if len(db_list) != 1:
            raise gr.Error("Manual table entry requires exactly one selected database.")
        table_ids = [f"{db_list[0]}|{table_text.strip()}"]
    else:
        raise gr.Error("Select at least one object from discovery or provide a table in the text field.")

    # Normalise the rows parameter (max rows) to an integer.  Gradio may
    # return strings with commas or floats.  Remove commas and cast to int.
    try:
        rows_limit = int(float(str(rows).replace(',', '')))
    except Exception:
        rows_limit = int(rows) if isinstance(rows, int) else 0
    results = []
    previews = []
    for tid in table_ids:
        if "|" in tid:
            db, tbl = tid.split("|", 1)
        else:
            if len(db_list) == 1:
                db, tbl = db_list[0], tid
            else:
                raise gr.Error("Ambiguous table identifier: no database prefix and multiple databases selected.")
        # Always fetch a preview after performing both exports.  Fetching
        # previews prior to export sometimes produced empty results.  We
        # default to 10 rows when preview_rows is zero or unset.  Any
        # exception thrown by either export is captured and added as an error
        # message on the preview entry.  Both export strategies are always
        # recorded separately in the summary.
        error_msg: Optional[str] = None
        try:
            # Perform CSV export; capture any error inside benchmark
            res_csv = dict(benchmark_sql_to_csv(
                server,
                db,
                tbl,
                rows_limit,
                int(sql_arraysize),
                csv_root,
                progress=gr.Progress(track_tqdm=False),
                multi=len(table_ids) > 1,
            ))
            res_csv["table"] = f"{db}|{tbl}"
            # Perform MongoDB export; capture any error inside benchmark
            res_mongo = dict(benchmark_sql_to_mongo(
                server,
                db,
                tbl,
                rows_limit,
                int(sql_arraysize),
                mongo_uri,
                mongo_db,
                mongo_coll_ignored,
                int(mongo_batch),
                bool(drop_before),
                int(write_concern_w),
                progress=gr.Progress(track_tqdm=False),
                multi=len(table_ids) > 1,
            ))
            res_mongo["table"] = f"{db}|{tbl}"
            # Capture any error from either export
            error_msg = res_csv.get("error") or res_mongo.get("error")
            results.extend([res_csv, res_mongo])
        except Exception as ex:
            # On unexpected exceptions, record both strategies as errors
            error_msg = str(ex)
            results.append({
                "strategy": "SQL -> CSV",
                "table": f"{db}|{tbl}",
                "available_rows": None,
                "rows_target": rows_limit,
                "rows_actual": 0,
                "elapsed_sec": 0,
                "throughput_rows_per_sec": None,
                "output": None,
                "file_size_bytes": 0,
                "error": error_msg,
            })
            results.append({
                "strategy": "SQL -> MongoDB",
                "table": f"{db}|{tbl}",
                "available_rows": None,
                "rows_target": rows_limit,
                "rows_actual": 0,
                "elapsed_sec": 0,
                "throughput_rows_per_sec": None,
                "output": None,
                "file_size_bytes": 0,
                "error": error_msg,
            })
        # Always fetch preview rows after both exports
        try:
            num_preview = int(preview_rows) if (preview_rows and int(preview_rows) > 0) else 10
        except Exception:
            num_preview = 10
        try:
            preview_data = fetch_preview_rows(server, db, tbl, num_preview, int(sql_arraysize))
        except Exception:
            preview_data = []
        entry = {
            "database": db,
            "table": tbl,
            "rows": preview_data if preview_data else [],
            "rows_returned": len(preview_data) if preview_data else 0,
        }
        if error_msg:
            entry["error"] = error_msg
        previews.append(entry)
    # Build the raw data preview: group by object and include up to 10 rows per object
    raw_preview = format_raw_data_preview(previews, max_objects=30, max_entries=10)
    return summarize(results, header="Combined Summary (CSV & Mongo)"), raw_preview

# -----------------------------------------------------------------------------
# Table discovery actions (CheckboxGroup + toggle)
# -----------------------------------------------------------------------------
def list_objects_action(api_key: str, server: str, databases: List[str], name_filter: str, include_views: bool, include_synonyms: bool, max_items: Optional[int]):
    """
    Gradio event handler to list objects across one or more databases and update UI.

    The incoming ``databases`` parameter is a list of selected database names.
    For each database the helper ``list_objects()`` is invoked to enumerate
    user tables, views and synonyms that match the provided filters.  The
    resulting objects are augmented with their originating database and the
    database name is prefixed to the ``qualified_name`` using a pipe (``|``)
    separator.  This encoded name allows downstream actions to determine
    which database a selected object belongs to.  The combined list of
    objects is pretty‑printed as JSON for reference and the ``CheckboxGroup``
    used to select tables is updated accordingly.
    """
    _require_key(api_key)
    # Accumulate rows across multiple filter terms.  The `name_filter` input
    # may contain a comma‑delimited list of patterns (e.g. "orders,customer").
    # Split on commas and strip whitespace; ignore empty entries.  When no
    # filter is provided (empty string), use a single None entry to fetch all
    # objects.  Duplicates arising from overlapping patterns are removed via
    # a set on the encoded ``qualified_name_db`` key.
    all_rows = []
    seen: Set[str] = set()
    # Normalize the incoming list (Gradio may supply None or a single string)
    if isinstance(databases, str):
        db_list = [databases]
    else:
        db_list = databases or []
    # Parse the filter into individual patterns
    # patterns will be a list of optional strings derived below
    if name_filter and isinstance(name_filter, str):
        parts = [p.strip() for p in name_filter.split(',') if p.strip()]
        patterns = parts if parts else [None]
    else:
        patterns = [None]
    for db in db_list:
        try:
           with open_sql_conn(server, db) as conn:
                for patt in patterns:
                    # For each filter pattern, call list_objects.  When ``patt`` is None,
                    # it means no filter (list all).  Duplicate objects are skipped.
                    rows = list_objects(
                        conn,
                        patt,
                        bool(include_views),
                       bool(include_synonyms),
                        None if max_items is None else int(max_items),
                    )
                    for r in rows:
                        r = dict(r)  # shallow copy to avoid mutating original
                        r['database'] = db
                        qname = f"{db}|{r['qualified_name']}"
                        if qname in seen:
                            continue
                        seen.add(qname)
                        r['qualified_name_db'] = qname
                        all_rows.append(r)
        except Exception:
            # In case a particular database cannot be accessed, record a stub entry
            stub_key = f"{db}|<error>"
            if stub_key not in seen:
                seen.add(stub_key)
                all_rows.append({
                    'schema': None,
                    'object': None,
                    'type_desc': None,
                    'qualified_name': None,
                    'synonym_base_object': None,
                    'database': db,
                    'qualified_name_db': stub_key,
                })
    choices = [r['qualified_name_db'] for r in all_rows if r.get('qualified_name_db')]
    # Build summary without adding another Raw Data Preview header. The web UI
    # already contains a dedicated raw preview area, so including an additional
    # "Raw list" heading in the summary would duplicate the label. See
    # PR/bugfix for details.
    summary = (
        "## Object list\n"
        + "Found **{}** object(s) across **{}** database(s). ".format(len(all_rows), len(db_list))
        + "Use the checkboxes below; **Select/Deselect All** toggles everything.\n\n"
        + "Tip: set **Max items = 0** to remove the TOP cap (can be heavy!)."
    )
    return summary, pretty_json(all_rows), gr.CheckboxGroup(choices=choices, value=[]), choices

def toggle_select_all(current_selection: List[str], choices: List[str]):
    """
    Toggle selection state for all objects in the CheckboxGroup.  If not all
    objects are currently selected, return a selection of all; oherwise
    deselect everything.
    """
    choice_list = choices or []
    sel_set = set(current_selection or [])
    if len(sel_set) != len(choice_list):
        return gr.CheckboxGroup(value=choice_list)
    return gr.CheckboxGroup(value=[])

# -----------------------------------------------------------------------------
# New: Mongo → CSV benchmark, Mongo preview, and combined run flow
# -----------------------------------------------------------------------------
def benchmark_mongo_to_csv(
    mongo_uri: str,
    mongo_db: str,
    database: str,
    table: str,
    limit_rows: int,
    csv_root: str,
    progress: Optional[gr.Progress] = None,
    multi: bool = False,
) -> Dict[str, Any]:
    """
    Export rows from a MongoDB collection to a CSV file and report metrics.

    The target collection name is derived from the fully-qualified table name by
    replacing dots with underscores. The CSV path mirrors the SQL variant using
    the original table name for directory structure and filename.
    """
    t0 = time.perf_counter()
    rows_written = 0
    error_message: Optional[str] = None
    file_path: Optional[str] = None
    coll_name = _mongo_safe_coll_name(f"{database}.{table}")
    available: Optional[int] = None
    try:
        file_path = os.path.abspath(_resolve_csv_path(csv_root, database, table))
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        client = MongoClient(
            sanitize_mongo_uri(mongo_uri),
            uuidRepresentation="standard",
            connectTimeoutMS=15_000,
            serverSelectionTimeoutMS=15_000,
        )
        db = client[mongo_db]
        coll = db.get_collection(coll_name)
        try:
            available = coll.estimated_document_count()
        except Exception:
            available = None
        lim = int(limit_rows) if int(limit_rows) > 0 else 0
        # Global CSV write chunk size: 10K for steady progress and memory bounds
        GLOBAL_SLICE = 500_000
        write_chunk = 10_000
        if lim > 0:
            write_chunk = max(1_000, min(write_chunk, lim))
        # Align cursor batch size similarly but do not exceed 10K
        cursor = coll.find({}, no_cursor_timeout=True).batch_size(write_chunk)
        first_chunk = True
        buffer = []
        remaining = lim if lim > 0 else None
        cols = None
        last_write_t = time.perf_counter()
        for doc in cursor:
            buffer.append(doc)
            # Decrement remaining counter if we have a max rows cap
            if remaining is not None:
                remaining -= 1
            if len(buffer) >= write_chunk:
                # Convert and write chunk
                t_chunk = time.perf_counter()
                df = pd.DataFrame([{k: _to_bson_safe(v) for k, v in d.items()} for d in buffer])
                if cols is None:
                    cols = list(df.columns)
                else:
                    df = df.reindex(columns=cols, fill_value="")
                df.to_csv(
                    file_path,
                    mode='w' if first_chunk else 'a',
                    header=first_chunk,
                    index=False,
                    encoding='utf-8',
                    lineterminator='\n',
                )
                rows_written += len(df)
                first_chunk = False
                buffer = []
                # Opportunistic GC to reduce memory peaks
                try:
                    import gc as _gc
                    _gc.collect()
                except Exception:
                    pass
                if progress and lim:
                    dt_ms = int((time.perf_counter() - t_chunk) * 1000)
                    progress(
                        min(1.0, rows_written / max(1, lim)),
                        desc=f"[{table}] CSV: {rows_written:,}/{lim:,}",
                        count=int(rows_written),
                        total=int(lim),
                        chunk=int(len(df)),
                        dt_ms=dt_ms,
                    )
            # Stop once we've reached the exact limit
            if remaining == 0:
                break
        # flush remaining
        if buffer:
            t_chunk = time.perf_counter()
            df = pd.DataFrame([{k: _to_bson_safe(v) for k, v in d.items()} for d in buffer])
            if cols is None:
                cols = list(df.columns)
            else:
                df = df.reindex(columns=cols, fill_value="")
            df.to_csv(
                file_path,
                mode='w' if first_chunk else 'a',
                header=first_chunk,
                index=False,
                encoding='utf-8',
                lineterminator='\n',
            )
            rows_written += len(df)
            if progress and lim:
                dt_ms = int((time.perf_counter() - t_chunk) * 1000)
                progress(
                    min(1.0, rows_written / max(1, lim)),
                    desc=f"[{table}] CSV: {rows_written:,}/{lim:,}",
                    count=int(rows_written),
                    total=int(lim),
                    chunk=int(len(df)),
                    dt_ms=dt_ms,
                )
    except Exception as exc:
        error_message = str(exc)
    elapsed = time.perf_counter() - t0
    size_bytes = os.path.getsize(file_path) if (file_path and os.path.exists(file_path)) else 0
    result: Dict[str, Any] = {
        "strategy": "MongoDB -> CSV",
        "available_rows": available,
        "rows_target": int(limit_rows),
        "rows_actual": int(rows_written),
        "elapsed_sec": round(elapsed, 3),
        "throughput_rows_per_sec": round(rows_written / elapsed, 2) if elapsed > 0 else None,
        "output": file_path,
        "file_size_bytes": int(size_bytes),
        "table": table,
    }
    if error_message:
        result["error"] = error_message
    return result

def fetch_preview_mongo_rows(mongo_uri: str, mongo_db: str, database: str, table: str, preview_rows: int) -> List[Dict[str, Any]]:
    """
    Fetch a limited number of documents from MongoDB for preview purposes.
    Converts BSON types to JSON-safe values.
    """
    n = max(0, int(preview_rows or 0))
    if n == 0:
        return []
    client = MongoClient(
        sanitize_mongo_uri(mongo_uri),
        uuidRepresentation="standard",
        connectTimeoutMS=15_000,
        serverSelectionTimeoutMS=15_000,
    )
    coll = client[mongo_db].get_collection(_mongo_safe_coll_name(f"{database}.{table}"))
    cur = coll.find({}).limit(n).batch_size(min(10_000, max(1, n)))
    out = []
    for doc in cur:
        try:
            out.append({k: _to_bson_safe(v) for k, v in doc.items()})
        except Exception:
            out.append({"_error": "unserializable document"})
    return out

def run_mongo_then_csv_selected(
    api_key: str,
    server: str,
    databases: List[str],
    selected_tables: List[str],
    table_text: str,
    max_rows: int,
    sql_arraysize: int,
    csv_root: str,
    mongo_uri: str,
    mongo_db: str,
    drop_before: bool,
    write_concern_w: int,
    preview_rows: int,
):
    """
    Always persist data to MongoDB first, then generate CSV(s) from Mongo.

    Uses `max_rows` as the row cap for both operations. Returns a combined
    summary (with throughput for both strategies) and a raw preview of documents
    fetched from MongoDB.
    """
    _require_key(api_key)
    # Normalise databases
    db_list = [databases] if isinstance(databases, str) else (databases or [])
    # Determine tables
    table_ids = []
    if selected_tables:
        table_ids = list(selected_tables)
    elif table_text and table_text.strip():
        if len(db_list) != 1:
            raise gr.Error("Manual table entry requires exactly one selected database.")
        table_ids = [f"{db_list[0]}|{table_text.strip()}"]
    else:
        raise gr.Error("Select at least one object from discovery or provide a table in the text field.")
    # Parse max_rows robustly
    try:
        rows_limit = int(float(str(max_rows).replace(',', '')))
    except Exception:
        rows_limit = int(max_rows) if isinstance(max_rows, int) else 0
    results = []
    previews = []
    # Insert chunk size: default from env, but if max_rows (rows_limit) is provided,
    # use it as the batch cap so we never introduce a lower artificial cap.
    # Large values are safe because actual sub-chunks will be bounded by incoming
    # SQL fetch batches; we also split on errors.
    try:
        env_chunk = int(os.environ.get("FDI_MONGO_INSERT_CHUNK", "50000"))
    except Exception:
        env_chunk = 50000
    insert_chunk = max(1, int(rows_limit)) if (rows_limit and rows_limit > 0) else env_chunk
    # Capture original fast-fetch env once per invocation
    prev_fast = os.environ.get("FDI_FAST_FETCH")
    for tid in table_ids:
        if "|" in tid:
            db, tbl = tid.split("|", 1)
        else:
            if len(db_list) == 1:
                db, tbl = db_list[0], tid
            else:
                raise gr.Error("Ambiguous table identifier: no database prefix and multiple databases selected.")
        # Step 1: SQL -> MongoDB
        try:
            # Force fast batched fetching and align SQL arraysize to max_rows to avoid implicit caps
            os.environ["FDI_FAST_FETCH"] = "1"
            arraysize_eff = int(rows_limit) if (rows_limit and rows_limit > 0) else int(sql_arraysize)
            res_mongo = dict(benchmark_sql_to_mongo(
                server,
                db,
                tbl,
                rows_limit,
                int(arraysize_eff),
                mongo_uri,
                mongo_db,
                mongo_coll_ignored="ignored",
                mongo_batch=int(insert_chunk),
                drop_before=bool(drop_before),
                write_concern_w=int(write_concern_w),
                progress=gr.Progress(track_tqdm=False),
                multi=len(table_ids) > 1,
            ))
            res_mongo["table"] = f"{db}|{tbl}"
            results.append(res_mongo)
        except Exception as ex:
            results.append({
                "strategy": "SQL -> MongoDB",
                "table": f"{db}|{tbl}",
                "available_rows": None,
                "rows_target": rows_limit,
                "rows_actual": 0,
                "elapsed_sec": 0,
                "throughput_rows_per_sec": None,
                "output": None,
                "file_size_bytes": 0,
                "error": str(ex),
            })
        # Step 2: MongoDB -> CSV (use newly saved data)
        try:
            res_csv = dict(benchmark_mongo_to_csv(
                mongo_uri,
                mongo_db,
                db,
                tbl,
                rows_limit,
                csv_root,
                progress=gr.Progress(track_tqdm=False),
                multi=len(table_ids) > 1,
            ))
            res_csv["table"] = f"{db}|{tbl}"
            results.append(res_csv)
        except Exception as ex:
            results.append({
                "strategy": "MongoDB -> CSV",
                "table": f"{db}|{tbl}",
                "available_rows": None,
                "rows_target": rows_limit,
                "rows_actual": 0,
                "elapsed_sec": 0,
                "throughput_rows_per_sec": None,
                "output": None,
                "file_size_bytes": 0,
                "error": str(ex),
        })
        # Preview from MongoDB to reflect newly stored data
        num_preview = int(preview_rows) if (preview_rows and int(preview_rows) > 0) else 10
        try:
            preview_data = fetch_preview_mongo_rows(mongo_uri, mongo_db, db, tbl, num_preview)
        except Exception:
            preview_data = []
        previews.append({
            "database": db,
            "table": tbl,
            "rows": preview_data if preview_data else [],
            "rows_returned": len(preview_data) if preview_data else 0,
        })
    raw_preview = format_raw_data_preview(previews, max_objects=30, max_entries=10)
    # Restore previous fetch mode if altered
    try:
        if prev_fast is None:
            os.environ.pop("FDI_FAST_FETCH", None)
        else:
            os.environ["FDI_FAST_FETCH"] = prev_fast
    except Exception:
        pass
    return summarize(results), raw_preview
