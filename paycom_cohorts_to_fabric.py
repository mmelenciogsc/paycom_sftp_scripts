#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import shutil
import re
import struct
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List

import paramiko


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = ""
DEFAULT_WATCH_DIR = Path("")
DEFAULT_GLOB = "*Cohorts*PaycomID*.csv"
SFTP_HOST = os.environ.get("PAYCOM_SFTP_HOST", "push.paycomonline.net")
SFTP_PORT = int(os.environ.get("PAYCOM_SFTP_PORT", "22"))
SFTP_REMOTE_DIR = os.environ.get("PAYCOM_SFTP_REMOTE_DIR", "/Outbound")
DEFAULT_SERVER = os.environ.get(
    "PAYCOM_COHORTS_FABRIC_SERVER",
    os.environ.get(
        "PAYCOM_FABRIC_SERVER",
        "qot6olhksufuxlapzmfsu3zsci-fivilhqesnzepkg2twbufnheqy.datawarehouse.fabric.microsoft.com",
    ),
)
DEFAULT_DB = os.environ.get("PAYCOM_COHORTS_FABRIC_DATABASE", "tm_charteasy")
DEFAULT_SCHEMA = os.environ.get("PAYCOM_COHORTS_FABRIC_SCHEMA", "dbo")
DEFAULT_TABLE = os.environ.get("PAYCOM_COHORTS_FABRIC_TABLE", "paycom_cohorts_bronze")
DEFAULT_DRIVER = os.environ.get("PAYCOM_FABRIC_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
FETCH_BATCH = 1000
MAX_COL_LEN = 128
SQL_SCOPE = "https://database.windows.net/.default"
RUNTIME_ROOT = Path(os.environ.get("PAYCOM_COHORTS_RUNTIME_ROOT", str(BASE_DIR / "runtime_cohorts"))).expanduser()
LOG_DIR = RUNTIME_ROOT / "logs" / "raw"
STATE_DIR = RUNTIME_ROOT / "state"
TMP_DIR = RUNTIME_ROOT / "tmp" / "paycom_cohorts_bronze"
LOG_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_STATE_FILE = STATE_DIR / "create_paycom_cohorts_bronze_fabric_table_state.json"


def _meta_columns() -> List[str]:
    return [
        "csv_filename",
        "source_file_path",
        "source_file_timestamp",
        "loaded_at_utc",
        "updatedAt",
        "row_number",
        "payload_json",
    ]


def _require_pyodbc():
    try:
        import pyodbc  # type: ignore
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Missing dependency: pyodbc is not installed in this Python environment."
        ) from exc
    return pyodbc


def _require_azure_identity():
    try:
        from azure.identity import (  # type: ignore
            AzureCliCredential,
            DefaultAzureCredential,
            EnvironmentCredential,
            ManagedIdentityCredential,
        )
    except ModuleNotFoundError as exc:
        raise SystemExit(
            "Missing dependency: azure-identity is not installed in this Python environment."
        ) from exc
    return AzureCliCredential, ManagedIdentityCredential, EnvironmentCredential, DefaultAzureCredential


def newest_sql_driver(default: str = DEFAULT_DRIVER) -> str:
    pyodbc = _require_pyodbc()
    try:
        drivers = [d for d in pyodbc.drivers() if d.endswith("for SQL Server")]
        return sorted(drivers)[-1] if drivers else default
    except Exception:
        return default


def get_azure_token_attr() -> dict[int, bytes]:
    creds = _require_azure_identity()
    for cred_type in creds:
        try:
            token = cred_type().get_token(SQL_SCOPE).token
            token_bytes = token.encode("UTF-16-LE")
            packed = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
            return {1256: packed}
        except Exception:
            continue
    raise SystemExit(
        "Azure AD authentication failed. Run 'az login', use a managed identity, "
        "or set Azure environment credentials for this shell."
    )


def open_sql_conn(server: str, database: str, driver: str | None = None):
    pyodbc = _require_pyodbc()
    drv = driver or newest_sql_driver()
    conn_str = (
        f"DRIVER={{{drv}}};SERVER={server};PORT=1433;DATABASE={database};"
        "Encrypt=yes;TrustServerCertificate=yes"
    )
    return pyodbc.connect(conn_str, attrs_before=get_azure_token_attr(), autocommit=True)


def _setup_logging() -> Path:
    log_path = LOG_DIR / f"masterlog_{time.strftime('%Y%m%d_%H%M%S')}.txt"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )
    return log_path


def _clean_value(val: object) -> str:
    text = "" if val is None else str(val)
    text = text.replace("\x00", "").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    return " ".join(text.split()).strip()


def _normalize_value(val: object) -> str | None:
    cleaned = _clean_value(val)
    return cleaned if cleaned else None


def _env_sftp_creds() -> tuple[str, str]:
    values: Dict[str, str] = {}
    credential_file = (os.environ.get("SFTP_CREDENTIAL_FILE") or "").strip()
    if credential_file:
        path = Path(credential_file).expanduser()
        if path.is_file():
            for line in path.read_text(encoding="utf-8").splitlines():
                if "=" in line and not line.lstrip().startswith("#"):
                    key, value = line.split("=", 1)
                    values[key.strip()] = value.strip().strip("\"'")
    user = (os.environ.get("SFTP_UN") or values.get("SFTP_UN") or "").strip()
    pw = os.environ.get("SFTP_PW") or values.get("SFTP_PW") or ""
    return user, pw


def _resolve_sftp_creds(user_override: str = "", pw_override: str = "") -> tuple[str, str]:
    user = (user_override or "").strip()
    pw = (pw_override or "").strip()
    if user and pw:
        return user, pw
    env_user, env_pw = _env_sftp_creds()
    if env_user and env_pw:
        return env_user, env_pw
    raise SystemExit(
        "Missing SFTP credentials. Set SFTP_UN / SFTP_PW in the environment "
        "or pass --sftp-un and --sftp-pw."
    )


@contextmanager
def _open_sftp(user_override: str = "", pw_override: str = ""):
    username, password = _resolve_sftp_creds(user_override, pw_override)
    client = paramiko.SSHClient()
    known_hosts_path = (
        os.environ.get("SFTP_KNOWN_HOSTS_PATH")
        or str(BASE_DIR / "paycom_known_hosts")
    ).strip()
    if not known_hosts_path or not Path(known_hosts_path).is_file():
        raise SystemExit(
            "Missing verified SFTP host-key file. Set SFTP_KNOWN_HOSTS_PATH "
            "to a protected known_hosts file before starting the worker."
        )
    client.load_host_keys(known_hosts_path)
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(SFTP_HOST, port=SFTP_PORT, username=username, password=password, timeout=30)
    sftp = client.open_sftp()
    try:
        yield client, sftp
    finally:
        try:
            sftp.close()
        except Exception:
            pass
        try:
            client.close()
        except Exception:
            pass


def _sanitize_colname(name: str) -> str:
    raw = (name or "").strip()
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in raw)
    cleaned = "_".join(part for part in cleaned.split("_") if part)
    if not cleaned:
        cleaned = "col"
    if cleaned[0].isdigit():
        cleaned = f"c_{cleaned}"
    return cleaned[:MAX_COL_LEN]


def _dedupe_columns(cols: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    for col in cols:
        key = col.lower()
        if key not in seen:
            seen[key] = 0
            out.append(col)
            continue
        seen[key] += 1
        suffix = seen[key]
        base = col[: MAX_COL_LEN - (len(str(suffix)) + 1)]
        new_col = f"{base}_{suffix}"
        while new_col.lower() in seen:
            suffix += 1
            base = col[: MAX_COL_LEN - (len(str(suffix)) + 1)]
            new_col = f"{base}_{suffix}"
        out.append(new_col)
        seen[new_col.lower()] = 0
    return out


def _qualified_table(schema: str, table: str) -> str:
    return f"[{schema}].[{table}]"


def _parse_filename_timestamp(path: Path) -> datetime | None:
    match = re.match(r"^(\d{14})_", path.name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
    except Exception:
        return None


def _load_rows(input_path: Path, limit: int = 0) -> tuple[List[str], List[dict], datetime | None]:
    if not input_path.exists():
        raise SystemExit(f"Input CSV not found: {input_path}")
    rows: List[dict] = []
    source_ts = _parse_filename_timestamp(input_path)
    with input_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        for idx, row in enumerate(reader, start=1):
            normalized = {k: _normalize_value(v) for k, v in row.items()}
            rows.append(normalized)
            if limit > 0 and idx >= limit:
                break
    return headers, rows, source_ts


def _build_table_columns(raw_headers: List[str]) -> tuple[List[str], Dict[str, str]]:
    sanitized = _dedupe_columns([_sanitize_colname(h) for h in raw_headers])
    col_map = {raw: san for raw, san in zip(raw_headers, sanitized)}
    meta_cols = _meta_columns()
    all_cols = meta_cols + sanitized
    return all_cols, col_map


def _column_sql_type(col: str) -> str:
    if col in {"source_file_timestamp", "loaded_at_utc", "updatedAt"}:
        return "DATETIME2(6) NULL"
    if col == "row_number":
        return "INT NOT NULL"
    return "VARCHAR(MAX) NULL"


def _fetch_existing_columns(conn, schema: str, table: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.name
        FROM sys.columns c
        JOIN sys.tables t ON t.object_id = c.object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = ? AND t.name = ?
        """,
        schema,
        table,
    )
    return {str(row[0]).lower() for row in cur.fetchall()}


def _ensure_table(conn, schema: str, table: str, columns: List[str], recreate: bool = False) -> None:
    qualified = _qualified_table(schema, table)
    col_defs = [f"[{col}] {_column_sql_type(col)}" for col in columns]
    cur = conn.cursor()
    cur.execute(f"SELECT OBJECT_ID('{schema}.{table}', 'U')")
    existing = cur.fetchone()
    exists = bool(existing and existing[0] is not None)

    if exists and recreate:
        logging.info("Dropping existing table %s", qualified)
        cur.execute(f"DROP TABLE {qualified}")
        exists = False

    if not exists:
        logging.info("Creating table %s with %s columns", qualified, len(columns))
        cur.execute(f"CREATE TABLE {qualified} ({', '.join(col_defs)})")
        return

    existing_cols = _fetch_existing_columns(conn, schema, table)
    missing_cols = [col for col in columns if col.lower() not in existing_cols]
    for col in missing_cols:
        logging.info("Adding missing column %s to %s", col, qualified)
        cur.execute(f"ALTER TABLE {qualified} ADD [{col}] {_column_sql_type(col)}")


def _truncate_table(conn, schema: str, table: str) -> None:
    qualified = _qualified_table(schema, table)
    cur = conn.cursor()
    cur.execute(f"SELECT OBJECT_ID('{schema}.{table}', 'U')")
    existing = cur.fetchone()
    exists = bool(existing and existing[0] is not None)
    if not exists:
        logging.info("Skipping truncate because %s does not exist yet.", qualified)
        return
    logging.info("Truncating %s before the first append.", qualified)
    cur.execute(f"TRUNCATE TABLE {qualified}")


def _iter_insert_rows(
    rows: Iterable[dict],
    raw_headers: List[str],
    col_map: Dict[str, str],
    input_path: Path,
    source_ts: datetime | None,
    all_cols: List[str],
    source_file_path: str | None = None,
) -> Iterable[tuple]:
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    source_path_value = source_file_path or str(input_path)
    for idx, row in enumerate(rows, start=1):
        payload = {k: row.get(k) for k in raw_headers}
        payload["csv_filename"] = input_path.name
        payload["source_file_path"] = source_path_value
        payload["source_file_timestamp"] = (
            source_ts.strftime("%Y-%m-%d %H:%M:%S") if source_ts else None
        )
        payload["loaded_at_utc"] = loaded_at.strftime("%Y-%m-%d %H:%M:%S")
        payload["updatedAt"] = loaded_at.strftime("%Y-%m-%d %H:%M:%S")
        payload["row_number"] = idx
        row_out: Dict[str, object] = {
            "csv_filename": input_path.name,
            "source_file_path": source_path_value,
            "source_file_timestamp": source_ts,
            "loaded_at_utc": loaded_at,
            "updatedAt": loaded_at,
            "row_number": idx,
            "payload_json": json.dumps(payload, ensure_ascii=False),
        }
        for raw in raw_headers:
            row_out[col_map[raw]] = row.get(raw)
        yield tuple(row_out.get(col) for col in all_cols)


def _load_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {
            "processed_files": {},
            "last_checked_at": None,
            "last_loaded_path": None,
            "truncate_done": False,
        }
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data.setdefault("processed_files", {})
            data.setdefault("last_checked_at", None)
            data.setdefault("last_loaded_path", None)
            data.setdefault("truncate_done", False)
            return data
    except Exception:
        logging.warning("State file %s is unreadable. Starting with empty state.", state_path)
    return {
        "processed_files": {},
        "last_checked_at": None,
        "last_loaded_path": None,
        "truncate_done": False,
    }


def _save_state(state_path: Path, state: dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _file_signature(path: Path) -> str:
    stat = path.stat()
    return f"{stat.st_mtime_ns}:{stat.st_size}"


def _list_matching_csvs(watch_dir: Path, glob_pattern: str) -> List[Path]:
    if not watch_dir.exists():
        return []
    files = [p for p in watch_dir.glob(glob_pattern) if p.is_file()]
    return sorted(files, key=lambda p: (p.stat().st_mtime_ns, p.name))


def _remote_join(base: str, name: str) -> str:
    if base.endswith("/"):
        return f"{base}{name}"
    return f"{base}/{name}"


def _compile_remote_glob(glob_pattern: str) -> re.Pattern[str]:
    pattern = glob_pattern or "*.csv"
    regex = re.escape(pattern).replace(r"\*", ".*").replace(r"\?", ".")
    return re.compile(rf"^{regex}$", re.IGNORECASE)


def _list_matching_remote_csvs(
    remote_dir: str,
    glob_pattern: str,
    sftp_user: str,
    sftp_pw: str,
) -> List[dict]:
    matcher = _compile_remote_glob(glob_pattern)
    with _open_sftp(sftp_user, sftp_pw) as (_, sftp):
        matches: List[dict] = []
        for attr in sftp.listdir_attr(remote_dir):
            name = attr.filename
            if not matcher.match(name):
                continue
            if not name.lower().endswith(".csv"):
                continue
            matches.append(
                {
                    "name": name,
                    "remote_path": _remote_join(remote_dir, name),
                    "mtime_ns": int(attr.st_mtime) * 1_000_000_000,
                    "size": int(attr.st_size),
                }
            )
    return sorted(matches, key=lambda item: (item["mtime_ns"], item["name"]))


def _remote_signature(item: dict) -> str:
    return f"{int(item['mtime_ns'])}:{int(item['size'])}"


def _download_remote_file(
    remote_path: str,
    local_path: Path,
    sftp_user: str,
    sftp_pw: str,
) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = local_path.with_suffix(local_path.suffix + ".part")
    try:
        if tmp_path.exists():
            tmp_path.unlink()
    except Exception:
        pass
    with _open_sftp(sftp_user, sftp_pw) as (_, sftp):
        remote_size = int(sftp.stat(remote_path).st_size)
        sftp.get(remote_path, str(tmp_path))
    local_size = tmp_path.stat().st_size if tmp_path.exists() else -1
    if local_size != remote_size:
        raise RuntimeError(
            f"size mismatch after remote download: remote={remote_size} local={local_size}"
        )
    tmp_path.replace(local_path)


def load_csv_to_fabric(
    server: str,
    database: str,
    schema: str,
    table: str,
    input_path: Path,
    limit: int = 0,
    driver: str | None = None,
    progress_every: int = 100,
    recreate: bool = False,
    truncate_first: bool = False,
    source_file_path: str | None = None,
) -> int:
    raw_headers, rows, source_ts = _load_rows(input_path, limit=limit)
    all_cols, col_map = _build_table_columns(raw_headers)
    logging.info(
        "Preparing Fabric load for %s rows into %s",
        f"{len(rows):,}",
        _qualified_table(schema, table),
    )
    source_path_value = source_file_path or str(input_path)
    with open_sql_conn(server, database, driver=driver) as conn:
        logging.info("Opened Fabric connection to %s / %s", server, database)
        _ensure_table(conn, schema, table, all_cols, recreate=recreate)
        if truncate_first and not recreate:
            _truncate_table(conn, schema, table)
        # A source file is the idempotency boundary.  Network loss during a
        # row-by-row autocommit sequence previously left partial cohorts data
        # behind and a retry would append duplicates.  Replace that one
        # source-file partition inside a single transaction instead.
        conn.autocommit = False
        inserted = 0
        try:
            cur = conn.cursor()
            cur.execute(
                f"DELETE FROM {_qualified_table(schema, table)} WHERE [source_file_path]=?",
                source_path_value,
            )
            col_list = ", ".join(f"[{c}]" for c in all_cols)
            insert_prefix = f"INSERT INTO {_qualified_table(schema, table)} ({col_list}) VALUES "
            batch: list[tuple] = []
            # Keep every statement under SQL Server's parameter ceiling.
            # This avoids ODBC's slow row-at-a-time executemany behavior for
            # wide cohort rows while retaining one source-file transaction.
            batch_size = max(1, min(100, 1900 // len(all_cols)))

            def insert_batch(rows: list[tuple]) -> None:
                if not rows:
                    return
                row_placeholders = "(" + ", ".join("?" for _ in all_cols) + ")"
                batch_sql = insert_prefix + ", ".join(row_placeholders for _ in rows)
                params: list[object] = []
                for values in rows:
                    params.extend(values)
                cur.execute(batch_sql, *params)

            for item in _iter_insert_rows(
                rows,
                raw_headers,
                col_map,
                input_path,
                source_ts,
                all_cols,
                source_file_path=source_path_value,
            ):
                batch.append(item)
                if len(batch) >= batch_size:
                    insert_batch(batch)
                    inserted += len(batch)
                    batch.clear()
                    logging.info(
                        "Inserted %s / %s rows into %s",
                        f"{inserted:,}",
                        f"{len(rows):,}",
                        _qualified_table(schema, table),
                    )
            if batch:
                insert_batch(batch)
                inserted += len(batch)
                logging.info(
                    "Inserted %s / %s rows into %s",
                    f"{inserted:,}",
                    f"{len(rows):,}",
                    _qualified_table(schema, table),
                )
            cur.execute(
                f"SELECT COUNT_BIG(*) FROM {_qualified_table(schema, table)} "
                "WHERE [source_file_path]=?",
                source_path_value,
            )
            validated = int(cur.fetchone()[0])
            if validated != inserted:
                raise RuntimeError(
                    "post-write row validation failed for "
                    f"{source_path_value}: expected={inserted} actual={validated}"
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        logging.info(
            "Completed and validated load into %s with %s rows inserted",
            _qualified_table(schema, table),
            f"{inserted:,}",
        )
    return inserted


def watch_and_append(
    server: str,
    database: str,
    schema: str,
    table: str,
    watch_dir: Path,
    remote_dir: str,
    glob_pattern: str,
    state_path: Path,
    interval_hours: float,
    limit: int,
    driver: str | None,
    progress_every: int,
    dry_run: bool,
    run_once: bool,
    bootstrap_existing: bool,
    truncate_first: bool,
    sftp_user: str,
    sftp_pw: str,
) -> int:
    state_exists = state_path.exists()
    state = _load_state(state_path)
    processed_files = dict(state.get("processed_files", {}))
    truncate_pending = bool(truncate_first) and not bool(state.get("truncate_done"))
    cycle = 0
    total_inserted = 0

    while True:
        cycle += 1
        logging.info("Watch cycle %s started for %s on %s (%s)", cycle, remote_dir, SFTP_HOST, glob_pattern)
        matched = _list_matching_remote_csvs(remote_dir, glob_pattern, sftp_user, sftp_pw)
        if not matched:
            logging.info("No matching CSV files found in %s on %s for %s", remote_dir, SFTP_HOST, glob_pattern)
        else:
            if not processed_files and not state_exists and not bootstrap_existing:
                for item in matched:
                    processed_files[str(item["remote_path"])] = _remote_signature(item)
                state["processed_files"] = processed_files
                state["last_checked_at"] = datetime.now(timezone.utc).isoformat()
                state["last_loaded_path"] = None
                _save_state(state_path, state)
                state_exists = True
                logging.info(
                    "Initialized watch state from %s existing file(s) without appending historical data. "
                    "Only future new or modified cohorts CSVs will be appended. Use --bootstrap-existing "
                    "or --input for an initial backfill.",
                    len(matched),
                )
                if run_once:
                    return 0
            pending: List[dict] = []
            for item in matched:
                signature = _remote_signature(item)
                if processed_files.get(str(item["remote_path"])) != signature:
                    pending.append(item)
            if not pending:
                logging.info("No new or modified matching CSV files detected.")
            for item in pending:
                remote_path = str(item["remote_path"])
                signature = _remote_signature(item)
                temp_local = TMP_DIR / Path(remote_path).name
                logging.info("Detected new or modified remote CSV: %s", remote_path)
                _download_remote_file(remote_path, temp_local, sftp_user, sftp_pw)
                if dry_run:
                    raw_headers, rows, source_ts = _load_rows(temp_local, limit=limit)
                    all_cols, col_map = _build_table_columns(raw_headers)
                    logging.info("Dry run rows for %s: %s", temp_local.name, f"{len(rows):,}")
                    logging.info(
                        "Dry run source timestamp for %s: %s",
                        temp_local.name,
                        source_ts.isoformat(sep=" ") if source_ts else "not found in filename",
                    )
                    logging.info("Dry run final columns for %s: %s", temp_local.name, all_cols)
                    logging.info(
                        "Dry run sanitized column map for %s: %s",
                        temp_local.name,
                        json.dumps(col_map, ensure_ascii=False),
                    )
                    if truncate_pending:
                        logging.info(
                            "Dry run only: one-time truncate for %s remains pending until the first real append.",
                            _qualified_table(schema, table),
                        )
                else:
                    inserted = load_csv_to_fabric(
                        server=server,
                        database=database,
                        schema=schema,
                        table=table,
                        input_path=temp_local,
                        limit=limit,
                        driver=driver,
                        progress_every=progress_every,
                        recreate=False,
                        truncate_first=truncate_pending,
                        source_file_path=remote_path,
                    )
                    total_inserted += inserted
                    if truncate_pending:
                        state["truncate_done"] = True
                        truncate_pending = False
                    logging.info(
                        "Appended %s row(s) from %s into %s.",
                        f"{inserted:,}",
                        Path(remote_path).name,
                        _qualified_table(schema, table),
                    )
                    # Persist a remote signature only after the Fabric
                    # transaction and its post-write validation commit. A dry
                    # run intentionally remains non-destructive and must not
                    # suppress a later real load.
                    processed_files[remote_path] = signature
                    state["processed_files"] = processed_files
                    state["last_checked_at"] = datetime.now(timezone.utc).isoformat()
                    state["last_loaded_path"] = remote_path
                    _save_state(state_path, state)
                    state_exists = True
                try:
                    if temp_local.exists():
                        temp_local.unlink()
                except Exception:
                    pass

        state["processed_files"] = processed_files
        state["last_checked_at"] = datetime.now(timezone.utc).isoformat()
        _save_state(state_path, state)
        state_exists = True

        if run_once:
            return total_inserted

        sleep_seconds = max(interval_hours * 3600.0, 60.0)
        logging.info("Sleeping for %.2f hour(s) before the next check.", interval_hours)
        time.sleep(sleep_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Append Paycom cohorts CSV data into tm_charteasy.dbo.paycom_cohorts_bronze. "
            "By default, this script watches the SFTP Outbound cohorts directory, checks every 24 hours, "
            "and appends only newly seen or newly modified CSVs."
        )
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT),
        help="Optional explicit cohorts CSV path for a one-time append instead of watch mode",
    )
    parser.add_argument(
        "--source-file-path",
        default="",
        help="Canonical remote source path for an explicit --input retry; used as the idempotent source-file partition key",
    )
    parser.add_argument(
        "--watch-dir",
        default=str(DEFAULT_WATCH_DIR),
        help="Legacy local directory watch path. Remote SFTP watch mode is used by default.",
    )
    parser.add_argument(
        "--remote-dir",
        default=SFTP_REMOTE_DIR,
        help="Remote SFTP directory to watch for cohorts CSVs when --input is not provided",
    )
    parser.add_argument(
        "--glob-pattern",
        default=DEFAULT_GLOB,
        help="Glob pattern used inside --watch-dir to find cohorts CSVs",
    )
    parser.add_argument(
        "--state-file",
        default=str(DEFAULT_STATE_FILE),
        help="JSON state file used to track already appended or unchanged CSVs",
    )
    parser.add_argument(
        "--interval-hours",
        type=float,
        default=24.0,
        help="Hours to wait between outbound directory checks in watch mode",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Check once and exit instead of sleeping and repeating in watch mode",
    )
    parser.add_argument(
        "--bootstrap-existing",
        action="store_true",
        help="In watch mode, append currently existing matching CSVs on the first run instead of only seeding state",
    )
    parser.add_argument("--sftp-un", default="", help="Optional SFTP username override")
    parser.add_argument("--sftp-pw", default="", help="Optional SFTP password override")
    parser.add_argument("--server", default=DEFAULT_SERVER, help="Fabric server hostname")
    parser.add_argument("--database", default=DEFAULT_DB, help="Target Fabric database")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Target schema")
    parser.add_argument("--table", default=DEFAULT_TABLE, help="Target table name")
    parser.add_argument("--limit", type=int, default=0, help="Optional row limit for testing")
    parser.add_argument("--driver", default="", help="Optional explicit ODBC driver name")
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Log progress every N inserted rows during a real load",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print inferred schema and row counts without writing to Fabric",
    )
    parser.add_argument(
        "--recreate-table",
        action="store_true",
        help="Drop and recreate the target table before a one-time --input load",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help=(
            "Truncate the target table once before the first real append. "
            "In watch mode this is persisted in the state file and will not repeat on later cycles."
        ),
    )
    args = parser.parse_args()
    log_path = _setup_logging()

    input_text = (args.input or "").strip()
    input_path = Path(input_text).expanduser() if input_text else None
    watch_dir = Path(args.watch_dir).expanduser()
    remote_dir = (args.remote_dir or "").strip() or SFTP_REMOTE_DIR
    state_path = Path(args.state_file).expanduser()
    driver = (args.driver or "").strip() or None
    # An explicit locally staged file is a recovery path and does not require
    # SFTP authentication. Resolve credentials only for remote watch mode.
    sftp_user, sftp_pw = ("", "")
    if not input_path:
        sftp_user, sftp_pw = _resolve_sftp_creds(args.sftp_un, args.sftp_pw)

    logging.info("Log file: %s", log_path)
    logging.info("Fabric server: %s", args.server)
    logging.info("Fabric database: %s", args.database)
    logging.info("Target table: %s", _qualified_table(args.schema, args.table))
    logging.info("Local watch directory (legacy): %s", watch_dir)
    logging.info("Remote SFTP host: %s", SFTP_HOST)
    logging.info("Remote SFTP directory: %s", remote_dir)
    logging.info("Watch glob pattern: %s", args.glob_pattern)
    logging.info("State file: %s", state_path)
    logging.info("Run once: %s", bool(args.run_once))
    logging.info("Dry run: %s", bool(args.dry_run))
    logging.info("Bootstrap existing: %s", bool(args.bootstrap_existing))
    logging.info("Truncate first load: %s", bool(args.truncate))

    if input_path:
        raw_headers, rows, source_ts = _load_rows(input_path, limit=int(args.limit or 0))
        all_cols, col_map = _build_table_columns(raw_headers)
        logging.info("Input CSV: %s", input_path)
        logging.info(
            "Source file timestamp: %s",
            source_ts.isoformat(sep=" ") if source_ts else "not found in filename",
        )
        logging.info("Rows loaded from CSV: %s", f"{len(rows):,}")
        logging.info("Raw headers: %s", raw_headers)
        logging.info("Sanitized column map: %s", json.dumps(col_map, ensure_ascii=False))
        if args.dry_run:
            logging.info("Final table columns: %s", all_cols)
            logging.info("Dry run only. No Fabric changes were made.")
            return 0

        inserted = load_csv_to_fabric(
            server=args.server,
            database=args.database,
            schema=args.schema,
            table=args.table,
            input_path=input_path,
            limit=int(args.limit or 0),
            driver=driver,
            progress_every=int(args.progress_every or 0),
            recreate=bool(args.recreate_table),
            truncate_first=bool(args.truncate and not args.recreate_table),
            source_file_path=(args.source_file_path or "").strip() or None,
        )
        logging.info(
            "Appended %s row(s) into %s in %s.",
            f"{inserted:,}",
            _qualified_table(args.schema, args.table),
            args.database,
        )
        return 0

    inserted = watch_and_append(
        server=args.server,
        database=args.database,
        schema=args.schema,
        table=args.table,
        watch_dir=watch_dir,
        remote_dir=remote_dir,
        glob_pattern=args.glob_pattern,
        state_path=state_path,
        interval_hours=float(args.interval_hours or 24.0),
        limit=int(args.limit or 0),
        driver=driver,
        progress_every=int(args.progress_every or 0),
        dry_run=bool(args.dry_run),
        run_once=bool(args.run_once),
        bootstrap_existing=bool(args.bootstrap_existing),
        truncate_first=bool(args.truncate),
        sftp_user=sftp_user,
        sftp_pw=sftp_pw,
    )
    logging.info(
        "Watch cycle(s) completed with %s appended row(s) into %s.",
        f"{inserted:,}",
        _qualified_table(args.schema, args.table),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
