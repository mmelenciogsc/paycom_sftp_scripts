#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import base64
import types
import logging
import os
import stat
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import paramiko
import pyodbc

# Fabric helpers (same as FDI_z/app.py)
import sys

FDI_Z_ROOT = Path(__file__).resolve().parent / "FDI_z"
if str(FDI_Z_ROOT) not in sys.path:
    sys.path.insert(0, str(FDI_Z_ROOT))

# --- Preload huggingface_hub stub before importing FDI_z/utils (gradio dependency) ---
def _prepare_huggingface_hub() -> types.ModuleType:
    try:
        import huggingface_hub as _hf_module
    except ModuleNotFoundError:
        _hf_module = types.ModuleType("huggingface_hub")
        sys.modules["huggingface_hub"] = _hf_module
    if not hasattr(_hf_module, "HfFolder"):
        class HfFolder:
            @staticmethod
            def path_token_file() -> str:
                return ""

            @staticmethod
            def get_token() -> str | None:
                return None

            @staticmethod
            def save_token(token: str) -> None:
                pass

            @staticmethod
            def delete_token() -> None:
                pass

        setattr(_hf_module, "HfFolder", HfFolder)
    if not hasattr(_hf_module, "whoami"):
        setattr(_hf_module, "whoami", lambda token=None: {})
    return _hf_module


_hf_module = _prepare_huggingface_hub()

from utils import open_sql_conn  # type: ignore

HOST = os.environ.get("PAYCOM_SFTP_HOST", "push.paycomonline.net")
PORT = int(os.environ.get("PAYCOM_SFTP_PORT", "22"))
REMOTE_DIR = os.environ.get("PAYCOM_SFTP_REMOTE_DIR", "/Outbound")

BASE_ROOT = Path(os.environ.get("PAYCOM_SFTP_WORK_ROOT", str(Path(__file__).resolve().parent / "runtime"))).expanduser()
DOWNLOAD_ROOT = BASE_ROOT / "downloads"
PROCESSED_ROOT = BASE_ROOT / "processed"
REPORT_ROOT = BASE_ROOT / "reports"
STATE_PATH = BASE_ROOT / "state.json"
LOG_DIR = BASE_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / f"masterlog_{time.strftime('%Y%m%d_%H%M%S')}.txt"

DEFAULT_SERVER = os.environ.get(
    "PAYCOM_FABRIC_SERVER",
    "qot6olhksufuxlapzmfsu3zsci-fivilhqesnzepkg2twbufnheqy.datawarehouse.fabric.microsoft.com",
)
TARGET_DB = "dw-silver"
TARGET_SCHEMA = "dbo"

TABLE_EMPLOYEE = "paycom_employee_schedules"
TABLE_SHIFTS = "paycom_theoria_shifts"

MAX_COL_LEN = 128

LOGO_PATH = Path(__file__).resolve().parent / "LOGO_TheoriaMedical_gold.png"


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d%H%M%S")


def _ts_to_dt(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y%m%d%H%M%S")


def _load_state() -> Dict[str, Dict[str, int]]:
    if not STATE_PATH.exists():
        return {}
    try:
        raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {}
        normalized: Dict[str, Dict[str, int]] = {}
        for name, val in raw.items():
            if isinstance(val, dict):
                normalized[name] = {
                    "mtime": int(val.get("mtime", 0) or 0),
                    "size": int(val.get("size", 0) or 0),
                }
            else:
                try:
                    normalized[name] = {"mtime": int(val), "size": 0}
                except Exception:
                    normalized[name] = {"mtime": 0, "size": 0}
        return normalized
    except Exception:
        return {}


def _save_state(state: Dict[str, Dict[str, int]]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _get_state_entry(state: Dict[str, Dict[str, int]], name: str) -> Tuple[int, int]:
    entry = state.get(name) or {}
    return int(entry.get("mtime", 0) or 0), int(entry.get("size", 0) or 0)


def _set_state_entry(state: Dict[str, Dict[str, int]], name: str, mtime: int, size: int) -> None:
    state[name] = {"mtime": int(mtime), "size": int(size)}


def _open_sftp(username: str, password: str) -> Tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    client = paramiko.SSHClient()
    known_hosts_path = (os.environ.get("SFTP_KNOWN_HOSTS_PATH") or str(Path(__file__).resolve().parent / "paycom_known_hosts")).strip()
    if not known_hosts_path or not Path(known_hosts_path).is_file():
        raise SystemExit(
            "Missing verified SFTP host-key file. Set SFTP_KNOWN_HOSTS_PATH "
            "to a protected known_hosts file before starting the worker."
        )
    client.load_host_keys(known_hosts_path)
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(HOST, port=PORT, username=username, password=password, timeout=30)
    return client, client.open_sftp()


def _list_outbound(sftp: paramiko.SFTPClient) -> List[paramiko.SFTPAttributes]:
    # Cohorts-PaycomID files are handled by the dedicated cohorts watcher and
    # must not be downloaded/marked by this schedules-and-shifts worker.
    return [
        a for a in sftp.listdir_attr(REMOTE_DIR)
        if a.filename.lower().endswith(".csv") and _target_table_for_file(a.filename)
    ]


def _download_new_csvs(
    username: str,
    password: str,
    state: Dict[str, Dict[str, int]],
    batch_dir: Path,
    check_mode: str,
) -> Tuple[List[Tuple[Path, str, int, int]], List[str]]:
    client, sftp = _open_sftp(username, password)
    downloaded: List[Tuple[Path, str, int, int]] = []
    new_names: List[str] = []
    try:
        files = _list_outbound(sftp)
        attrs = []
        for attr in files:
            attrs.append((attr.filename, int(attr.st_mtime), int(attr.st_size)))

        # Auto mode compares the complete remote signature for every file.
        # The prior staged fallback only checked size after *no* mtime changes
        # were found, which could miss a same-mtime replacement in a cycle
        # that also contained an ordinary new file.
        station = "mtime"
        if check_mode == "auto":
            station = "signature"
            candidates = [
                (name, mtime, size)
                for name, mtime, size in attrs
                if (mtime, size) != _get_state_entry(state, name)
            ]
        else:
            candidates = [
                (name, mtime, size)
                for name, mtime, size in attrs
                if mtime > _get_state_entry(state, name)[0] or _get_state_entry(state, name)[0] == 0
            ]

        if check_mode != "auto":
            station = check_mode
            if check_mode == "mtime":
                candidates = [
                    (name, mtime, size)
                    for name, mtime, size in attrs
                    if mtime > _get_state_entry(state, name)[0] or _get_state_entry(state, name)[0] == 0
                ]
            elif check_mode == "size+mtime":
                candidates = [
                    (name, mtime, size)
                    for name, mtime, size in attrs
                    if (mtime, size) != _get_state_entry(state, name)
                ]
            elif check_mode == "size":
                candidates = [
                    (name, mtime, size)
                    for name, mtime, size in attrs
                    if size != _get_state_entry(state, name)[1]
                ]

        if candidates:
            logging.info("New/updated CSV detection station: %s (count=%s)", station, len(candidates))
        else:
            logging.info("No new/updated CSVs detected via mtime/size checks.")

        for name, mtime, size in candidates:
            remote_path = f"{REMOTE_DIR}/{name}"
            local_path = batch_dir / name
            logging.info("Downloading %s -> %s", remote_path, local_path)
            sftp.get(remote_path, str(local_path))
            # Do not advance durable state on download.  A file is complete
            # only after its Fabric transaction and post-write row-count
            # validation succeed.
            downloaded.append((local_path, name, mtime, size))
            new_names.append(name)
    finally:
        try:
            sftp.close()
        except Exception:
            pass
        try:
            client.close()
        except Exception:
            pass
    return downloaded, new_names


def _clean_value(val: object) -> str:
    text = "" if val is None else str(val)
    text = text.replace("\x00", "").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    return " ".join(text.split()).strip()


def _sanitize_colname(name: str) -> str:
    raw = (name or "").strip()
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in raw)
    cleaned = "_".join([p for p in cleaned.split("_") if p])
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
        else:
            seen[key] += 1
            suffix = seen[key]
            base = col[: MAX_COL_LEN - (len(str(suffix)) + 1)]
            new_col = f"{base}_{suffix}"
            out.append(new_col)
            seen[new_col.lower()] = 0
    return out


def _ensure_table_columns(conn, table: str, columns: List[str]) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """,
        TARGET_SCHEMA,
        table,
    )
    existing = {row[0].lower() for row in cur.fetchall()}
    if not existing:
        col_defs = []
        for col in columns:
            if col in {"UpdatedAt"}:
                col_defs.append(f"[{col}] DATETIME2(6) NULL")
            else:
                col_defs.append(f"[{col}] VARCHAR(MAX) NULL")
        cur.execute(
            f"CREATE TABLE {TARGET_SCHEMA}.{table} ({', '.join(col_defs)})"
        )
        return
    for col in columns:
        if col.lower() in existing:
            continue
        dtype = "DATETIME2(6)" if col in {"UpdatedAt"} else "VARCHAR(MAX)"
        cur.execute(f"ALTER TABLE {TARGET_SCHEMA}.{table} ADD [{col}] {dtype} NULL")


def _process_csv(path: Path, updated_at: datetime, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / path.name
    processed = 0
    with path.open("r", encoding="utf-8", newline="") as f_in, out_path.open(
        "w", encoding="utf-8", newline=""
    ) as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.writer(f_out)
        writer.writerow(["csv_filename", "UpdatedAt", "payload_json"])
        for row in reader:
            payload = {k: _clean_value(v) for k, v in row.items()}
            payload["csv_filename"] = path.name
            payload["UpdatedAt"] = updated_at.strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow(
                [path.name, payload["UpdatedAt"], json.dumps(payload, ensure_ascii=False)]
            )
            processed += 1
            if processed % 5000 == 0:
                logging.info("Processed %s rows from %s", processed, path.name)
    return out_path


def _ensure_db_schema_tables(server: str) -> None:
    logging.info("Ensuring schema/tables exist...")
    with open_sql_conn(server, TARGET_DB) as conn:
        cur = conn.cursor()
        # dbo already exists; no CREATE DATABASE / CREATE SCHEMA needed.
        for table in (TABLE_EMPLOYEE, TABLE_SHIFTS):
            cur.execute(
                f"""
                IF OBJECT_ID('{TARGET_SCHEMA}.{table}', 'U') IS NULL
                CREATE TABLE {TARGET_SCHEMA}.{table} (
                    csv_filename VARCHAR(MAX) NULL,
                    UpdatedAt DATETIME2(6) NULL,
                    payload_json VARCHAR(MAX) NULL
                )
                """
            )


def _truncate_tables(server: str) -> None:
    logging.info("Truncating target tables (one-time)...")
    with open_sql_conn(server, TARGET_DB) as conn:
        cur = conn.cursor()
        for table in (TABLE_EMPLOYEE, TABLE_SHIFTS):
            cur.execute(
                f"""
                IF OBJECT_ID('{TARGET_SCHEMA}.{table}', 'U') IS NOT NULL
                TRUNCATE TABLE {TARGET_SCHEMA}.{table}
                """
            )


def _target_table_for_file(filename: str) -> str | None:
    low = filename.lower()
    norm = "".join(ch if ch.isalnum() else "_" for ch in low)
    norm = "_".join([p for p in norm.split("_") if p])
    if "employee_schedules" in norm or "employee_schedules" in low or "employee schedules" in low:
        return TABLE_EMPLOYEE
    if "theoria_shifts" in norm or "theoria-shifts" in low or "theoria shifts" in low:
        return TABLE_SHIFTS
    return None


def _resolve_sftp_credentials(username: str, password: str) -> Tuple[str, str]:
    values: Dict[str, str] = {}
    credential_file = (os.environ.get("SFTP_CREDENTIAL_FILE") or "").strip()
    if credential_file and Path(credential_file).is_file():
        for line in Path(credential_file).read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip("\"'")
    resolved_user = (username or os.environ.get("SFTP_UN") or values.get("SFTP_UN") or "").strip()
    resolved_password = (password or os.environ.get("SFTP_PW") or values.get("SFTP_PW") or "").strip()
    if not resolved_user or not resolved_password:
        raise SystemExit("Missing SFTP credentials. Set SFTP_UN and SFTP_PW in the protected service environment.")
    return resolved_user, resolved_password


def _insert_csv_to_fabric(server: str, csv_path: Path) -> int:
    table = _target_table_for_file(csv_path.name)
    if not table:
        logging.warning("Skipping unknown CSV type: %s", csv_path.name)
        return 0
    inserted = 0
    logging.info("Loading %s into %s.%s", csv_path.name, TARGET_SCHEMA, table)
    with open_sql_conn(server, TARGET_DB) as conn, csv_path.open(
        "r", encoding="utf-8", newline=""
    ) as f:
        cur = conn.cursor()
        cur.fast_executemany = True
        reader = csv.DictReader(f)
        batch: List[Tuple[str, datetime, str]] = []
        for row in reader:
            updated = datetime.strptime(row["UpdatedAt"], "%Y-%m-%d %H:%M:%S")
            batch.append((row["csv_filename"], updated, row["payload_json"]))
            if len(batch) >= 1000:
                _insert_batch(cur, table, batch)
                inserted += len(batch)
                logging.info("Inserted %s rows into %s.%s (running)", inserted, TARGET_SCHEMA, table)
                batch = []
        if batch:
            _insert_batch(cur, table, batch)
            inserted += len(batch)
    logging.info("Inserted %s rows into %s.%s", inserted, TARGET_SCHEMA, table)
    return inserted


def _stream_process_and_insert(server: str, src_path: Path, updated_at: datetime, out_dir: Path) -> int:
    table = _target_table_for_file(src_path.name)
    if not table:
        logging.warning("Skipping unknown CSV type: %s", src_path.name)
        return 0
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / src_path.name
    inserted = 0
    processed = 0
    logging.info("Streaming process+insert for %s into %s.%s", src_path.name, TARGET_SCHEMA, table)
    with open_sql_conn(server, TARGET_DB) as conn, src_path.open(
        "r", encoding="utf-8", newline=""
    ) as f_in, out_path.open("w", encoding="utf-8", newline="") as f_out:
        cur = conn.cursor()
        try:
            cur.fast_executemany = True
        except Exception:
            pass
        reader = csv.DictReader(f_in)
        raw_headers = reader.fieldnames or []
        base_cols = ["csv_filename", "UpdatedAt", "payload_json"]
        sanitized = [_sanitize_colname(h) for h in raw_headers]
        sanitized = _dedupe_columns(sanitized)
        col_map = {raw: san for raw, san in zip(raw_headers, sanitized)}
        all_cols = _dedupe_columns(base_cols + list(col_map.values()))
        _ensure_table_columns(conn, table, all_cols)

        # Replace a source-file partition in one transaction. This prevents a
        # retry from duplicating rows after a Fabric/network interruption.
        conn.autocommit = False
        try:
            cur.execute(f"DELETE FROM {TARGET_SCHEMA}.{table} WHERE csv_filename=?", src_path.name)
            writer = csv.writer(f_out)
            writer.writerow(["csv_filename", "UpdatedAt", "payload_json"])
            col_list = ", ".join(f"[{c}]" for c in all_cols)
            batch: List[Tuple] = []
            # Fabric's ODBC endpoint can degrade into one network round trip
            # per bound row for executemany with large JSON/text columns. Use
            # bounded multi-row statements instead, remaining below SQL
            # Server's parameter limit while retaining one transaction per
            # source-file partition.
            max_rows_per_statement = max(1, min(100, 1900 // len(all_cols)))

            def insert_batch(rows: List[Tuple]) -> None:
                if not rows:
                    return
                row_placeholders = "(" + ", ".join("?" for _ in all_cols) + ")"
                insert_sql = (
                    f"INSERT INTO {TARGET_SCHEMA}.{table} ({col_list}) VALUES "
                    + ", ".join(row_placeholders for _ in rows)
                )
                params: List[object] = []
                for values in rows:
                    params.extend(values)
                cur.execute(insert_sql, *params)

            for row in reader:
                payload = {k: _clean_value(v) for k, v in row.items()}
                payload["csv_filename"] = src_path.name
                payload["UpdatedAt"] = updated_at.strftime("%Y-%m-%d %H:%M:%S")
                payload_json = json.dumps(payload, ensure_ascii=False)
                writer.writerow([src_path.name, payload["UpdatedAt"], payload_json])
                row_out: Dict[str, object] = {"csv_filename": src_path.name, "UpdatedAt": updated_at, "payload_json": payload_json}
                for raw, val in row.items():
                    col = col_map.get(raw)
                    if col:
                        row_out[col] = _clean_value(val)
                batch.append(tuple(row_out.get(c) for c in all_cols))
                processed += 1
                if processed % 5000 == 0:
                    logging.info("Processed %s rows from %s", processed, src_path.name)
                if len(batch) >= max_rows_per_statement:
                    insert_batch(batch)
                    inserted += len(batch)
                    logging.info("Inserted %s rows into %s.%s (running)", inserted, TARGET_SCHEMA, table)
                    batch = []
            if batch:
                insert_batch(batch)
                inserted += len(batch)
            cur.execute(f"SELECT COUNT_BIG(*) FROM {TARGET_SCHEMA}.{table} WHERE csv_filename=?", src_path.name)
            validated = int(cur.fetchone()[0])
            if validated != processed:
                raise RuntimeError(f"post-write row validation failed for {src_path.name}: expected={processed} actual={validated}")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    logging.info("Completed %s: processed=%s inserted=%s", src_path.name, processed, inserted)
    return inserted


def _insert_batch(cur: pyodbc.Cursor, table: str, rows: List[Tuple[str, datetime, str]]) -> None:
    sql = f"INSERT INTO {TARGET_SCHEMA}.{table} (csv_filename, UpdatedAt, payload_json) VALUES (?, ?, ?)"
    cur.executemany(sql, rows)


def _report_css() -> str:
    return """
    body { background:#0f1114; color:#e6e6e6; font-family: Arial, Helvetica, sans-serif; }
    .wrap { max-width: 980px; margin: 20px auto; }
    .logo { text-align:center; margin: 16px 0; }
    .title { text-align:center; color:#f5d66a; font-size: 26px; margin-bottom: 8px; }
    .card { background:#15181d; border:1px solid #2a2f38; border-radius:12px; padding:16px; margin:12px 0; }
    table { width:100%; border-collapse:collapse; }
    th, td { border:1px solid #2a2f38; padding:8px 10px; text-align:left; }
    th { color:#f5d66a; background:#1f232b; }
    """


def _query_table_preview(conn, table: str) -> Tuple[int, List[Tuple]]:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT_BIG(*) FROM {TARGET_SCHEMA}.{table}")
    count = int(cur.fetchone()[0])
    cur.execute(f"SELECT TOP 3 csv_filename, UpdatedAt FROM {TARGET_SCHEMA}.{table} ORDER BY UpdatedAt DESC")
    rows = cur.fetchall() or []
    return count, rows


def _write_report(batch_ts: str, server: str, new_files: List[str]) -> Path:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    report_path = REPORT_ROOT / f"{batch_ts}_report.html"
    with open_sql_conn(server, TARGET_DB) as conn:
        emp_count, emp_rows = _query_table_preview(conn, TABLE_EMPLOYEE)
        shift_count, shift_rows = _query_table_preview(conn, TABLE_SHIFTS)

    logo_html = ""
    if LOGO_PATH.exists():
        data = LOGO_PATH.read_bytes()
        b64 = base64.b64encode(data).decode("ascii")
        logo_html = (
            "<div class='logo'>"
            f"<img alt='Theoria Medical' style='max-height:120px;' src='data:image/png;base64,{b64}' />"
            "</div>"
        )

    def _rows_html(rows: Iterable[Tuple]) -> str:
        return "".join(
            f"<tr><td>{r[0]}</td><td>{r[1]}</td></tr>" for r in rows
        )

    html = f"""
    <!DOCTYPE html>
    <html><head><meta charset="utf-8"/><style>{_report_css()}</style></head>
    <body><div class="wrap">
    {logo_html}
    <div class="title">Paycom SFTP → Fabric Report</div>
    <div class="card"><strong>Batch:</strong> {batch_ts}</div>
    <div class="card">
      <h3>New files detected</h3>
      <ul>
        {''.join(f'<li>{name}</li>' for name in (new_files or []))}
      </ul>
    </div>
    <div class="card">
      <h3>employee_schedules</h3>
      <p>Total rows: {emp_count}</p>
      <table><thead><tr><th>csv_filename</th><th>UpdatedAt</th></tr></thead>
      <tbody>{_rows_html(emp_rows)}</tbody></table>
    </div>
    <div class="card">
      <h3>theoria_shifts</h3>
      <p>Total rows: {shift_count}</p>
      <table><thead><tr><th>csv_filename</th><th>UpdatedAt</th></tr></thead>
      <tbody>{_rows_html(shift_rows)}</tbody></table>
    </div>
    </div></body></html>
    """
    report_path.write_text(html, encoding="utf-8")
    return report_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Paycom SFTP → Fabric pipeline")
    parser.add_argument("--un", default="", help="Optional SFTP username override")
    parser.add_argument("--pw", default="", help="Optional SFTP password override")
    parser.add_argument("--interval", type=int, default=300, help="Loop interval in seconds")
    parser.add_argument("--server", default=DEFAULT_SERVER, help="Fabric server")
    parser.add_argument("--timer-reset", action="store_true", help="Ignore previous state and treat all files as new")
    parser.add_argument(
        "--check-mode",
        default="mtime",
        choices=["auto", "mtime", "size+mtime", "size"],
        help="Detection mode: mtime (default), auto (mtime→size+mtime→size), or force a station",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate target tables once on first successful batch before insert",
    )
    parser.add_argument("--run-once", action="store_true", help="Process one poll cycle and exit")
    args = parser.parse_args()
    sftp_user, sftp_password = _resolve_sftp_credentials(args.un, args.pw)

    BASE_ROOT.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Starting paycom_sftp2fabric. masterlog=%s", LOG_PATH)

    if args.timer_reset or not any(DOWNLOAD_ROOT.glob("*")):
        state = {}
        if not args.timer_reset:
            logging.info("Downloads directory is empty; resetting timer/state.")
    else:
        state = _load_state()
    truncated_once = False
    while True:
        batch_ts = _now_ts()
        download_dir = DOWNLOAD_ROOT / batch_ts
        processed_dir = PROCESSED_ROOT / batch_ts
        download_dir.mkdir(parents=True, exist_ok=True)
        processed_dir.mkdir(parents=True, exist_ok=True)

        logging.info("Checking SFTP for new CSVs...")
        downloaded, new_names = _download_new_csvs(
            sftp_user, sftp_password, state, download_dir, args.check_mode
        )
        if new_names:
            logging.info("New files detected: %s", ", ".join(new_names))
        if not downloaded:
            logging.info("No new CSVs found. Sleeping %s seconds.", args.interval)
            if args.run_once:
                return
            time.sleep(args.interval)
            continue

        logging.info("Processing %s CSVs (streaming)...", len(downloaded))
        updated_at = _ts_to_dt(batch_ts)
        _ensure_db_schema_tables(args.server)
        if args.truncate and not truncated_once:
            _truncate_tables(args.server)
            truncated_once = True
        total_inserted = 0
        completed_names: List[str] = []
        for p, name, mtime, size in downloaded:
            table = _target_table_for_file(p.name)
            if not table:
                # Cohorts are owned by the dedicated cohorts watcher. Do not
                # advance the general-loader state for that separate feed.
                logging.info("Deferring cohorts CSV to the dedicated cohorts watcher: %s", p.name)
                continue
            logging.info("Processing file: %s", p.name)
            try:
                total_inserted += _stream_process_and_insert(args.server, p, updated_at, processed_dir)
                _set_state_entry(state, name, mtime, size)
                _save_state(state)
                completed_names.append(name)
            except Exception as exc:
                logging.exception("Failed processing %s: %s", p.name, exc)
        logging.info("Inserted total %s rows into Fabric.", total_inserted)

        report_path = _write_report(batch_ts, args.server, completed_names)
        logging.info("Report written: %s", report_path)

        if args.run_once:
            return
        logging.info("Sleeping %s seconds before next check.", args.interval)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
