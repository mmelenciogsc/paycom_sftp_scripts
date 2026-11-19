#!/usr/bin/env python3
"""Download new Paycom PTO SFTP exports and publish them to dedicated UAT Fabric items."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import posixpath
import stat
import struct
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import paramiko
import pyarrow as pa
import pyarrow.parquet as pq


PACKAGE_ROOT = Path(__file__).resolve().parent
WORKSPACE_ID = os.environ.get("PAYCOM_PTO_WORKSPACE_ID", "9e852a2a-9304-4772-a8da-9d8342b4e486")
WORKSPACE_NAME = os.environ.get("PAYCOM_PTO_WORKSPACE_NAME", "ws_uat")
LAKEHOUSE_ID = os.environ.get("PAYCOM_PTO_LAKEHOUSE_ID", "c64fb4ca-c668-46a4-9536-35ea798a16af")
LAKEHOUSE_NAME = os.environ.get("PAYCOM_PTO_LAKEHOUSE_NAME", "lh_uat_paycom_pto")
WAREHOUSE_ID = os.environ.get("PAYCOM_PTO_WAREHOUSE_ID", "fbdc6177-1aba-45fe-bc54-9d010069e543")
WAREHOUSE_NAME = os.environ.get("PAYCOM_PTO_WAREHOUSE_NAME", "wh_uat_paycom_pto")
WAREHOUSE_DATABASE = os.environ.get("PAYCOM_PTO_WAREHOUSE_DATABASE", "wh_uat_paycom_pto")
WAREHOUSE_SCHEMA = "dbo"
WAREHOUSE_TABLE = os.environ.get("PAYCOM_PTO_TABLE", "paycom_pto_raw")
LAKEHOUSE_SUBDIRECTORY = os.environ.get("PAYCOM_PTO_LAKEHOUSE_SUBDIRECTORY", "paycom_pto_raw")

SFTP_HOST = os.environ.get("PAYCOM_SFTP_HOST", "push.paycomonline.net")
SFTP_PORT = int(os.environ.get("PAYCOM_SFTP_PORT", "22"))
SFTP_REMOTE_DIRECTORY = os.environ.get("PAYCOM_SFTP_REMOTE_DIR", "/Outbound")
SFTP_KNOWN_HOSTS = str(PACKAGE_ROOT / "paycom_known_hosts")

RUNTIME_ROOT = Path(os.environ.get("PAYCOM_PTO_RUNTIME_ROOT", str(PACKAGE_ROOT / "runtime_pto"))).expanduser()
DOWNLOAD_ROOT = RUNTIME_ROOT / "downloads"
PARQUET_ROOT = RUNTIME_ROOT / "parquet"
STATE_PATH = RUNTIME_ROOT / "state.json"
LOG_PATH = RUNTIME_ROOT / "worker.log"
INTERVAL_SECONDS = 24 * 60 * 60

SOURCE_HEADERS = (
    "Employee Code",
    "Employee",
    "Department",
    "Date Requested",
    "Time Requested",
    "Earning",
    "Supervisor Comments",
    "Time Added",
    "Unit of Time",
    "POD",
    "Supervisor Primary",
)
TARGET_COLUMNS = {
    "Employee Code": "employee_code",
    "Employee": "employee_name",
    "Department": "department",
    "Date Requested": "date_requested",
    "Time Requested": "time_requested",
    "Earning": "earning",
    "Supervisor Comments": "supervisor_comments",
    "Time Added": "time_added",
    "Unit of Time": "unit_of_time",
    "POD": "pod",
    "Supervisor Primary": "supervisor_primary",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def configure_logging() -> None:
    RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ %(levelname)s %(message)s",
        handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"schema_version": 1, "files": {}, "last_success_utc": None}
    try:
        value = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if isinstance(value, dict) and isinstance(value.get("files"), dict):
            return value
    except (OSError, json.JSONDecodeError):
        logging.warning("State file could not be read; retaining it and starting with an empty in-memory state")
    return {"schema_version": 1, "files": {}, "last_success_utc": None}


def save_state(state: dict[str, Any]) -> None:
    temporary = STATE_PATH.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(STATE_PATH)


def credentials() -> tuple[str, str]:
    values: dict[str, str] = {}
    credential_file = (os.environ.get("SFTP_CREDENTIAL_FILE") or "").strip()
    if credential_file:
        path = Path(credential_file).expanduser()
        if path.is_file():
            for line in path.read_text(encoding="utf-8").splitlines():
                if "=" in line and not line.lstrip().startswith("#"):
                    key, value = line.split("=", 1)
                    values[key.strip()] = value.strip().strip("\"'")
    username = (os.environ.get("SFTP_UN") or os.environ.get("PAYCOM_SFTP_USERNAME") or values.get("SFTP_UN") or "").strip()
    password = os.environ.get("SFTP_PW") or os.environ.get("PAYCOM_SFTP_PASSWORD") or values.get("SFTP_PW") or ""
    if not username or not password:
        raise RuntimeError("SFTP_UN/SFTP_PW or PAYCOM_SFTP_USERNAME/PAYCOM_SFTP_PASSWORD must be set")
    return username, password


def open_sftp(username: str, password: str) -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    known_hosts = Path(os.environ.get("SFTP_KNOWN_HOSTS_PATH") or SFTP_KNOWN_HOSTS)
    if not known_hosts.is_file():
        raise RuntimeError(f"Verified SFTP known-hosts file is missing: {known_hosts}")
    client = paramiko.SSHClient()
    client.load_host_keys(str(known_hosts))
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(
        SFTP_HOST,
        port=SFTP_PORT,
        username=username,
        password=password,
        look_for_keys=False,
        allow_agent=False,
        timeout=30,
        banner_timeout=30,
        auth_timeout=30,
    )
    return client, client.open_sftp()


def is_pto_file(name: str) -> bool:
    lower = name.lower()
    return lower.endswith(".csv") and ("time off" in lower or "pto" in lower)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_new_files(state: dict[str, Any], username: str, password: str) -> list[tuple[Path, str, int, int, str]]:
    client, sftp = open_sftp(username, password)
    batch = DOWNLOAD_ROOT / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    downloaded: list[tuple[Path, str, int, int, str]] = []
    try:
        entries = sorted(sftp.listdir_attr(SFTP_REMOTE_DIRECTORY), key=lambda item: item.filename)
        for entry in entries:
            if not is_pto_file(entry.filename) or not stat.S_ISREG(entry.st_mode):
                continue
            prior = state["files"].get(entry.filename) or {}
            mtime = int(entry.st_mtime)
            size = int(entry.st_size)
            prior_mtime = prior.get("mtime", prior.get("source_mtime"))
            prior_size = prior.get("size", prior.get("source_size"))
            if prior.get("status") == "published" and int(prior_mtime or 0) == mtime and int(prior_size or 0) == size:
                continue
            batch.mkdir(parents=True, exist_ok=True)
            destination = batch / Path(entry.filename).name
            partial = destination.with_suffix(destination.suffix + ".part")
            remote_path = posixpath.join(SFTP_REMOTE_DIRECTORY, entry.filename)
            logging.info("Downloading PTO file %s", entry.filename)
            sftp.get(remote_path, str(partial))
            partial.replace(destination)
            digest = sha256_file(destination)
            downloaded.append((destination, entry.filename, mtime, size, digest))
    finally:
        sftp.close()
        client.close()
    return downloaded


def build_parquet(source: Path, remote_name: str, remote_mtime: int, digest: str) -> tuple[Path, int]:
    output = PARQUET_ROOT / f"{digest}.parquet"
    if output.exists():
        table = pq.read_table(output)
        return output, table.num_rows
    rows: list[dict[str, Any]] = []
    with source.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        headers = tuple(reader.fieldnames or ())
        missing = [header for header in SOURCE_HEADERS if header not in headers]
        if missing:
            raise ValueError(f"{remote_name} is missing required PTO columns: {missing}")
        retrieved = utc_now()
        source_mtime = datetime.fromtimestamp(remote_mtime, timezone.utc).isoformat().replace("+00:00", "Z")
        for number, raw in enumerate(reader, start=1):
            row = {target: ((raw.get(header) or "").strip() or None) for header, target in TARGET_COLUMNS.items()}
            row.update(
                source_file_name=remote_name,
                source_remote_path=posixpath.join(SFTP_REMOTE_DIRECTORY, remote_name),
                source_file_mtime_utc=source_mtime,
                retrieved_at_utc=retrieved,
                source_content_sha256=digest,
                source_row_number=str(number),
                schema_version="paycom-pto-raw-v1",
            )
            rows.append(row)
    if not rows:
        raise ValueError(f"{remote_name} contains no data rows")
    fields = list(TARGET_COLUMNS.values()) + [
        "source_file_name", "source_remote_path", "source_file_mtime_utc", "retrieved_at_utc",
        "source_content_sha256", "source_row_number", "schema_version",
    ]
    schema = pa.schema([(name, pa.string()) for name in fields])
    table = pa.Table.from_pylist(rows, schema=schema)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".tmp.parquet")
    pq.write_table(table, temporary, compression="snappy")
    temporary.replace(output)
    return output, table.num_rows


def fabric_token(scope: str) -> str:
    from azure.identity import AzureCliCredential, DefaultAzureCredential, EnvironmentCredential, ManagedIdentityCredential
    for credential_type in (AzureCliCredential, ManagedIdentityCredential, EnvironmentCredential, DefaultAzureCredential):
        try:
            return credential_type().get_token(scope).token
        except Exception:
            continue
    raise RuntimeError(f"Azure credential unavailable for {scope}")


def fabric_get(url: str, token: str) -> dict[str, Any]:
    import requests
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=120)
    if response.status_code >= 400:
        raise RuntimeError(f"Fabric API GET failed ({response.status_code})")
    return response.json() if response.content else {}


def resolve_targets() -> dict[str, str]:
    token = fabric_token("https://api.fabric.microsoft.com/.default")
    lakehouse = fabric_get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/lakehouses/{LAKEHOUSE_ID}", token)
    warehouse = fabric_get(f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/warehouses/{WAREHOUSE_ID}", token)
    if lakehouse.get("displayName") not in {None, LAKEHOUSE_NAME}:
        raise RuntimeError("Configured Lakehouse ID does not match the expected dedicated PTO Lakehouse")
    if warehouse.get("displayName") not in {None, WAREHOUSE_NAME}:
        raise RuntimeError("Configured Warehouse ID does not match the expected dedicated PTO Warehouse")
    files_base = str((lakehouse.get("properties") or {}).get("oneLakeFilesPath") or "").rstrip("/")
    server = str((warehouse.get("properties") or {}).get("connectionInfo") or "").strip()
    if not files_base or not server:
        raise RuntimeError("Dedicated UAT Fabric target metadata is incomplete")
    return {"files_base": files_base, "server": server}


def storage_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "x-ms-version": "2021-06-08", "Content-Type": "application/octet-stream"}


def upload_onelake(path: Path, relative: str, files_base: str) -> str:
    import requests
    token = fabric_token("https://storage.azure.com/.default")
    headers = storage_headers(token)
    relative = relative.strip("/")
    base = files_base.rstrip("/")
    parts = str(Path(relative).parent).replace("\\", "/")
    if parts and parts != ".":
        built: list[str] = []
        for part in parts.split("/"):
            built.append(part)
            response = requests.put(f"{base}/{'/'.join(built)}?resource=directory", headers=headers, timeout=120)
            if response.status_code not in {200, 201, 202, 409}:
                raise RuntimeError(f"OneLake directory creation failed ({response.status_code})")
    url = f"{base}/{relative}"
    response = requests.put(f"{url}?resource=file", headers=headers, timeout=120)
    if response.status_code not in {200, 201, 202}:
        raise RuntimeError(f"OneLake file creation failed ({response.status_code})")
    position = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            response = requests.patch(f"{url}?action=append&position={position}", headers=headers, data=chunk, timeout=300)
            if response.status_code not in {200, 202}:
                raise RuntimeError(f"OneLake append failed ({response.status_code})")
            position += len(chunk)
    response = requests.patch(f"{url}?action=flush&position={position}&close=true", headers=headers, timeout=300)
    if response.status_code not in {200, 202}:
        raise RuntimeError(f"OneLake flush failed ({response.status_code})")
    return url


def sql_connection(server: str):
    import pyodbc
    token = fabric_token("https://database.windows.net/.default")
    token_bytes = token.encode("utf-16-le")
    attrs = {1256: struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)}
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};PORT=1433;DATABASE={WAREHOUSE_DATABASE};Encrypt=yes;TrustServerCertificate=yes",
        attrs_before=attrs,
        autocommit=True,
    )


def sql_ident(value: str) -> str:
    return "[" + value.replace("]", "]]" ) + "]"


def publish_to_warehouse(parquet: Path, source_name: str, digest: str, row_count: int, target: dict[str, str], onelake_url: str) -> int:
    connection = sql_connection(target["server"])
    cursor = connection.cursor()
    table = f"{sql_ident(WAREHOUSE_SCHEMA)}.{sql_ident(WAREHOUSE_TABLE)}"
    columns = list(TARGET_COLUMNS.values()) + [
        "source_file_name", "source_remote_path", "source_file_mtime_utc", "retrieved_at_utc",
        "source_content_sha256", "source_row_number", "schema_version",
    ]
    definition = ", ".join(f"{sql_ident(column)} VARCHAR(MAX) NULL" for column in columns)
    try:
        cursor.execute(f"IF OBJECT_ID(N'{WAREHOUSE_SCHEMA}.{WAREHOUSE_TABLE}', N'U') IS NULL CREATE TABLE {table} ({definition})")
        # Remove only the prior version of this exact source file. This makes retries and
        # same-name replacements idempotent without affecting other PTO files.
        cursor.execute(
            f"DELETE FROM {table} WHERE {sql_ident('source_file_name')} = ? OR {sql_ident('source_content_sha256')} = ?",
            source_name,
            digest,
        )
        safe_url = onelake_url.replace("'", "''")
        cursor.execute(f"COPY INTO {table} FROM '{safe_url}' WITH (FILE_TYPE='PARQUET')")
        cursor.execute(f"SELECT COUNT_BIG(*) FROM {table} WHERE {sql_ident('source_content_sha256')} = ?", digest)
        loaded = int(cursor.fetchone()[0])
        if loaded != row_count:
            raise RuntimeError(f"Warehouse row-count validation failed: expected {row_count}, got {loaded}")
        return loaded
    finally:
        cursor.close()
        connection.close()


def process_file(path: Path, remote_name: str, remote_mtime: int, remote_size: int, digest: str) -> dict[str, Any]:
    parquet, rows = build_parquet(path, remote_name, remote_mtime, digest)
    target = resolve_targets()
    relative = f"{LAKEHOUSE_SUBDIRECTORY}/{digest}.parquet"
    onelake_url = upload_onelake(parquet, relative, target["files_base"])
    loaded = publish_to_warehouse(parquet, remote_name, digest, rows, target, onelake_url)
    return {
        "status": "published",
        "source_file_name": remote_name,
        "source_mtime": remote_mtime,
        "source_size": remote_size,
        "sha256": digest,
        "row_count": rows,
        "warehouse_row_count": loaded,
        "lakehouse": LAKEHOUSE_NAME,
        "lakehouse_path": relative,
        "warehouse": WAREHOUSE_NAME,
        "warehouse_table": f"{WAREHOUSE_SCHEMA}.{WAREHOUSE_TABLE}",
        "published_at_utc": utc_now(),
    }


def run_once() -> int:
    state = load_state()
    username, password = credentials()
    files = download_new_files(state, username, password)
    if not files:
        logging.info("No new PTO SFTP files found")
        return 0
    failed: list[str] = []
    for path, name, mtime, size, digest in files:
        try:
            result = process_file(path, name, mtime, size, digest)
            state["files"][name] = result
            state["last_success_utc"] = utc_now()
            save_state(state)
            logging.info("Published %s rows from %s to %s and %s", result["row_count"], name, WAREHOUSE_TABLE, LAKEHOUSE_NAME)
        except Exception:
            failed.append(name)
            logging.exception("PTO file failed; it remains eligible for retry: %s", name)
    if failed:
        raise RuntimeError("PTO files failed: " + ", ".join(failed))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Run one polling cycle and exit")
    parser.add_argument("--interval-hours", type=float, default=24.0, help="Polling interval in hours")
    args = parser.parse_args()
    configure_logging()
    while True:
        try:
            run_once()
        except Exception:
            logging.exception("PTO polling cycle failed; next cycle will retry")
            if args.once:
                return 1
        if args.once:
            return 0
        time.sleep(max(300.0, args.interval_hours * 3600.0))


if __name__ == "__main__":
    raise SystemExit(main())
