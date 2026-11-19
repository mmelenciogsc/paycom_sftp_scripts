#!/usr/bin/env python3
"""Consolidate Paycom SFTP feeds into the UAT bronze Lakehouse paycom schema."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import posixpath
import stat
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import paramiko
import pyarrow as pa
import pyarrow.parquet as pq
import requests

try:
    from azure.identity import AzureCliCredential, DefaultAzureCredential, EnvironmentCredential, ManagedIdentityCredential
except Exception:  # pragma: no cover - dependency diagnostics are handled at runtime
    AzureCliCredential = DefaultAzureCredential = EnvironmentCredential = ManagedIdentityCredential = None


PACKAGE_ROOT = Path(__file__).resolve().parent
WORKSPACE_ID = os.environ.get("PAYCOM_TARGET_WORKSPACE_ID", "9e852a2a-9304-4772-a8da-9d8342b4e486")
WORKSPACE_NAME = os.environ.get("PAYCOM_TARGET_WORKSPACE_NAME", "ws_uat")
LAKEHOUSE_ID = os.environ.get("PAYCOM_TARGET_LAKEHOUSE_ID", "82eb79c4-8b8d-4761-8fc3-2a39898aaadc")
LAKEHOUSE_NAME = os.environ.get("PAYCOM_TARGET_LAKEHOUSE_NAME", "lh_uat_theoria_bronze")
SCHEMA = os.environ.get("PAYCOM_TARGET_SCHEMA", "paycom")
SFTP_HOST = os.environ.get("PAYCOM_SFTP_HOST", "push.paycomonline.net")
SFTP_PORT = int(os.environ.get("PAYCOM_SFTP_PORT", "22"))
SFTP_REMOTE_DIR = os.environ.get("PAYCOM_SFTP_REMOTE_DIR", "/Outbound")
KNOWN_HOSTS = Path(os.environ.get("SFTP_KNOWN_HOSTS_PATH", str(PACKAGE_ROOT / "paycom_known_hosts"))).expanduser()
RUNTIME_ROOT = Path(os.environ.get("PAYCOM_CONSOLIDATED_RUNTIME_ROOT", str(PACKAGE_ROOT / "runtime_consolidated"))).expanduser()
STATE_PATH = RUNTIME_ROOT / "state.json"
DOWNLOAD_ROOT = RUNTIME_ROOT / "downloads"
PARQUET_ROOT = RUNTIME_ROOT / "parquet"
FILES_ROOT = "paycom_parquet"
INTERVAL_SECONDS = 24 * 60 * 60

FEEDS: dict[str, dict[str, Any]] = {
    "employee_schedules": {"pattern": "*Employee Schedules*.csv", "table": "paycom_employee_schedules"},
    "theoria_shifts": {"pattern": "*Theoria-Shifts*.csv", "table": "paycom_theoria_shifts"},
    "cohorts": {"pattern": "*Cohorts*PaycomID*.csv", "table": "paycom_cohorts"},
    "pto": {"pattern": "*Time Off*.csv", "table": "paycom_pto_raw"},
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def credentials() -> tuple[str, str]:
    values: dict[str, str] = {}
    file_name = (os.environ.get("SFTP_CREDENTIAL_FILE") or "").strip()
    if file_name:
        path = Path(file_name).expanduser()
        if path.is_file():
            for line in path.read_text(encoding="utf-8").splitlines():
                if "=" in line and not line.lstrip().startswith("#"):
                    key, value = line.split("=", 1)
                    values[key.strip()] = value.strip().strip("\"'")
    user = (os.environ.get("SFTP_UN") or values.get("SFTP_UN") or "").strip()
    password = os.environ.get("SFTP_PW") or values.get("SFTP_PW") or ""
    if not user or not password:
        raise RuntimeError("SFTP_UN/SFTP_PW or SFTP_CREDENTIAL_FILE must be configured")
    return user, password


def open_sftp(user: str, password: str) -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    if not KNOWN_HOSTS.is_file():
        raise RuntimeError(f"Verified known-hosts file is missing: {KNOWN_HOSTS}")
    client = paramiko.SSHClient()
    client.load_host_keys(str(KNOWN_HOSTS))
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(SFTP_HOST, port=SFTP_PORT, username=user, password=password, look_for_keys=False, allow_agent=False, timeout=30, banner_timeout=30, auth_timeout=30)
    return client, client.open_sftp()


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"schema_version": 1, "files": {}, "last_success_utc": None}
    try:
        value = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if isinstance(value, dict) and isinstance(value.get("files"), dict):
            return value
    except Exception:
        logging.warning("State file could not be read; starting with empty state")
    return {"schema_version": 1, "files": {}, "last_success_utc": None}


def save_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = STATE_PATH.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(STATE_PATH)


def list_remote(user: str, password: str) -> list[dict[str, Any]]:
    client, sftp = open_sftp(user, password)
    try:
        entries = sftp.listdir_attr(SFTP_REMOTE_DIR)
        result: list[dict[str, Any]] = []
        for entry in entries:
            if not stat.S_ISREG(entry.st_mode) or not entry.filename.lower().endswith(".csv"):
                continue
            for feed, spec in FEEDS.items():
                import fnmatch
                if fnmatch.fnmatchcase(entry.filename.lower(), str(spec["pattern"]).lower()):
                    result.append({"feed": feed, "name": entry.filename, "remote_path": posixpath.join(SFTP_REMOTE_DIR, entry.filename), "mtime": int(entry.st_mtime), "size": int(entry.st_size)})
                    break
        return sorted(result, key=lambda x: (x["mtime"], x["name"]))
    finally:
        try:
            sftp.close()
        finally:
            client.close()


def copy_remote(item: dict[str, Any], user: str, password: str) -> Path:
    batch = DOWNLOAD_ROOT / now_utc().strftime("%Y%m%dT%H%M%SZ")
    batch.mkdir(parents=True, exist_ok=True)
    destination = batch / Path(str(item["name"])).name
    partial = destination.with_suffix(destination.suffix + ".part")
    last_error: Exception | None = None
    for attempt in range(1, 4):
        client = sftp = None
        try:
            client, sftp = open_sftp(user, password)
            sftp.get(str(item["remote_path"]), str(partial))
            last_error = None
            break
        except Exception as exc:
            last_error = exc
            try:
                if partial.exists():
                    partial.unlink()
            except OSError:
                pass
            if attempt < 3:
                logging.warning("SFTP download attempt %s/3 failed for %s; retrying", attempt, item["name"])
                time.sleep(min(30, attempt * 5))
        finally:
            if sftp is not None:
                sftp.close()
            if client is not None:
                client.close()
    if last_error is not None:
        raise last_error
    if partial.stat().st_size != int(item["size"]):
        raise RuntimeError(f"Downloaded size mismatch for {item['name']}")
    partial.replace(destination)
    return destination


def local_candidates(root: Path) -> list[dict[str, Any]]:
    import fnmatch
    result: list[dict[str, Any]] = []
    for path in root.glob("*.csv"):
        for feed, spec in FEEDS.items():
            if fnmatch.fnmatchcase(path.name.lower(), str(spec["pattern"]).lower()):
                st = path.stat()
                result.append({"feed": feed, "name": path.name, "remote_path": f"local://{path}", "mtime": int(st.st_mtime), "size": int(st.st_size), "local_path": path})
                break
    return sorted(result, key=lambda x: (x["mtime"], x["name"]))


def sanitize_column(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in (value or "").strip())
    cleaned = "_".join(part for part in cleaned.split("_") if part)
    return (cleaned or "col")[:128]


def unique_columns(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    result: list[str] = []
    for header in headers:
        base = sanitize_column(header)
        key = base.lower(); count = seen.get(key, 0)
        name = base if count == 0 else f"{base[:124]}_{count}"
        while name.lower() in seen:
            count += 1; name = f"{base[:124-len(str(count))-1]}_{count}"
        seen[key] = count + 1; seen[name.lower()] = 1; result.append(name)
    return result


def read_csv(path: Path, source_path: str, feed: str) -> pa.Table:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = reader.fieldnames or []
        mapped = unique_columns(headers)
        rows = list(reader)
    loaded = now_utc().replace(tzinfo=None)
    timestamp = None
    try:
        timestamp = datetime.strptime(path.name[:14], "%Y%m%d%H%M%S")
    except Exception:
        pass
    names = ["csv_filename", "source_file_path", "source_file_timestamp", "loaded_at_utc", "row_number", "payload_json"] + mapped
    output: dict[str, list[Any]] = {name: [] for name in names}
    for index, row in enumerate(rows, start=1):
        normalized = {header: (str(row.get(header) or "").replace("\x00", "").strip() or None) for header in headers}
        output["csv_filename"].append(path.name)
        output["source_file_path"].append(source_path)
        output["source_file_timestamp"].append(timestamp)
        output["loaded_at_utc"].append(loaded)
        output["row_number"].append(index)
        output["payload_json"].append(json.dumps(normalized, ensure_ascii=False, sort_keys=True))
        for header, name in zip(headers, mapped):
            output[name].append(normalized.get(header))
    arrays: dict[str, pa.Array] = {}
    for name in names:
        if name in {"source_file_timestamp", "loaded_at_utc"}:
            arrays[name] = pa.array(output[name], type=pa.timestamp("us"))
        elif name == "row_number":
            arrays[name] = pa.array(output[name], type=pa.int32())
        else:
            arrays[name] = pa.array(output[name], type=pa.string())
    return pa.table(arrays)


def storage_token() -> str:
    if AzureCliCredential is None:
        raise RuntimeError("azure-identity is required for Fabric Lakehouse writes")
    for credential_type in (AzureCliCredential, ManagedIdentityCredential, EnvironmentCredential, DefaultAzureCredential):
        try:
            return credential_type().get_token("https://storage.azure.com/.default").token
        except Exception:
            continue
    raise RuntimeError("Azure storage authentication is unavailable")


def table_uri(table: str) -> str:
    # IDs are required here because the current tenant contains another workspace
    # with the same friendly name. This avoids ambiguous OneLake resolution.
    return f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables/{SCHEMA}/{table}"


def existing_schema(uri: str) -> pa.Schema | None:
    try:
        from deltalake import DeltaTable
        token = storage_token()
        return DeltaTable(uri, storage_options={"azure_storage_account_name": "onelake", "azure_storage_token": token, "use_fabric_endpoint": "true"}).schema().to_pyarrow()
    except Exception:
        return None


def align_table(table: pa.Table, schema: pa.Schema | None) -> pa.Table:
    if schema is None:
        return table
    columns: list[pa.Array] = []
    for field in schema:
        if field.name in table.column_names:
            columns.append(table[field.name].cast(field.type, safe=False))
        else:
            columns.append(pa.nulls(table.num_rows, type=field.type))
    return pa.table(columns, schema=schema)


def write_delta(table: pa.Table, table_name: str) -> None:
    from deltalake import write_deltalake
    token = storage_token()
    uri = table_uri(table_name)
    schema = existing_schema(uri)
    aligned = align_table(table, schema)
    options = {"azure_storage_account_name": "onelake", "azure_storage_token": token, "use_fabric_endpoint": "true"}
    kwargs: dict[str, Any] = {"mode": "append", "storage_options": options}
    if schema is None:
        kwargs["schema_mode"] = "merge"
    write_deltalake(uri, aligned, **kwargs)


def upload_files_copy(table: pa.Table, feed: str, source_name: str) -> str:
    path = PARQUET_ROOT / feed / (hashlib.sha256(source_name.encode()).hexdigest() + ".parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="snappy")
    token = storage_token()
    relative = f"{FILES_ROOT}/{FEEDS[feed]['table']}/{path.name}"
    base = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{LAKEHOUSE_ID}/Files"
    url = f"{base}/{relative}"
    headers = {"Authorization": f"Bearer {token}", "x-ms-version": "2021-12-02", "Content-Type": "application/octet-stream"}
    parent = "/".join(relative.split("/")[:-1])
    built: list[str] = []
    for part in parent.split("/"):
        built.append(part)
        response = requests.put(f"{base}/{'/'.join(built)}?resource=directory", headers=headers, timeout=120)
        if response.status_code not in {200, 201, 202, 409}:
            raise RuntimeError(f"Lakehouse directory creation failed ({response.status_code})")
    response = requests.put(f"{url}?resource=file", headers=headers, timeout=120)
    if response.status_code not in {200, 201, 202}:
        raise RuntimeError(f"Lakehouse file creation failed ({response.status_code})")
    position = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            response = requests.patch(f"{url}?action=append&position={position}", headers=headers, data=chunk, timeout=300)
            if response.status_code not in {200, 202}:
                raise RuntimeError(f"Lakehouse file append failed ({response.status_code})")
            position += len(chunk)
    response = requests.patch(f"{url}?action=flush&position={position}&close=true", headers=headers, timeout=300)
    if response.status_code not in {200, 202}:
        raise RuntimeError(f"Lakehouse file flush failed ({response.status_code})")
    return relative


def process_item(item: dict[str, Any], state: dict[str, Any], user: str | None, password: str | None) -> None:
    key = str(item["remote_path"])
    signature = f"{item['mtime']}:{item['size']}"
    prior = state["files"].get(key) or {}
    if prior.get("signature") == signature:
        return
    local_path = Path(item.get("local_path") or copy_remote(item, user or "", password or ""))
    arrow_table = read_csv(local_path, key, str(item["feed"]))
    table_name = str(FEEDS[str(item["feed"])]["table"])
    write_delta(arrow_table, table_name)
    files_path = upload_files_copy(arrow_table, str(item["feed"]), key)
    state["files"][key] = {"signature": signature, "feed": item["feed"], "table": table_name, "rows": arrow_table.num_rows, "managed_table": f"{SCHEMA}.{table_name}", "parquet_directory": f"Files/{FILES_ROOT}/{table_name}", "parquet_path": files_path, "completed_at_utc": now_utc().isoformat()}
    save_state(state)
    logging.info("Published %s rows from %s to %s.%s", arrow_table.num_rows, item["name"], SCHEMA, table_name)


def run_once(local_root: Path | None, bootstrap: str) -> int:
    state = load_state()
    user = password = None
    if local_root is None:
        user, password = credentials()
        items = list_remote(user, password)
    else:
        items = local_candidates(local_root)
    if not items:
        logging.info("No Paycom SFTP feed files found")
        return 0
    if not state["files"] and bootstrap == "latest":
        # Existing cohorts are already represented in paycom.paycom_cohorts.
        # Bootstrap the current file for each feed, while marking older files
        # already present in the SFTP archive as known. This prevents a first
        # production start from replaying an unbounded historical archive.
        latest_by_feed: dict[str, dict[str, Any]] = {}
        for item in items:
            latest_by_feed[str(item["feed"])] = item
        for item in items:
            feed = str(item["feed"])
            if feed == "cohorts" or latest_by_feed.get(feed) is not item:
                key = str(item["remote_path"])
                state["files"][key] = {"signature": f"{item['mtime']}:{item['size']}", "feed": feed, "seeded_existing": True, "table": str(FEEDS[feed]["table"]), "seeded_at_utc": now_utc().isoformat()}
        items = [item for feed, item in latest_by_feed.items() if feed != "cohorts"]
        state["bootstrap_completed_utc"] = now_utc().isoformat()
        save_state(state)
    failures = 0
    for item in items:
        try:
            process_item(item, state, user, password)
        except Exception:
            failures += 1
            logging.exception("Failed consolidated item %s", item.get("name"))
    if failures == 0:
        state["last_success_utc"] = now_utc().isoformat()
    else:
        state["last_failed_utc"] = now_utc().isoformat()
        logging.error("Consolidated cycle completed with %s failed item(s); they will be retried", failures)
    save_state(state)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument("--interval-hours", type=float, default=24.0)
    parser.add_argument("--local-root", type=Path, default=None, help="Use a local staged SFTP directory instead of remote SFTP")
    parser.add_argument("--bootstrap", choices=("latest", "all"), default="latest", help="Initial state behavior")
    args = parser.parse_args()
    RUNTIME_ROOT.mkdir(parents=True, exist_ok=True); DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True); PARQUET_ROOT.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)sZ %(levelname)s %(message)s", handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(RUNTIME_ROOT / "worker.log", encoding="utf-8")])
    while True:
        try:
            run_once(args.local_root, args.bootstrap)
        except Exception:
            logging.exception("Consolidated Paycom cycle failed")
            if args.once:
                return 1
        if args.once:
            return 0
        time.sleep(max(300.0, float(args.interval_hours) * 3600.0))


if __name__ == "__main__":
    raise SystemExit(main())
