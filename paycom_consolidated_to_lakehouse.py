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
INTERVAL_SECONDS = 60 * 60
LOAD_TIMEOUT_SECONDS = int(os.environ.get("PAYCOM_FABRIC_LOAD_TIMEOUT_SECONDS", "900"))
# In the deployed UAT tenant, paycom.paycom_cohorts is a read-only shortcut
# backed by the existing tm_charteasy Warehouse table. The dedicated cohorts
# watcher owns its append path. Set this to 0 only when a writable schemas-
# enabled table has been provisioned and the Fabric Load Schema Table API is
# authorized for this identity.
COHORTS_EXTERNAL_LOADER = os.environ.get("PAYCOM_COHORTS_EXTERNAL_LOADER", "1").strip().lower() not in {"0", "false", "no"}
_default_external_state = PACKAGE_ROOT.parent / "orig_ai_mips" / "ai-mips" / "state" / "create_paycom_cohorts_bronze_fabric_table_state.json"
COHORTS_EXTERNAL_STATE = Path(os.environ.get("PAYCOM_COHORTS_EXTERNAL_STATE_FILE", str(_default_external_state))).expanduser()

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


def external_cohorts_processed(remote_path: str, signature: str) -> bool:
    """Check the dedicated cohorts watcher's durable receipt, if available."""
    if not COHORTS_EXTERNAL_STATE.is_file():
        return False
    try:
        payload = json.loads(COHORTS_EXTERNAL_STATE.read_text(encoding="utf-8"))
        receipt = str((payload.get("processed_files") or {}).get(remote_path, ""))
        if receipt == signature:
            return True
        # The legacy watcher stores SFTP mtime in nanoseconds while the
        # consolidated worker stores seconds. Compare normalized integer mtime.
        wanted_mtime, _, wanted_size = signature.partition(":")
        receipt_mtime, _, receipt_size = receipt.partition(":")
        return bool(wanted_size and wanted_size == receipt_size and receipt_mtime.isdigit() and wanted_mtime.isdigit() and int(receipt_mtime) // 1_000_000_000 == int(wanted_mtime))
    except Exception:
        logging.warning("Could not inspect the dedicated cohorts state file; leaving source pending")
        return False


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


def fabric_api_token() -> str:
    """Return a token for the Fabric REST API using the VM's existing identity."""
    if AzureCliCredential is None:
        raise RuntimeError("azure-identity is required for Fabric table loads")
    for credential_type in (AzureCliCredential, ManagedIdentityCredential, EnvironmentCredential, DefaultAzureCredential):
        try:
            return credential_type().get_token("https://api.fabric.microsoft.com/.default").token
        except Exception:
            continue
    raise RuntimeError("Fabric REST authentication is unavailable")


def table_uri(table: str) -> str:
    # IDs are required here because the current tenant contains another workspace
    # with the same friendly name. This avoids ambiguous OneLake resolution.
    return f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables/{SCHEMA}/{table}"


def existing_schema(uri: str) -> pa.Schema | None:
    try:
        from deltalake import DeltaTable
        token = storage_token()
        delta_schema = DeltaTable(uri, storage_options={"azure_storage_account_name": "onelake", "azure_storage_token": token, "use_fabric_endpoint": "true"}).schema()
        # Delta Lake clients before 1.6 expose to_pyarrow(); newer clients
        # expose to_arrow(). Support both so existing column-mapped tables are
        # aligned instead of incorrectly entering schema-evolution mode.
        converter = getattr(delta_schema, "to_pyarrow", None)
        if converter is not None:
            return converter()
        # Delta Lake 1.x returns an arro3 Schema. Convert the small set of
        # primitive types used by these raw feeds without requiring an arro3 /
        # PyArrow bridge in the runtime.
        definition = json.loads(delta_schema.to_json()) if hasattr(delta_schema, "to_json") else delta_schema.json()
        primitive = {
            "string": pa.string(), "binary": pa.binary(), "byte": pa.int8(),
            "short": pa.int16(), "integer": pa.int32(), "long": pa.int64(),
            "float": pa.float32(), "double": pa.float64(), "boolean": pa.bool_(),
            "date": pa.date32(), "timestamp": pa.timestamp("us"),
            "timestamp_ntz": pa.timestamp("us"),
        }
        fields = []
        for field in definition.get("fields", []):
            field_type = field.get("type")
            if isinstance(field_type, str):
                arrow_type = primitive.get(field_type)
                if arrow_type is None and field_type.startswith("decimal("):
                    precision, scale = (int(x) for x in field_type[8:-1].split(","))
                    arrow_type = pa.decimal128(precision, scale)
                if arrow_type is None:
                    raise RuntimeError(f"Unsupported Delta field type: {field_type}")
            else:
                raise RuntimeError("Unsupported nested Delta field type")
            fields.append(pa.field(field["name"], arrow_type, nullable=bool(field.get("nullable", True))))
        return pa.schema(fields)
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
    relative = f"{FILES_ROOT}/{FEEDS[feed]['table']}/{path.name}"
    upload_local_file(path, relative)
    return relative


def upload_local_file(path: Path, relative: str) -> str:
    """Upload a local file to Files using the OneLake filesystem API."""
    token = storage_token()
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


def load_schema_table(table_name: str, source_path: Path, *, mode: str = "Append", format_name: str = "Csv") -> str:
    """Load a CSV into a schema-enabled Lakehouse table through Fabric's API.

    Schema-enabled tables and shortcuts reject direct Delta PUT/append calls.
    The supported table-load operation accepts a Files-relative CSV and returns
    an operation URL that must be polled before recording success.
    """
    relative = f"paycom_ingest/{table_name}/{source_path.name}"
    upload_local_file(source_path, relative)
    endpoint = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}"
        f"/lakehouses/{LAKEHOUSE_ID}/schemas/{SCHEMA}/tables/{table_name}/load?beta=true"
    )
    format_options: dict[str, Any] = {"format": format_name}
    if format_name.lower() == "csv":
        format_options.update({"header": True, "delimiter": ","})
    body = {
        "relativePath": f"Files/{relative}",
        "pathType": "File",
        "mode": mode,
        "formatOptions": format_options,
    }
    response = requests.post(
        endpoint,
        headers={"Authorization": f"Bearer {fabric_api_token()}", "Content-Type": "application/json"},
        json=body,
        timeout=60,
    )
    if response.status_code not in {200, 201, 202}:
        raise RuntimeError(f"Fabric table load submission failed ({response.status_code}): {response.text[:500]}")
    operation_url = response.headers.get("Location")
    if not operation_url:
        # Some successful responses are synchronous and contain no operation.
        return "synchronous"
    operation_id = operation_url.rstrip("/").rsplit("/", 1)[-1]
    deadline = time.monotonic() + LOAD_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        status_response = requests.get(
            operation_url,
            headers={"Authorization": f"Bearer {fabric_api_token()}"},
            timeout=60,
        )
        if status_response.status_code >= 400:
            raise RuntimeError(f"Fabric table load status failed ({status_response.status_code}): {status_response.text[:500]}")
        payload = status_response.json() if status_response.content else {}
        status = str(payload.get("status", payload.get("Status", ""))).strip().lower()
        if status in {"completed", "success", "succeeded", "done", "3"}:
            return operation_id
        if status in {"failed", "error", "canceled", "cancelled", "4", "5"}:
            detail = payload.get("error") or payload.get("message") or status_response.text[:500]
            raise RuntimeError(f"Fabric table load operation failed ({operation_id}): {detail}")
        time.sleep(5)
    raise TimeoutError(f"Fabric table load operation timed out ({operation_id})")


def normalized_parquet(table: pa.Table, table_name: str, source_name: str) -> tuple[Path, pa.Table]:
    """Write a schema-aligned, typed Parquet file for the table-load API."""
    aligned = align_table(table, existing_schema(table_uri(table_name)))
    destination = RUNTIME_ROOT / "lakehouse_load" / table_name / (
        hashlib.sha256(source_name.encode("utf-8")).hexdigest() + ".parquet"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(aligned, destination, compression="snappy")
    return destination, aligned


def process_item(item: dict[str, Any], state: dict[str, Any], user: str | None, password: str | None) -> None:
    key = str(item["remote_path"])
    signature = f"{item['mtime']}:{item['size']}"
    prior = state["files"].get(key) or {}
    if prior.get("signature") == signature:
        if prior.get("status") == "parquet_snapshot_waiting_external_loader" and external_cohorts_processed(key, signature):
            prior.update({"status": "loaded", "load_method": "Dedicated cohorts watcher -> tm_charteasy.dbo.paycom_cohorts_bronze (Warehouse-backed shortcut)", "completed_at_utc": now_utc().isoformat()})
            save_state(state)
            logging.info("Dedicated cohorts watcher receipt confirmed for %s", item["name"])
        return
    local_path = Path(item.get("local_path") or copy_remote(item, user or "", password or ""))
    arrow_table = read_csv(local_path, key, str(item["feed"]))
    table_name = str(FEEDS[str(item["feed"])]["table"])
    # The deployed cohorts object is a Warehouse-backed shortcut. Its existing
    # dedicated watcher appends the source to tm_charteasy.dbo.paycom_cohorts_bronze,
    # which is reflected in paycom.paycom_cohorts. Keep a complete Parquet copy
    # here and do not issue futile direct-Delta/API writes to that shortcut.
    if str(item["feed"]) == "cohorts":
        if COHORTS_EXTERNAL_LOADER:
            _, aligned = normalized_parquet(arrow_table, table_name, key)
            files_path = upload_files_copy(aligned, str(item["feed"]), key)
            loaded = external_cohorts_processed(key, signature)
            state["files"][key] = {"signature": signature, "feed": item["feed"], "table": table_name, "rows": arrow_table.num_rows, "managed_table": f"{SCHEMA}.{table_name}", "parquet_directory": f"Files/{FILES_ROOT}/{table_name}", "parquet_path": files_path, "status": "loaded" if loaded else "parquet_snapshot_waiting_external_loader", "load_method": "Dedicated cohorts watcher -> tm_charteasy.dbo.paycom_cohorts_bronze (Warehouse-backed shortcut)" if loaded else "Dedicated cohorts watcher pending", "external_state_file": str(COHORTS_EXTERNAL_STATE), "completed_at_utc": now_utc().isoformat() if loaded else None}
            save_state(state)
            logging.info("Retained %s rows from %s as Parquet; dedicated cohorts SQL receipt=%s", arrow_table.num_rows, item["name"], loaded)
            return
        load_parquet, aligned = normalized_parquet(arrow_table, table_name, key)
        operation_id = load_schema_table(table_name, load_parquet, format_name="Parquet")
        files_path = upload_files_copy(aligned, str(item["feed"]), key)
        state["files"][key] = {"signature": signature, "feed": item["feed"], "table": table_name, "rows": arrow_table.num_rows, "managed_table": f"{SCHEMA}.{table_name}", "parquet_directory": f"Files/{FILES_ROOT}/{table_name}", "parquet_path": files_path, "status": "loaded", "load_method": "Fabric Load Table API", "load_operation_id": operation_id, "completed_at_utc": now_utc().isoformat()}
        save_state(state)
        logging.info("Loaded %s rows from %s to %s.%s via Fabric Load Table API (%s)", arrow_table.num_rows, item["name"], SCHEMA, table_name, operation_id)
        return
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
    parser.add_argument("--interval-hours", type=float, default=1.0)
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
