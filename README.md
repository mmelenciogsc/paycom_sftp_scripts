# Paycom SFTP loaders

This directory is a portable bundle for the Paycom SFTP-to-Fabric loaders used by FABRIC8. The canonical production path is the consolidated worker described below.

## Canonical consolidated destinations

All four recognized feeds use the same UAT Lakehouse and schema:

- Workspace: `ws_uat`
- Lakehouse: `lh_uat_theoria_bronze`
- Schema: `paycom`
- Managed Delta/SQL tables: `paycom.paycom_employee_schedules`, `paycom.paycom_theoria_shifts`, `paycom.paycom_cohorts`, and `paycom.paycom_pto_raw`
- Reviewable Parquet copies: `Files/paycom_parquet/paycom_employee_schedules/`, `Files/paycom_parquet/paycom_theoria_shifts/`, `Files/paycom_parquet/paycom_cohorts/`, and `Files/paycom_parquet/paycom_pto_raw/`

The managed Delta tables are the Lakehouse SQL-endpoint counterparts. Fabric may take time to expose a newly created Delta table through `INFORMATION_SCHEMA`; the OneLake Delta log and the SQL endpoint are both checked during validation.

`paycom_pto_raw` is the explicit raw PTO/time-off destination requested for the existing `paycom` schema. The other table names intentionally identify their feed without abbreviations.

- `paycom_sftp2fabric.py` loads recognized Employee Schedules and Theoria Shifts exports into the existing daily Fabric Warehouse tables.
- `paycom_pto_sftp_to_fabric.py` loads new PTO/time-off CSV exports into the dedicated UAT Lakehouse and Warehouse destination (`paycom_pto_raw`).
- `paycom_cohorts_to_fabric.py` watches `Cohorts-PaycomID` exports and appends them to `tm_charteasy.dbo.paycom_cohorts_bronze`.

The older scripts remain available for compatibility and historical recovery. They are not the canonical recurring path after `paycom_consolidated_to_lakehouse.py` is enabled.

The bundle includes the local `FDI_z/utils.py` helper required by the daily loader, the verified Paycom host-key file, and the report logo used by the existing loader. No credentials are included.

## Prerequisites

- Python 3.11 or newer.
- Python packages from `requirements.txt`.
- Microsoft ODBC Driver 18 for SQL Server (`pyodbc`) for Warehouse operations.
- An Azure CLI login, managed identity, or other supported `azure.identity` credential with access to the configured Fabric workspace/items.
- Network access to the Paycom SFTP host and the configured Fabric endpoints.

Install Python dependencies with:

```bash
python -m pip install -r requirements.txt
```

## Credentials and host verification

Provide SFTP credentials through a protected file or environment variables. The recommended file contains `SFTP_UN=...` and `SFTP_PW=...`, is outside source control, and has mode `600`:

```bash
cp .env.example sftp.env
chmod 600 sftp.env
export SFTP_CREDENTIAL_FILE="$PWD/sftp.env"
```

Alternatively set `SFTP_UN` and `SFTP_PW` directly. Do not commit `sftp.env`, passwords, tokens, or other secrets. `paycom_known_hosts` is public trust material for the Paycom host key; replace it only after independently verifying the new fingerprint. You may override it with `SFTP_KNOWN_HOSTS_PATH`.

## Daily Employee Schedules/Shifts loader

Run one polling cycle:

```bash
python paycom_sftp2fabric.py --run-once --check-mode auto --server "$PAYCOM_FABRIC_SERVER"
```

Run continuously with a 24-hour interval:

```bash
python paycom_sftp2fabric.py --interval 86400 --check-mode auto --server "$PAYCOM_FABRIC_SERVER"
```

The default working directory is `./runtime`; override it with `PAYCOM_SFTP_WORK_ROOT`. The existing target database/table names and the default Fabric server are retained for the FABRIC8 environment; use `PAYCOM_FABRIC_SERVER` or `--server` for another tenant.

## PTO/time-off loader

Run one cycle:

```bash
python paycom_pto_sftp_to_fabric.py --once
```

Run continuously with a 24-hour interval:

```bash
python paycom_pto_sftp_to_fabric.py --interval-hours 24
```

The PTO loader defaults to FABRIC8's dedicated UAT workspace, Lakehouse, Warehouse, database, and `paycom_pto_raw` table. For another environment, override the `PAYCOM_PTO_*` variables shown in `.env.example`. Its default working directory is `./runtime_pto`; override it with `PAYCOM_PTO_RUNTIME_ROOT`.

## Canonical consolidated worker

The consolidated worker checks all four feeds every 24 hours, downloads only unseen source signatures, appends normalized records to the matching `paycom` managed table, and writes a Parquet review copy under the matching `Files/paycom_parquet/<table>/` directory. It uses the workspace and Lakehouse IDs, avoiding ambiguity when friendly names are duplicated in the tenant. The pre-existing `paycom_cohorts` table is a schema-enabled shortcut that rejects direct OneLake appends; for that table the worker persists the new source as a clearly marked Parquet snapshot and records the SQL append as pending for the Fabric table-load path rather than retrying an impossible write.

One cycle:

```bash
python paycom_consolidated_to_lakehouse.py --once --bootstrap latest
```

Continuous 24-hour worker:

```bash
python paycom_consolidated_to_lakehouse.py --interval-hours 24
```

For the live UAT worker, set `AZURE_CONFIG_DIR` to the approved Azure CLI configuration, provide `SFTP_CREDENTIAL_FILE`, and set `SFTP_KNOWN_HOSTS_PATH`. The worker stores its durable source-signature state and logs under `PAYCOM_CONSOLIDATED_RUNTIME_ROOT`.

## Cohorts-PaycomID loader

Run one remote check and exit:

```bash
python paycom_cohorts_to_fabric.py --run-once
```

Run continuously with a 24-hour interval:

```bash
python paycom_cohorts_to_fabric.py --interval-hours 24
```

The default matching pattern is `*Cohorts*PaycomID*.csv`. The loader uses source-file path and remote signature state to avoid duplicate appends, validates the inserted row count, and writes to the configured Fabric Warehouse table. Override the Fabric target with `PAYCOM_COHORTS_FABRIC_SERVER`, `PAYCOM_COHORTS_FABRIC_DATABASE`, `PAYCOM_COHORTS_FABRIC_SCHEMA`, and `PAYCOM_COHORTS_FABRIC_TABLE`. Its default state/log/temp root is `./runtime_cohorts`; override it with `PAYCOM_COHORTS_RUNTIME_ROOT`.

Both loaders retain source-file state and use source identity/content checks to make retries safe. They do not include or expose PHI/credentials beyond the source data required by their configured destinations.
