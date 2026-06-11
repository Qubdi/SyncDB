# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 2.x     | ✅        |
| 1.x     | ❌        |

## Reporting a vulnerability

Email **qubdisolutions@gmail.com** with a description of the issue, steps to
reproduce, and the affected version.  Please do not open a public GitHub issue
for security reports.  You should receive an acknowledgement within a few days.

## Threat model and built-in protections

SyncDB executes SQL built from **developer-authored job configs**.  It is not
designed to accept identifiers, filter strings, or file paths directly from
untrusted end users.

What the library enforces:

- **Identifiers** (schema/table/column names, renames) are allowlist-validated
  (`[A-Za-z_][A-Za-z0-9_]*`) before being quoted and embedded in SQL.
- **Type overrides** are validated against a strict type-shape regex before
  landing in DDL.
- **Data values** are always parameterised — never interpolated into SQL text.
- **Raw WHERE filters** are screened by a deny-list (string literals stripped,
  keyword matching on word boundaries, comment/terminator tokens rejected).
  This is a safety net, **not a parser**: if a filter could ever contain
  untrusted input, use the parameterised form
  `{"where": "col = %s", "params": [value]}` instead of a raw string.
- **ODBC connection-string values** are brace-escaped so passwords containing
  `;` or `=` cannot inject extra connection attributes.
- **Pickle files** execute arbitrary code on load.  SyncDB warns
  (`PickleSecurityWarning`) when loading unverified pickles; pass `hmac_key=`
  to `FileTransfer.read/write` or `SyncDB.import_file_to_table` to enforce
  HMAC-SHA256 integrity verification.  Never import pickle files from sources
  you do not control.

## Credential handling

- `DatabaseConfig.password` is excluded from `repr()` output.
- Use `DatabaseConfig.from_env()` so credentials come from environment
  variables (populated from a secrets manager) instead of source code or
  config files checked into version control.

## Dependency auditing

The library has no required runtime dependencies; drivers are optional extras.
Audit your installed environment with:

```bash
pip install pip-audit
pip-audit
```
