#!/usr/bin/env bash
set -e

# mssql-tools18 uses -C (trust server certificate) and lives in a different
# path than the legacy mssql-tools. Prefer tools18 when available.
if [ -x /opt/mssql-tools18/bin/sqlcmd ]; then
  SQLCMD="/opt/mssql-tools18/bin/sqlcmd -C"
else
  SQLCMD="/opt/mssql-tools/bin/sqlcmd"
fi

# Accept both naming conventions used by different MSSQL Docker images.
SQL_PASSWORD="${MSSQL_SA_PASSWORD:-$SA_PASSWORD}"

echo "Waiting for SQL Server..."
for i in {1..90}; do
  if $SQLCMD -S mssql -U sa -P "$SQL_PASSWORD" -Q "SELECT 1" >/dev/null 2>&1; then
    echo "SQL Server is ready."
    break
  fi

  if [ "$i" -eq 90 ]; then
    echo "SQL Server did not become ready in time."
    exit 1
  fi

  sleep 2
done

echo "Applying MSSQL seed data..."
$SQLCMD -S mssql -U sa -P "$SQL_PASSWORD" -i /seed/seed.sql
echo "MSSQL seed data applied."
