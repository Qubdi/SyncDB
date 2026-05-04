# Usage:
#   .\run_tests.ps1              - run all tests
#   .\run_tests.ps1 sync         - run a specific suite
#   .\run_tests.ps1 sync -v      - run with verbose output
#   .\run_tests.ps1 sync -live   - show colored start/end test report
#   .\run_tests.ps1 db -detail   - show colored report plus SyncDB progress/summaries
#
# Available suites: config, connectors, files, progress, sql, sync, type_mapping, db
# Prerequisite from repo root: pip install -e ".[dev]"

param(
    [string]$Suite = "all",
    [switch]$v,
    [switch]$live,
    [switch]$detail
)

$suites = @{
    config       = "Tests/Library/Components/config"
    connectors   = "Tests/Library/Components/connectors"
    files        = "Tests/Library/Components/files"
    progress     = "Tests/Library/Components/progress"
    sql          = "Tests/Library/Components/sql"
    sync         = "Tests/Library/Components/sync"
    type_mapping = "Tests/Library/Components/type_mapping"
    db           = "Tests/Library/DatabaseToDatabase"
}

$pytest_args = @()
if ($v) { $pytest_args += "-v" }
if ($live) { $pytest_args += "--syncdb-live-output" }
if ($detail) { $pytest_args += "--syncdb-live-output-detail" }

if ($Suite -eq "all") {
    pytest @pytest_args
} elseif ($suites.ContainsKey($Suite)) {
    pytest $suites[$Suite] @pytest_args
} else {
    Write-Error "Unknown suite '$Suite'. Available: $($suites.Keys -join ', ')"
    exit 1
}
