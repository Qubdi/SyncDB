# Usage:
#   .\run_tests.ps1              - run all tests
#   .\run_tests.ps1 sync         - run a specific suite
#   .\run_tests.ps1 sync -v      - run with verbose output
#
# Available suites: config, connectors, files, progress, sql, sync, type_mapping
# Prerequisite from repo root: pip install -e ".[dev]"

param(
    [string]$Suite = "all",
    [switch]$v
)

$suites = @{
    config       = "Tests/Library/config"
    connectors   = "Tests/Library/connectors"
    files        = "Tests/Library/files"
    progress     = "Tests/Library/progress"
    sql          = "Tests/Library/sql"
    sync         = "Tests/Library/sync"
    type_mapping = "Tests/Library/type_mapping"
}

$pytest_args = @()
if ($v) { $pytest_args += "-v" }

if ($Suite -eq "all") {
    pytest @pytest_args
} elseif ($suites.ContainsKey($Suite)) {
    pytest $suites[$Suite] @pytest_args
} else {
    Write-Error "Unknown suite '$Suite'. Available: $($suites.Keys -join ', ')"
    exit 1
}
