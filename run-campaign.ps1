param(
    [ValidateSet('16', '32')][int[]]$Parallelism = @(16, 32),
    [string[]]$Scenario = @(),
    [double]$ProcessInterval = 0.5,
    [string]$Campaign = (Join-Path $PSScriptRoot ('results/celery-' + (Get-Date -Format 'yyyyMMdd-HHmmss')))
)

$Campaign = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Campaign)
$previousParallelism = $env:BENCH_PARALLELISM
$previousWorker = $env:BENCH_WORKER_CONCURRENCY
$previousInterval = $env:BENCH_PROCESS_INTERVAL
$env:BENCH_PROCESS_INTERVAL = "$ProcessInterval"
$cases = @(
    @{ args = @('noop') },
    @{ args = @('cpu', '--cpu-iterations', '5000000') },
    @{ args = @('io_http', '--delay', '2.5', '--io-bytes', '1048576') },
    @{ args = @('etl_csv', '--rows', '100000', '--cols', '12') },
    @{ args = @('api_ingest', '--pages', '8', '--page-rows', '1000', '--delay', '0.25') },
    @{ args = @('clickhouse_mart', '--days', '28') }
)

New-Item -ItemType Directory -Force -Path $Campaign | Out-Null
Push-Location $PSScriptRoot
try {
    $list = Join-Path $Campaign 'experiments.txt'
    $experiments = @(if (Test-Path -LiteralPath $list) { Get-Content -LiteralPath $list })
    @{parallelism=$Parallelism; sizes=@(8,16,32); process_interval=$ProcessInterval;
      cases=$cases; started_at=(Get-Date).ToUniversalTime().ToString('o')} |
        ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $Campaign 'campaign.json')
    foreach ($slots in $Parallelism) {
        $env:BENCH_PARALLELISM = "$slots"
        $env:BENCH_WORKER_CONCURRENCY = "$slots"
        docker compose up -d --wait --wait-timeout 150
        if ($LASTEXITCODE -ne 0) { throw "Airflow startup failed at parallelism=$slots" }
        # The observer must join the new worker PID namespace after concurrency changes.
        docker compose up -d --no-deps --force-recreate process-profiler
        if ($LASTEXITCODE -ne 0) { throw 'Process profiler startup failed' }
        foreach ($case in $cases) {
            $arguments = $case.args + @('--series', '8,16,32', '--repeats', '1', '--continue-on-failure')
            $name = $case.args[0]
            if ($Scenario.Count -and $name -notin $Scenario) { continue }
            $done = @($experiments | Where-Object {
                $tracePath = Join-Path $_ 'trace.json'
                if (!(Test-Path -LiteralPath $tracePath)) { return $false }
                $trace = Get-Content -Raw -LiteralPath $tracePath | ConvertFrom-Json
                $trace.config.parallelism -eq $slots -and $trace.config.scenario -eq $name -and
                @($trace.batches | Where-Object { $_.size -in @(8,16,32) -and $_.successful -eq $_.size -and $_.process_profile.available -and $_.measurement_quality.valid -ne $false }).Count -eq 3
            })
            if ($done.Count) { Write-Output "Already completed: parallelism=$slots $name"; continue }
            Write-Output "parallelism=$slots $($arguments -join ' ')"
            $log = Join-Path $Campaign "$slots-$name.log"
            if (Test-Path -LiteralPath $log) {
                $log = Join-Path $Campaign "$slots-$name-$(Get-Date -Format HHmmss).log"
            }
            docker compose run --rm --no-deps -T runner @arguments 2>&1 | Tee-Object -FilePath $log
            $benchmarkExit = $LASTEXITCODE
            $os = Get-CimInstance Win32_OperatingSystem
            @{at=(Get-Date).ToUniversalTime().ToString('o'); parallelism=$slots; scenario=$name;
              free_physical_kib=$os.FreePhysicalMemory; total_physical_kib=$os.TotalVisibleMemorySize} |
                ConvertTo-Json -Compress | Add-Content -LiteralPath (Join-Path $Campaign 'host-memory.jsonl')
            docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' |
                Set-Content -LiteralPath (Join-Path $Campaign "$slots-$name-containers.txt")
            $result = Select-String -LiteralPath $log -Pattern '^Results: (results/\S+)$' | Select-Object -Last 1
            if (!$result) { throw "No result path found in $log" }
            $experiments += $result.Matches[0].Groups[1].Value
            Add-Content -LiteralPath $list -Value $experiments[-1]
            python summarize_traces.py $Campaign @experiments
            if ($LASTEXITCODE -ne 0) { throw 'Could not create the comparison report' }
            if (Test-Path -LiteralPath (Join-Path $experiments[-1] 'error.json')) {
                throw "Infrastructure failure. No more scenarios will be started; see $($experiments[-1])/error.json"
            }
            $measured = Get-Content -Raw -LiteralPath (Join-Path $experiments[-1] 'trace.json') | ConvertFrom-Json
            if (@($measured.batches | Where-Object { $_.measurement_quality.valid -eq $false }).Count) {
                throw "Invalid resource measurement. Campaign stopped; see $($experiments[-1])/trace.html"
            }
            if ($benchmarkExit -ne 0) {
                Write-Warning "Scenario failed; diagnostic report saved in $($experiments[-1]). Continuing to next scenario."
            }
        }
    }
    Write-Output "Campaign: $Campaign"
    python -c "from pathlib import Path; from process_report import write_profile_index; write_profile_index(Path('results'))"
} finally {
    $env:BENCH_PARALLELISM = $previousParallelism
    $env:BENCH_WORKER_CONCURRENCY = $previousWorker
    $env:BENCH_PROCESS_INTERVAL = $previousInterval
    Pop-Location
}
