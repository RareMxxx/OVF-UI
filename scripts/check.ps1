. (Join-Path $PSScriptRoot 'environment.ps1')
& cargo @CargoToolchain fmt --all -- --check
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& cargo @CargoToolchain test --locked --features test-fixtures
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& cargo @CargoToolchain clippy --locked --all-targets --features test-fixtures -- -D warnings
exit $LASTEXITCODE
