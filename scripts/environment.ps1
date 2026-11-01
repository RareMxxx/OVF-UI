$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$cargoDirectory = Join-Path $env:USERPROFILE '.cargo\bin'
if (Test-Path -LiteralPath $cargoDirectory) { $env:PATH = $cargoDirectory + ';' + $env:PATH }
$llvmDirectory = Get-ChildItem -LiteralPath (Join-Path $projectRoot '.tools') -Directory -Filter 'llvm-mingw-*-ucrt-x86_64' -ErrorAction SilentlyContinue | Select-Object -First 1
$script:CargoToolchain = @()
if ($llvmDirectory) {
    $env:PATH = (Join-Path $llvmDirectory.FullName 'bin') + ';' + $env:PATH
    $env:CC = 'x86_64-w64-mingw32-clang'
    $env:CARGO_TARGET_X86_64_PC_WINDOWS_GNULLVM_LINKER = 'x86_64-w64-mingw32-clang'
    $script:CargoToolchain = @('+stable-x86_64-pc-windows-gnullvm')
}
Set-Location -LiteralPath $projectRoot
