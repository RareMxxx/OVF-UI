param([string]$TargetDirectory = $env:CARGO_TARGET_DIR)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'environment.ps1')
$targetMetadata = & cargo @CargoToolchain metadata --no-deps --format-version 1 --locked
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if ([string]::IsNullOrWhiteSpace($TargetDirectory)) { $TargetDirectory = ($targetMetadata | ConvertFrom-Json).target_directory }
$TargetDirectory = [IO.Path]::GetFullPath($TargetDirectory, $projectRoot)
& (Join-Path $PSScriptRoot 'build-release.ps1') -TargetDirectory $TargetDirectory
$installedOutput = Join-Path $projectRoot 'dist\OVF-UI-0.5.0-windows-x64'
# Package only freshly copied, explicitly selected files. Never archive the
# runnable app directory: it may contain profiles, logs or user-added images.
$stagingRoot = Join-Path $projectRoot ('dist\.package-' + [guid]::NewGuid().ToString('N'))
$output = Join-Path $stagingRoot 'OVF-UI-0.5.0-windows-x64'
New-Item -ItemType Directory -Path $output -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $TargetDirectory 'release\ovf-ui.exe') -Destination $output
Copy-Item -LiteralPath (Join-Path $projectRoot 'README.md') -Destination $output
Copy-Item -LiteralPath (Join-Path $projectRoot 'CHANGELOG.md') -Destination $output
Copy-Item -LiteralPath (Join-Path $projectRoot 'FUNCTIONAL-REVIEW-0.5.0.txt') -Destination $output
Copy-Item -LiteralPath (Join-Path $projectRoot 'LICENSE') -Destination $output
$docsOutput = Join-Path $output 'docs'
New-Item -ItemType Directory -Path $docsOutput -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $projectRoot 'docs\GITHUB-RELEASES.md') -Destination $docsOutput
Copy-Item -LiteralPath (Join-Path $projectRoot 'docs\UI-REVIEW-0.5.0.txt') -Destination $docsOutput
if ($llvmDirectory) {
    Copy-Item -LiteralPath (Join-Path $llvmDirectory.FullName 'x86_64-w64-mingw32\bin\libunwind.dll') -Destination $output
    Copy-Item -LiteralPath (Join-Path $llvmDirectory.FullName 'LICENSE.TXT') -Destination (Join-Path $output 'LLVM-LICENSE.txt')
}
$metadataText = & cargo @CargoToolchain metadata --format-version 1 --locked
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
$metadata = $metadataText | ConvertFrom-Json
$notices = Join-Path $output 'third-party-licenses'
New-Item -ItemType Directory -Path $notices -Force | Out-Null
$metadata.packages | Select-Object name,version,license,repository | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $notices 'packages.json') -Encoding utf8
foreach ($package in $metadata.packages) {
    if ($package.name -eq 'ovf-dock') { continue }
    $packageRoot = Split-Path -Parent $package.manifest_path
    $licenseFiles = Get-ChildItem -LiteralPath $packageRoot -File -Recurse | Where-Object { $_.Name -match '^(LICENSE|LICENCE|COPYING|NOTICE|OFL)(\.|-|$)' }
    foreach ($licenseFile in $licenseFiles) {
        $relative = [IO.Path]::GetRelativePath($packageRoot, $licenseFile.FullName)
        $destination = Join-Path $notices ($package.name + '-' + $package.version + '\' + $relative)
        New-Item -ItemType Directory -Path (Split-Path -Parent $destination) -Force | Out-Null
        Copy-Item -LiteralPath $licenseFile.FullName -Destination $destination
    }
}
$zipPath = Join-Path $projectRoot 'dist\OVF-UI-0.5.0-windows-x64.zip'
& (Join-Path $PSScriptRoot 'check-build-paths.ps1') -Path $output -BuildDirectory $TargetDirectory
Compress-Archive -LiteralPath $output -DestinationPath $zipPath -Force
$checksum = (Get-FileHash -LiteralPath $zipPath -Algorithm SHA256).Hash.ToLowerInvariant()
($checksum + '  ' + [IO.Path]::GetFileName($zipPath)) | Set-Content -LiteralPath (Join-Path $projectRoot 'dist\SHA256SUMS.txt') -Encoding ascii
New-Item -ItemType Directory -Path $installedOutput -Force | Out-Null
Get-ChildItem -LiteralPath $output | Copy-Item -Destination $installedOutput -Recurse -Force
$resolvedStage = (Resolve-Path -LiteralPath $stagingRoot).Path
$resolvedDist = (Resolve-Path -LiteralPath (Join-Path $projectRoot 'dist')).Path
if ([IO.Path]::GetDirectoryName($resolvedStage) -ne $resolvedDist -or [IO.Path]::GetFileName($resolvedStage) -notmatch '^\.package-[0-9a-f]{32}$') {
    throw 'Unexpected package staging path; cleanup refused.'
}
Remove-Item -LiteralPath $resolvedStage -Recurse -Force
Write-Output "Application: $installedOutput\ovf-ui.exe"
Write-Output "Portable archive: $zipPath"
