param([string]$TargetDirectory = $env:CARGO_TARGET_DIR)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'environment.ps1')

# This affects compiler diagnostics embedded in the executable, not runtime
# user directories, file dialogs, keyring identities or OVF Tool paths.
$metadataText = & cargo @CargoToolchain metadata --no-deps --format-version 1 --locked
if ($LASTEXITCODE -ne 0) { throw 'Could not resolve Cargo target directory.' }
$metadata = $metadataText | ConvertFrom-Json
if ([string]::IsNullOrWhiteSpace($TargetDirectory)) { $TargetDirectory = $metadata.target_directory }
$TargetDirectory = [IO.Path]::GetFullPath($TargetDirectory, $projectRoot)

$savedFlags = $env:CARGO_ENCODED_RUSTFLAGS
if (!$savedFlags -and $env:RUSTFLAGS) {
    throw 'For a private release build, pass custom flags via CARGO_ENCODED_RUSTFLAGS rather than RUSTFLAGS.'
}
$flags = [Collections.Generic.List[string]]::new()
if ($savedFlags) { $flags.AddRange([string[]]$savedFlags.Split([char]31)) }
$cargoRoot = if ($env:CARGO_HOME) { $env:CARGO_HOME } else { Join-Path $env:USERPROFILE '.cargo' }
$rustupRoot = if ($env:RUSTUP_HOME) { $env:RUSTUP_HOME } else { Join-Path $env:USERPROFILE '.rustup' }
# rustc applies the last matching mapping: specific roots follow the profile.
$mappings = @(
    @{ From = $env:USERPROFILE; To = '/build/user' },
    @{ From = $cargoRoot; To = '/build/cargo' },
    @{ From = $rustupRoot; To = '/build/rustup' },
    @{ From = $projectRoot; To = '/build/ovf-ui' },
    @{ From = $TargetDirectory; To = '/build/target' }
)
foreach ($mapping in $mappings) {
    $from = [IO.Path]::GetFullPath($mapping.From, $projectRoot).TrimEnd('\', '/')
    foreach ($variant in @($from, $from.Replace('\', '/'), ('\\?\' + $from))) {
        $flags.Add('--remap-path-prefix=' + $variant + '=' + $mapping.To)
    }
}
try {
    # Unit separators preserve spaces and non-ASCII characters in Windows paths.
    $env:CARGO_ENCODED_RUSTFLAGS = [string]::Join([char]31, $flags)
    & cargo @CargoToolchain build --release --locked --bin ovf-ui --target-dir $TargetDirectory
    if ($LASTEXITCODE -ne 0) { throw 'Release build failed.' }
} finally {
    $env:CARGO_ENCODED_RUSTFLAGS = $savedFlags
}

& (Join-Path $PSScriptRoot 'check-build-paths.ps1') -Path (Join-Path $TargetDirectory 'release\ovf-ui.exe') -BuildDirectory $TargetDirectory
