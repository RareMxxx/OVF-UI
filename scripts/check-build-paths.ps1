param(
    [Parameter(Mandatory = $true)][string]$Path,
    [string]$BuildDirectory
)

$ErrorActionPreference = 'Stop'
$sourceRoot = Split-Path -Parent $PSScriptRoot
$roots = @($env:USERPROFILE, $sourceRoot, $env:CARGO_HOME, $env:RUSTUP_HOME, $BuildDirectory) |
    Where-Object { ![string]::IsNullOrWhiteSpace($_) } |
    ForEach-Object { [IO.Path]::GetFullPath($_, $sourceRoot).TrimEnd('\', '/') } |
    Select-Object -Unique
$patterns = foreach ($root in $roots) { $root; $root.Replace('\', '/'); $root.Replace('\', '\\') }
$item = Get-Item -LiteralPath $Path
$files = if ($item.PSIsContainer) { @(Get-ChildItem -LiteralPath $item.FullName -File -Recurse) } else { @($item) }
if ($files.Count -eq 0) { throw 'Build privacy check received no files.' }
foreach ($file in $files) {
    $bytes = [IO.File]::ReadAllBytes($file.FullName)
    # Latin1 is a lossless byte-to-character mapping. Search encoded byte
    # sequences so UTF-16 strings are detected even at an odd binary offset.
    $binary = [Text.Encoding]::Latin1.GetString($bytes)
    foreach ($encoding in @([Text.Encoding]::UTF8, [Text.Encoding]::Unicode)) {
        foreach ($pattern in $patterns) {
            $needle = [Text.Encoding]::Latin1.GetString($encoding.GetBytes($pattern))
            if ($binary.IndexOf($needle, [StringComparison]::OrdinalIgnoreCase) -ge 0) {
                # Do not echo the matched personal path into CI or release logs.
                throw ('Local build path found in release file: ' + $file.Name)
            }
        }
    }
}
Write-Output ('Build privacy check passed: ' + $files.Count + ' files; no local profile/build paths.')
