# Installs the iroh-gateway binary on Windows from the GitHub releases.
#
#   irm https://raw.githubusercontent.com/irohkit/iroh-bridge/main/scripts/install-gateway.ps1 | iex
#
# Installs to %LOCALAPPDATA%\iroh-gateway and adds that directory to the user PATH. No admin
# rights needed. Settings, as parameters when run as a file or as environment variables:
#   -Version / IROH_GATEWAY_VERSION          release tag, default: the latest release
#   -InstallDir / IROH_GATEWAY_INSTALL_DIR   default: %LOCALAPPDATA%\iroh-gateway
#   -BaseUrl / IROH_GATEWAY_BASE_URL         where release assets live, as BASE/<tag>/<asset>; an
#                                            http(s) URL or a local directory
#   -NoPathUpdate / IROH_GATEWAY_NO_MODIFY_PATH=1   leave the user PATH alone
param(
    [string]$Version = $env:IROH_GATEWAY_VERSION,
    [string]$InstallDir = $env:IROH_GATEWAY_INSTALL_DIR,
    [string]$BaseUrl = $env:IROH_GATEWAY_BASE_URL,
    [switch]$NoPathUpdate = ($env:IROH_GATEWAY_NO_MODIFY_PATH -eq '1')
)

# Everything runs inside a function: under `irm | iex` the script shares the caller's session,
# so it must not change their preferences or `exit` their shell.
function Install-IrohGateway {
    param([string]$Version, [string]$InstallDir, [string]$BaseUrl, [bool]$NoPathUpdate)
    $ErrorActionPreference = 'Stop'
    $ProgressPreference = 'SilentlyContinue'  # the progress bar makes Invoke-WebRequest very slow on 5.1
    $repo = 'irohkit/iroh-bridge'

    if (-not $BaseUrl) { $BaseUrl = "https://github.com/$repo/releases/download" }
    if (-not $InstallDir) { $InstallDir = Join-Path $env:LOCALAPPDATA 'iroh-gateway' }

    # Windows PowerShell 5.1 may default to TLS 1.0, which GitHub refuses.
    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
    } catch {}

    $arch = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
    # Windows on ARM runs x64 binaries through emulation.
    if ($arch -notin @('AMD64', 'ARM64')) {
        throw "no prebuilt iroh-gateway for $arch; build it with: cargo build --release -p iroh-gateway"
    }
    $target = 'x86_64-pc-windows-msvc'

    if (-not $Version) {
        try {
            $Version = (Invoke-RestMethod -UseBasicParsing "https://api.github.com/repos/$repo/releases/latest").tag_name
        } catch {
            throw "could not look up the latest release ($($_.Exception.Message)); set `$env:IROH_GATEWAY_VERSION = 'vX.Y.Z'"
        }
    }
    if (-not $Version.StartsWith('v')) { $Version = "v$Version" }

    $name = "iroh-gateway-$Version-$target"
    $tmp = Join-Path ([IO.Path]::GetTempPath()) ("iroh-gateway-" + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $tmp | Out-Null
    try {
        foreach ($file in @("$name.zip", "$name.zip.sha256")) {
            $src = "$BaseUrl/$Version/$file"
            $dst = Join-Path $tmp $file
            try {
                if ($src -match '^https?://') {
                    Invoke-WebRequest -UseBasicParsing -Uri $src -OutFile $dst
                } else {
                    Copy-Item -LiteralPath ($src -replace '^file:///?', '') -Destination $dst
                }
            } catch {
                throw "could not download $src ($($_.Exception.Message)). Release $Version may have no Windows build; see https://github.com/$repo/releases"
            }
        }
        Write-Host "downloaded $name.zip"

        $expected = ((Get-Content -Raw (Join-Path $tmp "$name.zip.sha256")).Trim() -split '\s+')[0].ToLowerInvariant()
        $actual = (Get-FileHash -Algorithm SHA256 (Join-Path $tmp "$name.zip")).Hash.ToLowerInvariant()
        if (-not $expected -or $expected -ne $actual) {
            throw "checksum mismatch for $name.zip: expected $expected, got $actual"
        }
        Write-Host 'sha256 verified'

        $extract = Join-Path $tmp 'x'
        Expand-Archive -LiteralPath (Join-Path $tmp "$name.zip") -DestinationPath $extract
        $exe = Get-ChildItem -LiteralPath $extract -Recurse -Filter 'iroh-gateway.exe' | Select-Object -First 1
        if (-not $exe) { throw "$name.zip does not contain iroh-gateway.exe" }

        New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
        $dest = Join-Path $InstallDir 'iroh-gateway.exe'
        try {
            Copy-Item -LiteralPath $exe.FullName -Destination $dest -Force
        } catch {
            throw "could not write $dest; is iroh-gateway still running? Stop it and try again. ($($_.Exception.Message))"
        }
    } finally {
        Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue
    }

    Write-Host ''
    Write-Host "iroh-gateway $Version installed to $dest"
    $cmd = $dest
    if (-not $NoPathUpdate) {
        $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
        $parts = @($userPath -split ';' | Where-Object { $_ })
        if ($parts -notcontains $InstallDir) {
            [Environment]::SetEnvironmentVariable('Path', (($parts + $InstallDir) -join ';'), 'User')
            Write-Host "added $InstallDir to your user PATH (new terminals pick it up)"
        }
        if (($env:Path -split ';') -notcontains $InstallDir) { $env:Path = "$env:Path;$InstallDir" }
        $cmd = 'iroh-gateway'
    }
    Write-Host ''
    Write-Host 'Next steps:'
    Write-Host "  run it next to your backend:  `$env:BACKEND_URL = 'http://127.0.0.1:8080'; $cmd"
    Write-Host "  all options:                  $cmd --help"
    Write-Host "It prints IROH_ENDPOINT_ID=... once online; clients connect with that id. Ctrl-C stops it."
    Write-Host "Docs: https://github.com/$repo#backend-iroh-gateway"
}

Install-IrohGateway -Version $Version -InstallDir $InstallDir -BaseUrl $BaseUrl -NoPathUpdate ([bool]$NoPathUpdate)
