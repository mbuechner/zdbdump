<# : batch portion
@REM ----------------------------------------------------------------------------
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM    http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM ----------------------------------------------------------------------------

@REM ----------------------------------------------------------------------------
@REM Apache Maven Wrapper startup batch script, version 3.3.4
@REM
@REM Optional ENV vars
@REM   MVNW_REPOURL - repo url base for downloading maven distribution
@REM   MVNW_USERNAME/MVNW_PASSWORD - user and password for downloading maven
@REM   MVNW_VERBOSE - true: enable verbose log; others: silence the output
@REM ----------------------------------------------------------------------------

@SETLOCAL DisableDelayedExpansion
@IF NOT DEFINED __MVNW_ARG0_NAME__ SET "__MVNW_ARG0_NAME__=%~nx0"
@SET "__MVNW_SCRIPT_PATH__=%~f0"
@SET __MVNW_CMD__=
@SET "__MVNW_PSMODULEP_SAVE=%PSModulePath%"
@SET PSModulePath=
@FOR /F "usebackq tokens=1* delims==" %%A IN (`powershell -noprofile "& {$scriptDir=[IO.Path]::GetDirectoryName($env:__MVNW_SCRIPT_PATH__); $script=$env:__MVNW_ARG0_NAME__; icm -ScriptBlock ([Scriptblock]::Create((Get-Content -Raw -LiteralPath $env:__MVNW_SCRIPT_PATH__))) -NoNewScope}"`) DO @(
  IF "%%A"=="MVN_CMD" (set "__MVNW_CMD__=%%B") ELSE IF "%%B"=="" (echo %%A) ELSE (echo %%A=%%B)
)
@SET "PSModulePath=%__MVNW_PSMODULEP_SAVE%"
@SET __MVNW_PSMODULEP_SAVE=
@SET __MVNW_ARG0_NAME__=
@SET __MVNW_SCRIPT_PATH__=
@SET MVNW_USERNAME=
@SET MVNW_PASSWORD=
@IF NOT "%__MVNW_CMD__%"=="" ("%__MVNW_CMD__%" %*)
@echo Cannot start maven from wrapper >&2 && exit /b 1
@GOTO :EOF
: end batch / begin powershell #>

$ErrorActionPreference = "Stop"
if ($env:MVNW_VERBOSE -eq "true") {
  $VerbosePreference = "Continue"
}

# calculate distributionUrl, requires .mvn/wrapper/maven-wrapper.properties
$wrapperProperties = Get-Content -Raw -LiteralPath "$scriptDir/.mvn/wrapper/maven-wrapper.properties" | ConvertFrom-StringData
$distributionUrl = $wrapperProperties.distributionUrl
if (!$distributionUrl) {
  Write-Error "cannot read distributionUrl property in $scriptDir/.mvn/wrapper/maven-wrapper.properties"
}

switch -wildcard -casesensitive ( $($distributionUrl -replace '^.*/','') ) {
  "maven-mvnd-*" {
    $USE_MVND = $true
    $distributionUrl = $distributionUrl -replace '-bin\.[^.]*$',"-windows-amd64.zip"
    $MVN_CMD = "mvnd.cmd"
    break
  }
  default {
    $USE_MVND = $false
    $MVN_CMD = $script -replace '^mvnw','mvn'
    break
  }
}

# apply MVNW_REPOURL and calculate MAVEN_HOME
# maven home pattern: ~/.m2/wrapper/dists/{apache-maven-<version>,maven-mvnd-<version>-<platform>}/<hash>
if ($env:MVNW_REPOURL) {
  $MVNW_REPO_PATTERN = if ($USE_MVND -eq $False) { "/org/apache/maven/" } else { "/maven/mvnd/" }
  $distributionUrl = "$env:MVNW_REPOURL$MVNW_REPO_PATTERN$($distributionUrl -replace "^.*$MVNW_REPO_PATTERN",'')"
}
$distributionUrlName = $distributionUrl -replace '^.*/',''
$distributionUrlNameMain = $distributionUrlName -replace '\.[^.]*$','' -replace '-bin$',''
if (!$distributionUrlNameMain -or $distributionUrlName -notmatch '\A[^\\/]+\.zip\z') {
  Write-Error "distributionUrl is not valid, must end with *-bin.zip, but found $distributionUrl"
}

$MAVEN_M2_PATH = "$HOME/.m2"
if ($env:MAVEN_USER_HOME) {
  $MAVEN_M2_PATH = "$env:MAVEN_USER_HOME"
}

$MAVEN_WRAPPER_DISTS = $null
$MAVEN_M2_PATH = [IO.Directory]::CreateDirectory($MAVEN_M2_PATH).FullName
$mavenHomeTarget = (Get-Item -LiteralPath $MAVEN_M2_PATH).Target
if ($mavenHomeTarget) {
  $MAVEN_WRAPPER_DISTS = @($mavenHomeTarget)[0] + "/wrapper/dists"
} else {
  $MAVEN_WRAPPER_DISTS = "$MAVEN_M2_PATH/wrapper/dists"
}

$MAVEN_HOME_PARENT = [IO.Path]::GetFullPath("$MAVEN_WRAPPER_DISTS/$distributionUrlNameMain")
$distributionSha256Sum = $wrapperProperties.distributionSha256Sum
$cacheIdentity = $distributionUrl
if ($distributionSha256Sum) {
  if ($distributionSha256Sum -cnotmatch '\A[0-9A-Fa-f]{64}\z') {
    Write-Error "distributionSha256Sum must contain exactly 64 hexadecimal characters."
  }
  if ($USE_MVND) {
    Write-Error "Checksum validation is not supported for maven-mvnd."
  }
  $distributionSha256Sum = $distributionSha256Sum.ToLowerInvariant()
  # Include the checksum so only a cache entry installed with the current pin can be reused.
  $cacheIdentity = "$distributionUrl`n$distributionSha256Sum"
}
$MAVEN_HOME_NAME = ([System.Security.Cryptography.SHA256]::Create().ComputeHash([byte[]][char[]]$cacheIdentity) | ForEach-Object {$_.ToString("x2")}) -join ''
$MAVEN_HOME = Join-Path $MAVEN_HOME_PARENT $MAVEN_HOME_NAME
$mavenCommand = Join-Path $MAVEN_HOME "bin/$MVN_CMD"

if (Test-Path -LiteralPath $mavenCommand -PathType Leaf) {
  Write-Verbose "found existing MAVEN_HOME at $MAVEN_HOME"
  Write-Output "MVN_CMD=$mavenCommand"
  exit 0
}

# Stage the download on the same filesystem as the cache destination.
[IO.Directory]::CreateDirectory($MAVEN_HOME_PARENT) | Out-Null
$TMP_DOWNLOAD_DIR = Join-Path $MAVEN_HOME_PARENT ".mvnw-$([Guid]::NewGuid())"
[IO.Directory]::CreateDirectory($TMP_DOWNLOAD_DIR) | Out-Null
$distributionArchive = Join-Path $TMP_DOWNLOAD_DIR $distributionUrlName

try {
  Write-Verbose "Couldn't find MAVEN_HOME, downloading and installing it ..."
  Write-Verbose "Downloading from: $distributionUrl"
  Write-Verbose "Downloading to: $distributionArchive"

  $webclient = New-Object System.Net.WebClient
  if ($env:MVNW_USERNAME -and $env:MVNW_PASSWORD) {
    $webclient.Credentials = New-Object System.Net.NetworkCredential($env:MVNW_USERNAME, $env:MVNW_PASSWORD)
  }
  [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
  $webclient.DownloadFile($distributionUrl, $distributionArchive)

  # If specified, validate the SHA-256 sum of the Maven distribution zip file
  if ($distributionSha256Sum) {
    Import-Module $PSHOME\Modules\Microsoft.PowerShell.Utility -Function Get-FileHash
    if ((Get-FileHash -LiteralPath $distributionArchive -Algorithm SHA256).Hash.ToLowerInvariant() -ne $distributionSha256Sum) {
      Write-Error "Error: Failed to validate Maven distribution SHA-256, your Maven distribution might be compromised. If you updated your Maven version, you need to update the specified distributionSha256Sum property."
    }
  }

  Add-Type -AssemblyName System.IO.Compression.FileSystem
  [IO.Compression.ZipFile]::ExtractToDirectory($distributionArchive, $TMP_DOWNLOAD_DIR)

  # Snapshot archives may use a different top-level directory name.
  $actualDistributionPath = Join-Path $TMP_DOWNLOAD_DIR $distributionUrlNameMain
  if (!(Test-Path -LiteralPath (Join-Path $actualDistributionPath "bin/$MVN_CMD") -PathType Leaf)) {
    $distributionDirectory = Get-ChildItem -LiteralPath $TMP_DOWNLOAD_DIR -Directory | Where-Object {
      Test-Path -LiteralPath (Join-Path $_.FullName "bin/$MVN_CMD") -PathType Leaf
    } | Select-Object -First 1
    if (!$distributionDirectory) {
      Write-Error "Could not find Maven distribution directory in extracted archive"
    }
    $actualDistributionPath = $distributionDirectory.FullName
  }

  Write-Verbose "Found extracted Maven distribution directory: $actualDistributionPath"
  # Keep a stable lock file so parallel wrapper processes synchronize on the same cache entry.
  $cacheLock = $null
  for ($attempt = 1; !$cacheLock; $attempt++) {
    try {
      $cacheLock = [IO.File]::Open("$MAVEN_HOME.lock", [IO.FileMode]::OpenOrCreate,
        [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    } catch [IO.IOException] {
      if ($attempt -eq 100) {
        throw
      }
      Start-Sleep -Milliseconds 200
    }
  }
  try {
    if (!(Test-Path -LiteralPath $mavenCommand -PathType Leaf)) {
      $existingDistribution = Get-Item -LiteralPath $MAVEN_HOME -Force -ErrorAction SilentlyContinue
      if ($existingDistribution) {
        if ($existingDistribution.FullName -ne $MAVEN_HOME -or
            ($existingDistribution.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
          throw "Cannot repair an unexpected Maven cache entry at $MAVEN_HOME"
        }
        # Move the incomplete entry into this invocation's staging area for cleanup.
        $incompleteDistribution = Join-Path $TMP_DOWNLOAD_DIR (".incomplete-" + [Guid]::NewGuid())
        if ($existingDistribution.PSIsContainer) {
          [IO.Directory]::Move($MAVEN_HOME, $incompleteDistribution)
        } else {
          [IO.File]::Move($MAVEN_HOME, $incompleteDistribution)
        }
      }
      [IO.Directory]::Move($actualDistributionPath, $MAVEN_HOME)
    }
  } finally {
    $cacheLock.Dispose()
  }
} finally {
  if ([IO.Path]::GetDirectoryName($TMP_DOWNLOAD_DIR) -eq $MAVEN_HOME_PARENT) {
    try { Remove-Item -LiteralPath $TMP_DOWNLOAD_DIR -Recurse -Force }
    catch { Write-Warning "Cannot remove $TMP_DOWNLOAD_DIR" }
  }
}

Write-Output "MVN_CMD=$mavenCommand"
