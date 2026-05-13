param(
  [int]$Port = 8080,
  [string]$Root = "..\dist"
)

$ErrorActionPreference = "Stop"

function Get-MimeType {
  param([string]$FilePath)
  switch ([System.IO.Path]::GetExtension($FilePath).ToLowerInvariant()) {
    ".html" { "text/html; charset=utf-8" }
    ".js" { "text/javascript; charset=utf-8" }
    ".mjs" { "text/javascript; charset=utf-8" }
    ".css" { "text/css; charset=utf-8" }
    ".json" { "application/json; charset=utf-8" }
    ".svg" { "image/svg+xml" }
    ".png" { "image/png" }
    ".jpg" { "image/jpeg" }
    ".jpeg" { "image/jpeg" }
    ".ico" { "image/x-icon" }
    ".woff" { "font/woff" }
    ".woff2" { "font/woff2" }
    ".txt" { "text/plain; charset=utf-8" }
    default { "application/octet-stream" }
  }
}

$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$resolvedRoot = [System.IO.Path]::GetFullPath((Join-Path $scriptDirectory $Root))

if (-not (Test-Path $resolvedRoot -PathType Container)) {
  throw "No existe la carpeta de build: $resolvedRoot"
}

$listener = [System.Net.HttpListener]::new()
$prefix = "http://localhost:$Port/"
$listener.Prefixes.Add($prefix)
$listener.Start()

Write-Host "Dashboard listo en $prefix"
Write-Host "Sirviendo archivos desde: $resolvedRoot"
Write-Host "Pulsa Ctrl + C para detener."

try {
  while ($listener.IsListening) {
    $context = $listener.GetContext()
    $request = $context.Request
    $response = $context.Response

    $relativePath = [System.Uri]::UnescapeDataString($request.Url.AbsolutePath.TrimStart('/'))
    if ([string]::IsNullOrWhiteSpace($relativePath)) {
      $relativePath = "index.html"
    }

    $candidatePath = [System.IO.Path]::GetFullPath((Join-Path $resolvedRoot $relativePath))

    if (-not $candidatePath.StartsWith($resolvedRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
      $response.StatusCode = 403
      $response.OutputStream.Close()
      continue
    }

    if (-not (Test-Path $candidatePath -PathType Leaf)) {
      $candidatePath = Join-Path $resolvedRoot "index.html"
    }

    $bytes = [System.IO.File]::ReadAllBytes($candidatePath)
    $response.StatusCode = 200
    $response.ContentType = Get-MimeType -FilePath $candidatePath
    $response.ContentLength64 = $bytes.Length
    $response.OutputStream.Write($bytes, 0, $bytes.Length)
    $response.OutputStream.Close()
  }
}
finally {
  $listener.Stop()
  $listener.Close()
}
