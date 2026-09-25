# agente.ps1 — laço 24/7 do PC servidor (motor principal).
# Copie para C:\millennium-bridge\ (fora do OneDrive) e rode via NSSM.
# Jobs UM DE CADA VEZ (Millennium = 1 sessão). Heartbeat no fim de cada volta.
$ErrorActionPreference = 'Continue'
$base = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $base) { $base = 'C:\millennium-bridge' }

function Get-AgenteCfg {
    $cfgPath = Join-Path $base 'config.json'
    if (-not (Test-Path $cfgPath)) {
        Write-Host "ERRO: falta $cfgPath"
        return $null
    }
    try {
        return Get-Content $cfgPath -Raw -Encoding UTF8 | ConvertFrom-Json
    } catch {
        Write-Host "ERRO ao ler config.json: $_"
        return $null
    }
}

function Get-PythonExe {
    foreach ($n in @('python', 'py', 'python3')) {
        $c = Get-Command $n -ErrorAction SilentlyContinue
        if ($c) { return $c.Source }
    }
    return $null
}

function Send-Heartbeat([hashtable]$extra) {
    $cfg = Get-AgenteCfg
    if (-not $cfg) { return }
    $urlBase = [string]($cfg.canalml_base)
    if (-not $urlBase) { $urlBase = [string]($cfg.canal_base) }
    $token = [string]($cfg.canalml_token)
    if (-not $token) { $token = [string]($cfg.erp_token) }
    if (-not $urlBase -or -not $token) {
        Write-Host 'heartbeat: canalml_base/canalml_token (ou canal_base/erp_token) ausente'
        return
    }
    $payload = @{ agente = 'pc-millennium'; ok = $true }
    if ($extra) {
        foreach ($k in $extra.Keys) { $payload[$k] = $extra[$k] }
    }
    try {
        $uri = ($urlBase.TrimEnd('/')) + '/api/agente-heartbeat.php'
        $body = $payload | ConvertTo-Json -Compress
        Invoke-RestMethod -Method Post -TimeoutSec 20 -Uri $uri `
            -Headers @{ 'X-Agente-Token' = $token } `
            -ContentType 'application/json; charset=utf-8' `
            -Body $body | Out-Null
    } catch {
        Write-Host "heartbeat falhou: $_"
    }
}

function Invoke-JobFile([string]$ps1, [string]$py) {
    $ps1Path = Join-Path $base $ps1
    if ($ps1 -and (Test-Path $ps1Path)) {
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $ps1Path
        return $LASTEXITCODE
    }
    $pyPath = Join-Path $base $py
    if ($py -and (Test-Path $pyPath)) {
        $exe = Get-PythonExe
        if (-not $exe) {
            Write-Host "python ausente para $py"
            return 1
        }
        & $exe $pyPath
        return $LASTEXITCODE
    }
    return $null
}

function Test-CatalogoVenceu {
    $stamp = Join-Path $base 'catalogo.stamp'
    if (-not (Test-Path $stamp)) { return $true }
    $idade = (Get-Date) - (Get-Item $stamp).LastWriteTime
    return $idade.TotalHours -ge 20
}

while ($true) {
    $resumo = @{ ciclo_ok = $true }
    try {
        $cfg = Get-AgenteCfg
        if (-not $cfg) {
            $resumo.ciclo_ok = $false
            $resumo.erro = 'sem_config'
        } else {
            if (Test-CatalogoVenceu) {
                $c = Invoke-JobFile 'millennium-catalogo.ps1' ''
                if ($null -ne $c) {
                    $resumo.catalogo = $c
                    if ($c -eq 0) { Set-Content -Path (Join-Path $base 'catalogo.stamp') -Value (Get-Date).ToString('o') }
                }
            }
            $e = Invoke-JobFile 'millennium-puller.ps1' 'puller.py'
            if ($null -ne $e) { $resumo.estoque = $e }
            $p = Invoke-JobFile 'millennium-preco.ps1' ''
            if ($null -ne $p) { $resumo.preco = $p }
            $w = Invoke-JobFile 'pedido_worker.ps1' 'pedido_worker.py'
            if ($null -ne $w) { $resumo.pedido = $w }
        }
    } catch {
        Write-Host "ciclo com erro: $_"
        $resumo.ciclo_ok = $false
        $resumo.erro = [string]$_
    }
    Send-Heartbeat $resumo
    Start-Sleep -Seconds 150
}
