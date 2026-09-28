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
    # Como SERVICO (NSSM/LocalSystem) o PATH nao tem o Python instalado por usuario.
    # Procura: cache -> config.json (python_exe) -> PATH -> locais comuns/perfis.
    if ($script:PyExe -and (Test-Path $script:PyExe)) { return $script:PyExe }
    $cfg = Get-AgenteCfg
    if ($cfg -and $cfg.python_exe -and (Test-Path ([string]$cfg.python_exe))) {
        $script:PyExe = [string]$cfg.python_exe; return $script:PyExe
    }
    foreach ($n in @('python', 'py', 'python3')) {
        $c = Get-Command $n -ErrorAction SilentlyContinue
        if ($c) { $script:PyExe = $c.Source; return $script:PyExe }
    }
    $globs = @(
        'C:\Windows\py.exe',
        'C:\Program Files\Python*\python.exe',
        'C:\Program Files (x86)\Python*\python.exe',
        'C:\Python*\python.exe',
        "$env:LOCALAPPDATA\Programs\Python\Python*\python.exe",
        'C:\Users\*\AppData\Local\Programs\Python\Python*\python.exe',
        'C:\Users\*\AppData\Local\Python\pythoncore-*\python.exe',
        "$env:LOCALAPPDATA\Python\pythoncore-*\python.exe"
    )
    foreach ($g in $globs) {
        $hit = Get-ChildItem -Path $g -ErrorAction SilentlyContinue | Sort-Object FullName -Descending | Select-Object -First 1
        if ($hit) { $script:PyExe = $hit.FullName; return $script:PyExe }
    }
    return $null
}

function Set-JobEnv($cfg) {
    # Injeta a config.json como variaveis de ambiente pros workers Python.
    # pedido_worker.py e puller.py leem TUDO de env; sem isto o worker roda cego.
    # Assim o robo depende so de agente.ps1 + config.json (sem wrappers .ps1 soltos).
    if (-not $cfg) { return }
    $env:MILLENNIUM_URL     = [string]$cfg.millennium_url
    $env:MILLENNIUM_USER    = [string]$cfg.millennium_user
    $env:MILLENNIUM_PASS    = [string]$cfg.millennium_pass
    $env:VITRINE            = [string]$cfg.vitrine
    $env:CANAL_BASE         = [string]$cfg.canal_base
    $env:ERP_PUSH_TOKEN     = [string]$cfg.erp_token
    $env:GIST_PEDIDOS_ID    = [string]$cfg.gist_pedidos_id
    $env:GIST_PEDIDOS_TOKEN = [string]$cfg.gist_pedidos_token
    # estoque (puller.py) usa o mesmo gist, arquivo estoque.json (GIST_FILE padrao)
    $env:GIST_ID            = [string]$cfg.gist_pedidos_id
    $env:GIST_TOKEN         = [string]$cfg.gist_pedidos_token
    $env:DRY_RUN            = [string]$cfg.dry_run
    $we = [string]$cfg.worker_enabled
    if ($we -eq '') { $we = '1' }   # vazio = ligado (producao)
    $env:WORKER_ENABLED     = $we
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

function Update-Repo {
    # Auto-atualiza o codigo do repositorio a cada volta (deploy sem ir no servidor).
    # Best-effort: se nao for repo git ou faltar rede/credencial, apenas loga e segue.
    # OBS: mudancas em .py valem no proximo ciclo (sao chamadas na hora);
    #      mudancas neste agente.ps1 so valem apos reiniciar o servico.
    try {
        Push-Location $base
        & git rev-parse --is-inside-work-tree 2>$null | Out-Null
        if ($LASTEXITCODE -ne 0) { Pop-Location; return }
        & git fetch --quiet origin main 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) {
            $localRev  = (& git rev-parse HEAD 2>$null)
            $remoteRev = (& git rev-parse origin/main 2>$null)
            if ($localRev -ne $remoteRev) {
                & git reset --hard origin/main 2>&1 | Out-Null
                Write-Host "auto-update: atualizado para $remoteRev"
            }
        } else {
            Write-Host "auto-update: git fetch falhou (rede/credencial?) - seguindo com o codigo atual."
        }
    } catch {
        Write-Host "auto-update: erro $_"
    } finally {
        Pop-Location
    }
}

while ($true) {
    $resumo = @{ ciclo_ok = $true }
    Update-Repo
    try {
        $cfg = Get-AgenteCfg
        if (-not $cfg) {
            $resumo.ciclo_ok = $false
            $resumo.erro = 'sem_config'
        } else {
            Set-JobEnv $cfg
            if (Test-CatalogoVenceu) {
                $c = Invoke-JobFile 'millennium-catalogo.ps1' ''
                if ($null -ne $c) {
                    $resumo.catalogo = $c
                    if ($c -eq 0) { Set-Content -Path (Join-Path $base 'catalogo.stamp') -Value (Get-Date).ToString('o') }
                }
            }
            # Millennium = 1 licenca nossa. Rodamos 1 job por vez COM intervalo,
            # pra a sessao anterior liberar antes do proximo login (evita o "retag"/bloqueio).
            $e = Invoke-JobFile 'millennium-puller.ps1' 'puller.py'
            if ($null -ne $e) { $resumo.estoque = $e }
            Start-Sleep -Seconds 60
            $p = Invoke-JobFile 'millennium-preco.ps1' ''
            if ($null -ne $p) { $resumo.preco = $p }
            Start-Sleep -Seconds 60
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
