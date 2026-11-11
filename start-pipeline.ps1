# Observability Resilient Pipeline - inicializacao para demonstracao (PowerShell 5.1+)
# Execute na raiz do repositorio: .\start-pipeline.ps1 -OpenBrowser
param(
    [switch]$OpenBrowser,
    [switch]$OpenAllPages,
    [switch]$Build,
    [switch]$ShowDetails,
    [ValidateRange(15,600)][int]$TimeoutSeconds = 180
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($root)) { $root = (Get-Location).Path }
$composePath = Join-Path $root 'compose.yaml'
$envPath = Join-Path $root '.env'
$envExamplePath = Join-Path $root '.env.example'
$placeholderValues = @('seu_usuario', 'troque_esta_senha')
$serviceUrls = [ordered]@{
    'Grafana - Dashboard'     = 'http://localhost:3000/d/resilient-pipeline'
    'Prometheus - Targets'   = 'http://localhost:9090/targets'
    'Prometheus - Alertas'   = 'http://localhost:9090/alerts'
    'Alertmanager'           = 'http://localhost:9093/#/alerts'
    'Producer - Ready'       = 'http://localhost:8000/ready'
    'Producer - Metricas'    = 'http://localhost:8000/metrics'
    'Producer - API Docs'    = 'http://localhost:8000/docs'
}
$requiredJobs = @('producer','consumer','kafka-exporter','kafka-jmx','prometheus','alertmanager')
$provisionerService = 'grafana-viewer-provisioner'

function Invoke-Compose {
    param([string[]]$ArgsList)
    & docker compose --project-directory $root -f $composePath @ArgsList
    if ($LASTEXITCODE -ne 0) { throw "docker compose $($ArgsList -join ' ') falhou (codigo $LASTEXITCODE)." }
}

function Get-EnvValue {
    param([string[]]$Lines, [string]$Key)
    $pattern = '^\s*' + [regex]::Escape($Key) + '\s*=\s*(.*)$'
    foreach ($line in $Lines) {
        if ($line -match $pattern) {
            $raw = $Matches[1].Trim()
            if ($raw.Length -ge 2 -and (($raw.StartsWith("'") -and $raw.EndsWith("'")) -or ($raw.StartsWith('"') -and $raw.EndsWith('"')))) {
                return $raw.Substring(1, $raw.Length - 2)
            }
            return (($raw -split '\s+#', 2)[0]).Trim()
        }
    }
    return ''
}

function Set-EnvValue {
    param([string[]]$Lines, [string]$Key, [string]$Value)
    $pattern = '^\s*' + [regex]::Escape($Key) + '\s*='
    $result = @($Lines | Where-Object { $_ -notmatch $pattern })
    $result += "$Key=$Value"
    return $result
}

function ConvertFrom-SecureStringPlain {
    param([Security.SecureString]$Secure)
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Secure)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
        $Secure.Dispose()
    }
}

function Read-SecretValue {
    param([string]$Prompt)
    while ($true) {
        $first = Read-Host -Prompt $Prompt -AsSecureString
        $plainFirst = ConvertFrom-SecureStringPlain -Secure $first
        if ([string]::IsNullOrWhiteSpace($plainFirst)) {
            Write-Host '  A senha nao pode ser vazia.' -ForegroundColor Yellow
            $plainFirst = $null
            continue
        }
        if ($plainFirst -match "[`r`n']") {
            Write-Host '  A senha nao pode conter quebra de linha ou aspas simples.' -ForegroundColor Yellow
            $plainFirst = $null
            continue
        }
        $second = Read-Host -Prompt '  Confirme a senha' -AsSecureString
        $plainSecond = ConvertFrom-SecureStringPlain -Secure $second
        if ($plainFirst -cne $plainSecond) {
            Write-Host '  As senhas nao coincidem. Tente novamente.' -ForegroundColor Yellow
            $plainFirst = $null
            $plainSecond = $null
            continue
        }
        $plainSecond = $null
        if ($plainFirst.Length -lt 12) {
            Write-Host '  AVISO: senha com menos de 12 caracteres.' -ForegroundColor Yellow
        }
        return $plainFirst
    }
}

function ConvertTo-EnvQuotedValue {
    param([string]$Value)
    return "'$Value'"
}

function Ensure-EnvConfigured {
    param([string]$Path, [string]$ExamplePath)

    if (-not (Test-Path -LiteralPath $Path)) {
        if (-not (Test-Path -LiteralPath $ExamplePath)) {
            throw 'Arquivo .env ausente e .env.example nao encontrado. Crie o .env manualmente com GF_ADMIN_USER, GF_ADMIN_PASSWORD e GF_VIEWER_PASSWORD.'
        }
        Write-Host ''
        Write-Host '  Arquivo .env nao encontrado.' -ForegroundColor Yellow
        Write-Host '  Ele sera criado a partir de .env.example e as credenciais serao solicitadas.' -ForegroundColor Yellow
        Write-Host '  As senhas sao digitadas com entrada oculta e nao vao para o historico de comandos.' -ForegroundColor DarkGray
        Copy-Item -LiteralPath $ExamplePath -Destination $Path
        Write-Host '  .env criado a partir de .env.example.' -ForegroundColor Green
    }

    $lines = @(Get-Content -LiteralPath $Path -Encoding UTF8)
    $changed = $false

    $adminUser = Get-EnvValue -Lines $lines -Key 'GF_ADMIN_USER'
    if ([string]::IsNullOrWhiteSpace($adminUser) -or $placeholderValues -contains $adminUser) {
        Write-Host ''
        Write-Host '  GF_ADMIN_USER nao configurado.' -ForegroundColor Yellow
        $adminUser = Read-Host -Prompt '  Usuario administrador do Grafana [admin]'
        if ([string]::IsNullOrWhiteSpace($adminUser)) { $adminUser = 'admin' }
        if ($adminUser -notmatch '^[A-Za-z0-9._-]+$') {
            throw 'GF_ADMIN_USER invalido. Use letras, numeros, ponto, hifen ou sublinhado.'
        }
        $lines = @(Set-EnvValue -Lines $lines -Key 'GF_ADMIN_USER' -Value $adminUser)
        $changed = $true
    }

    foreach ($secretKey in @('GF_ADMIN_PASSWORD', 'GF_VIEWER_PASSWORD')) {
        $secretValue = Get-EnvValue -Lines $lines -Key $secretKey
        if ([string]::IsNullOrWhiteSpace($secretValue) -or $placeholderValues -contains $secretValue) {
            Write-Host ''
            Write-Host "  $secretKey nao configurado." -ForegroundColor Yellow
            $secretValue = Read-SecretValue -Prompt "  $secretKey (entrada oculta)"
            $lines = @(Set-EnvValue -Lines $lines -Key $secretKey -Value (ConvertTo-EnvQuotedValue -Value $secretValue))
            $secretValue = $null
            $changed = $true
        }
    }

    if ($changed) {
        $utf8 = New-Object System.Text.UTF8Encoding($false)
        [System.IO.File]::WriteAllLines($Path, [string[]]$lines, $utf8)
        Write-Host '  .env atualizado. As senhas nao foram exibidas.' -ForegroundColor Green
    } else {
        Write-Host '  .env ja contem as variaveis obrigatorias.' -ForegroundColor Green
    }
}

function Test-Http {
    param([string]$Url)
    try {
        $r = Invoke-WebRequest -Uri $Url -TimeoutSec 4 -UseBasicParsing
        return ($r.StatusCode -ge 200 -and $r.StatusCode -lt 400)
    } catch { return $false }
}

function Get-UpJobs {
    $up = @{}
    try {
        $r = Invoke-RestMethod 'http://localhost:9090/api/v1/targets?state=active' -TimeoutSec 5
        foreach ($target in @($r.data.activeTargets)) {
            $job = [string]$target.labels.job
            if ($job -and $target.health -eq 'up') { $up[$job] = $true }
        }
    } catch { }
    return $up
}

function Write-Stage {
    param([string]$Message)
    Write-Host "`n> $Message" -ForegroundColor Cyan
}

if (-not (Test-Path $composePath)) {
    Write-Host "ERRO: compose.yaml nao encontrado em $root" -ForegroundColor Red
    Write-Host 'Coloque este script na raiz do repositorio.'
    exit 1
}

try {
    & docker --version | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Docker nao encontrado no PATH.' }
    & docker compose version | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Docker Compose nao disponivel.' }
    & docker info --format '{{.ServerVersion}}' 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Docker Engine indisponivel. Abra o Docker Desktop.' }
} catch {
    Write-Host "ERRO: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Host "`n========================================================" -ForegroundColor DarkCyan
Write-Host ' OBSERVABILITY RESILIENT PIPELINE | Iniciar ambiente' -ForegroundColor White
Write-Host '========================================================' -ForegroundColor DarkCyan

try {
    Write-Stage 'Verificando configuracao local (.env)...'
    Ensure-EnvConfigured -Path $envPath -ExamplePath $envExamplePath

    Write-Stage 'Validando Docker Compose...'
    Invoke-Compose -ArgsList @('config','--quiet')
    Write-Host '  Configuracao valida' -ForegroundColor Green

    Write-Stage 'Subindo servicos sem apagar volumes...'
    $argsUp = @('up','-d')
    if ($Build) { $argsUp += '--build' }
    Invoke-Compose -ArgsList $argsUp
} catch {
    Write-Host "ERRO: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

Write-Stage 'Verificando disponibilidade e coleta de metricas...'
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$upJobs = @{}
$endpointState = @{}
$provisionerState = 'pendente'
$checks = [ordered]@{
    'API Producer'  = 'http://localhost:8000/ready'
    'Prometheus'    = 'http://localhost:9090/-/ready'
    'Grafana'       = 'http://localhost:3000/api/health'
    'Alertmanager'  = 'http://localhost:9093/-/ready'
}

while ((Get-Date) -lt $deadline) {
    foreach ($name in @($checks.Keys)) {
        if (-not $endpointState.ContainsKey($name) -or -not $endpointState[$name]) {
            $endpointState[$name] = Test-Http $checks[$name]
        }
    }
    $upJobs = Get-UpJobs
    $provisionerState = 'pendente'
    try {
        $provisionerId = @(& docker compose --project-directory $root -f $composePath ps -a -q $provisionerService 2>$null) | Select-Object -First 1
        if (-not [string]::IsNullOrWhiteSpace([string]$provisionerId)) {
            $provisionerRunning = [string](& docker inspect ([string]$provisionerId) --format '{{.State.Running}}' 2>$null)
            $provisionerExit = [string](& docker inspect ([string]$provisionerId) --format '{{.State.ExitCode}}' 2>$null)
            if ($provisionerRunning -eq 'true') { $provisionerState = 'executando' }
            elseif ($provisionerExit -eq '0') { $provisionerState = 'concluido' }
            else { $provisionerState = "falhou (exit $provisionerExit)" }
        }
    } catch { $provisionerState = 'verificar' }
    $missing = @($requiredJobs | Where-Object { -not $upJobs.ContainsKey($_) })
    $unready = @($checks.Keys | Where-Object { -not $endpointState[$_] })
    if ($missing.Count -eq 0 -and $unready.Count -eq 0 -and $provisionerState -eq 'concluido') { break }
    Start-Sleep -Seconds 3
}

foreach ($name in $checks.Keys) {
    if ($endpointState[$name]) { Write-Host "  [OK] $name" -ForegroundColor Green }
    else { Write-Host "  [PENDENTE] $name" -ForegroundColor Yellow }
}
Write-Host "`n  Prometheus - targets:" -ForegroundColor White
foreach ($job in $requiredJobs) {
    if ($upJobs.ContainsKey($job)) { Write-Host "  [UP] $job" -ForegroundColor Green }
    else { Write-Host "  [VERIFICAR] $job" -ForegroundColor Yellow }
}
if ($provisionerState -eq 'concluido') {
    Write-Host "  [OK] Grafana viewer provisioner (Viewer na organizacao 1)" -ForegroundColor Green
} else {
    Write-Host "  [VERIFICAR] Grafana viewer provisioner ($provisionerState)" -ForegroundColor Yellow
}

if ($ShowDetails) {
    Write-Stage 'Detalhes dos containers:'
    try { Invoke-Compose -ArgsList @('ps') } catch { Write-Host $_ -ForegroundColor Yellow }
}

$missing = @($requiredJobs | Where-Object { -not $upJobs.ContainsKey($_) })
$unready = @($checks.Keys | Where-Object { -not $endpointState[$_] })
$allGood = ($missing.Count -eq 0 -and $unready.Count -eq 0 -and $provisionerState -eq 'concluido')
$statusTitle = if ($allGood) { 'Ambiente pronto para demonstracao' } else { 'Ambiente iniciado - verificacoes pendentes' }
$statusColor = if ($allGood) { '#16a34a' } else { '#eab308' }
$statusText = if ($allGood) { 'Todos os endpoints, 6 targets e o provisionamento do Viewer responderam.' } else { 'Consulte o terminal e o Prometheus para identificar os itens pendentes.' }

# Uma unica pagina local serve como ponto de partida; nao altera a aplicacao nem o repositorio.
$cards = @(
    @{ title='Dashboard principal'; tag='GRAFANA'; url=$serviceUrls['Grafana - Dashboard']; descr='11 paineis: taxas, latencias, lag, alertas e bytes do Kafka'; accent='#f59e0b' },
    @{ title='Targets'; tag='PROMETHEUS'; url=$serviceUrls['Prometheus - Targets']; descr='Saude dos endpoints e exporters'; accent='#ef4444' },
    @{ title='Regras e alertas'; tag='PROMETHEUS'; url=$serviceUrls['Prometheus - Alertas']; descr='HighConsumerLag e HighErrorRate'; accent='#ef4444' },
    @{ title='Alertas ativos'; tag='ALERTMANAGER'; url=$serviceUrls['Alertmanager']; descr='Alertas recebidos, agrupados e silenciamentos'; accent='#fb923c' },
    @{ title='Producer - Ready'; tag='FASTAPI'; url=$serviceUrls['Producer - Ready']; descr='Disponibilidade da API e do topico Kafka'; accent='#38bdf8' },
    @{ title='Producer - Metricas'; tag='FASTAPI'; url=$serviceUrls['Producer - Metricas']; descr='Metricas no formato Prometheus'; accent='#38bdf8' },
    @{ title='Documentacao da API'; tag='FASTAPI'; url=$serviceUrls['Producer - API Docs']; descr='Endpoint de ingestao e contrato HTTP'; accent='#38bdf8' }
)

$cardsHtml = foreach ($card in $cards) {
    $safeTitle = [System.Net.WebUtility]::HtmlEncode($card.title)
    $safeDescr = [System.Net.WebUtility]::HtmlEncode($card.descr)
    $safeTag = [System.Net.WebUtility]::HtmlEncode($card.tag)
    $safeUrl = [System.Net.WebUtility]::HtmlEncode($card.url)
    "<a class='card' style='--accent:$($card.accent)' href='$safeUrl' target='_blank' rel='noopener noreferrer'><span class='tag'>$safeTag</span><strong>$safeTitle</strong><span class='description'>$safeDescr</span><span class='link'>Abrir pagina &#8599;</span></a>"
}
$targetHtml = foreach ($job in $requiredJobs) {
    $ok = $upJobs.ContainsKey($job)
    $class = if ($ok) { 'up' } else { 'pending' }
    $state = if ($ok) { 'UP' } else { 'VERIFICAR' }
    "<span class='target $class'><b class='dot'></b> $job <small>$state</small></span>"
}
$checkedAt = (Get-Date).ToString('dd/MM/yyyy HH:mm:ss')
$html = @"
<!doctype html>
<html lang='pt-BR'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Observability Pipeline | Central da demonstracao</title>
<style>
:root { color-scheme:dark; font-family:Segoe UI,Arial,sans-serif; }
* { box-sizing:border-box; }
body { margin:0; background:#0b1220; color:#e5ecf6; }
main { max-width:1120px; margin:auto; padding:48px 24px 64px; }
.eyebrow { color:#7dd3fc; font-size:12px; font-weight:700; letter-spacing:2px; }
h1 { font-size:clamp(28px,4vw,42px); margin:12px 0 10px; letter-spacing:-1px; }
.sub { color:#9caec6; line-height:1.6; margin-bottom:26px; }
.status { border:1px solid #263a52; background:#131f31; border-radius:14px; padding:20px; margin:24px 0; display:flex; align-items:center; gap:16px; }
.pulse { width:13px; height:13px; background:$statusColor; border-radius:50%; box-shadow:0 0 0 6px #ffffff0d; flex-shrink:0; }
.status strong { display:block; margin-bottom:6px; font-size:17px; }
.status span { color:#a8b7ca; font-size:13px; }
h2 { font-size:17px; margin:34px 0 16px; }
.targets { display:flex; flex-wrap:wrap; gap:8px; }
.target { padding:9px 12px; border-radius:10px; background:#152338; border:1px solid #304057; font-size:13px; }
.target small { color:#8ca1bc; margin-left:10px; }
.dot { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:6px; background:#eab308; }
.up .dot { background:#22c55e; }
.grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(230px,1fr)); gap:14px; }
.card { display:flex; flex-direction:column; gap:12px; min-height:176px; padding:20px; color:inherit; text-decoration:none; background:#131f31; border:1px solid #263a52; border-top:3px solid var(--accent); border-radius:12px; transition:transform .15s,border-color .15s; }
.card:hover { transform:translateY(-3px); border-color:var(--accent); }
.tag { color:var(--accent); font-size:10px; font-weight:800; letter-spacing:1.2px; }
.card strong { font-size:17px; }
.description { color:#9daec3; line-height:1.45; font-size:13px; flex:1; }
.link { color:#dce7f7; font-weight:600; font-size:13px; }
footer { margin-top:36px; color:#8294ad; font-size:12px; }
code { background:#1a2b41; border-radius:4px; padding:3px 6px; }
</style>
</head>
<body><main>
<div class='eyebrow'>DEMONSTRACAO &bull; PIPELINE DISTRIBUIDO</div>
<h1>Observability Resilient Pipeline</h1>
<p class='sub'>Central de acesso aos servicos do projeto: FastAPI, Kafka, Prometheus, Grafana e Alertmanager.</p>
<section class='status'><i class='pulse'></i><div><strong>$statusTitle</strong><span>$statusText</span></div></section>
<h2>Monitoramento dos targets</h2>
<div class='targets'>$($targetHtml -join "`n")</div>
<h2>Ferramentas da demonstracao</h2>
<section class='grid'>$($cardsHtml -join "`n")</section>
<footer>Verificacao executada em $checkedAt &bull; Esta pagina e um relatorio estatico da inicializacao. Para atualizar os estados, execute o script novamente.<p>Parar servicos: <code>docker compose stop</code> &bull; Logs: <code>docker compose logs --tail=100 -f</code></p></footer>
</main></body></html>
"@
$out = Join-Path $env:TEMP 'observability-pipeline-launchpad.html'
[System.IO.File]::WriteAllText($out, $html, (New-Object System.Text.UTF8Encoding($false)))

Write-Stage 'Central de demonstracao:'
Write-Host "  $out" -ForegroundColor White
Write-Host '  Navegue pelos links da pagina. Ela abre somente uma aba inicialmente.' -ForegroundColor DarkGray
Write-Host "  Docker Compose: docker compose stop (preserva volumes)" -ForegroundColor DarkGray

if ($OpenBrowser) {
    Start-Process ([System.Uri]::new($out).AbsoluteUri)
}
if ($OpenAllPages) {
    foreach ($key in @('Grafana - Dashboard','Prometheus - Targets','Prometheus - Alertas','Alertmanager')) {
        Start-Process $serviceUrls[$key]
    }
}
if ($allGood) {
    Write-Host "`nSUCESSO: ambiente pronto, 6/6 targets UP e Viewer provisionado." -ForegroundColor Green
    exit 0
}
Write-Host "`nATENCAO: ambiente iniciado, mas faltam verificacoes." -ForegroundColor Yellow
exit 2
