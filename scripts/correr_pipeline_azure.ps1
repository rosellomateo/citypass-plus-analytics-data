<#
.SYNOPSIS
    Dispara a demanda las 10 funciones (silver + gold, x5 dominios) del
    Function App ya desplegado en Azure, usando la API admin en vez del
    boton "Prueba/ejecucion" del portal (que puede quedar deshabilitado en
    apps desplegadas por CI/CD).

.USAGE
    1. Completar $FUNCTION_APP_HOSTNAME mas abajo (una sola vez, esto no es
       secreto) con el hostname completo tal cual aparece en "Informacion
       general" del portal (algunos planes de Azure le agregan un sufijo
       random + region, no siempre es solo "nombre.azurewebsites.net").
    2. Guardar la master key en local.settings.json (que ya esta gitignorado,
       igual que STORAGE_CONNECTION_STRING), en la clave
       AZURE_FUNCTIONS_MASTER_KEY. Alternativa sin tocar archivos: cargarla
       en la variable de entorno CITYPASS_MASTER_KEY antes de correr el
       script (tiene prioridad sobre local.settings.json).
    3. Correr: .\scripts\correr_pipeline_azure.ps1
       (o con un dominio puntual: .\scripts\correr_pipeline_azure.ps1 -Dominio reclamos)

    La master key se consigue en el portal: raiz del Function App (no la
    funcion individual) -> "Funciones" -> "Claves de la aplicacion" -> _master.
#>

param(
    [string]$Dominio = ""  # vacio = todos; o uno de: reclamos, movilidad, residuos, espacios, emergencias
)

# --- Completar antes de correr -------------------------------------------
$FUNCTION_APP_HOSTNAME = "functioneda-ffh4byc5bdbpf2ep.brazilsouth-01.azurewebsites.net"
# ---------------------------------------------------------------------------

# La master key nunca se hardcodea aca: primero se busca en el entorno
# (para overridearla sin tocar archivos), y si no esta, se lee de
# local.settings.json (gitignorado), igual que hace scripts/_storage.py con
# STORAGE_CONNECTION_STRING.
$MASTER_KEY = $env:CITYPASS_MASTER_KEY
if ([string]::IsNullOrWhiteSpace($MASTER_KEY)) {
    $localSettingsPath = Join-Path $PSScriptRoot "..\local.settings.json"
    if (Test-Path $localSettingsPath) {
        $localSettings = Get-Content $localSettingsPath -Raw | ConvertFrom-Json
        $MASTER_KEY = $localSettings.Values.AZURE_FUNCTIONS_MASTER_KEY
    }
}
if ($MASTER_KEY -eq "PEGAR_ACA_LA_MASTER_KEY_DEL_PORTAL") {
    $MASTER_KEY = $null  # sigue siendo el placeholder sin completar
}

$BASE_URL = "https://$FUNCTION_APP_HOSTNAME/admin/functions"
$SEGUNDOS_ESPERA_ENTRE_SILVER_Y_GOLD = 20

$DOMINIOS_TODOS = @("reclamos", "movilidad", "residuos", "espacios", "emergencias")
$dominios = if ($Dominio) { @($Dominio) } else { $DOMINIOS_TODOS }

function Invoke-FuncionAzure {
    param([string]$NombreFuncion)

    Write-Host "  -> Disparando $NombreFuncion..." -NoNewline
    try {
        Invoke-RestMethod -Method Post `
            -Uri "$BASE_URL/$NombreFuncion" `
            -Headers @{ "x-functions-key" = $MASTER_KEY; "Content-Type" = "application/json" } `
            -Body '{"input": ""}' `
            -ErrorAction Stop | Out-Null
        Write-Host " OK (202 Accepted)" -ForegroundColor Green
    } catch {
        Write-Host " ERROR: $($_.Exception.Message)" -ForegroundColor Red
    }
}

if ([string]::IsNullOrWhiteSpace($MASTER_KEY)) {
    Write-Host "Falta la master key. Alguna de las dos:" -ForegroundColor Yellow
    Write-Host '  - Completa AZURE_FUNCTIONS_MASTER_KEY en local.settings.json' -ForegroundColor Yellow
    Write-Host '  - O en esta misma terminal: $env:CITYPASS_MASTER_KEY = "la-master-key-del-portal"' -ForegroundColor Yellow
    exit 1
}

foreach ($d in $dominios) {
    Write-Host "`n=== $d ===" -ForegroundColor Cyan

    Invoke-FuncionAzure "${d}_bronze_a_silver"
    Write-Host "  Esperando $SEGUNDOS_ESPERA_ENTRE_SILVER_Y_GOLD s a que termine silver antes de correr gold..."
    Start-Sleep -Seconds $SEGUNDOS_ESPERA_ENTRE_SILVER_Y_GOLD

    Invoke-FuncionAzure "${d}_silver_a_gold"
}

Write-Host "`nListo. Revisa 'Registros' (Log stream) de cada funcion en el portal para confirmar que no hubo errores." -ForegroundColor Cyan
