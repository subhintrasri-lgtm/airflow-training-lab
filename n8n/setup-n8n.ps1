$ErrorActionPreference = "Stop"

Write-Host "[1/4] Starting n8n..." -ForegroundColor Cyan
docker compose up -d n8n
if ($LASTEXITCODE -ne 0) { throw "Cannot start n8n" }

Write-Host "[2/4] Waiting for n8n health check..." -ForegroundColor Cyan
$healthy = $false
for ($attempt = 1; $attempt -le 30; $attempt++) {
    $health = docker inspect --format "{{.State.Health.Status}}" airflow-training-lab-n8n-1 2>$null
    if ($health -eq "healthy") {
        $healthy = $true
        break
    }
    Start-Sleep -Seconds 2
}
if (-not $healthy) { throw "n8n did not become healthy" }

Write-Host "[3/4] Importing and publishing the combined Airflow + Chat workflow..." -ForegroundColor Cyan
docker compose exec -T n8n n8n import:workflow --input=/workflows/mra_airflow_webhook.json
if ($LASTEXITCODE -ne 0) { throw "Cannot import n8n workflow" }
docker compose exec -T n8n n8n publish:workflow --id=MraAirflowWebhook01
if ($LASTEXITCODE -ne 0) { throw "Cannot publish n8n workflow" }

Write-Host "[4/4] Restarting n8n..." -ForegroundColor Cyan
docker compose restart n8n
if ($LASTEXITCODE -ne 0) { throw "Cannot restart n8n" }

Write-Host "Ready: http://localhost:5678 (open Workflows > MRA - Airflow Quality + AI Chat)" -ForegroundColor Green
