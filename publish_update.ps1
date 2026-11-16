param(
    [string]$Message = "Update immich-sync",
    [switch]$DirectDocker,
    [string]$Image = ""
)

$ErrorActionPreference = "Stop"

if ($DirectDocker) {
    if (-not $Image) {
        $remoteUrl = ""
        try { $remoteUrl = (git remote get-url origin 2>$null) } catch {}
        if ($remoteUrl -match "github\.com[:/]([^/]+)/([^/.]+)") {
            $owner = $Matches[1].ToLower()
            $repo = $Matches[2].ToLower()
            $Image = "ghcr.io/$owner/$repo`:latest"
        } else {
            Write-Host "Please pass -Image, e.g.: .\publish_update.ps1 -DirectDocker -Image ghcr.io/YOUR_USER/immich-sync:latest" -ForegroundColor Yellow
            exit 1
        }
    }

    Write-Host "Building and pushing Docker image $Image (linux/amd64)..." -ForegroundColor Cyan
    docker buildx build --platform linux/amd64 -t $Image --push .
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    Write-Host "Done! In Unraid Docker tab, click 'Check for Updates' and 'Apply Update'." -ForegroundColor Green
    exit 0
}

$inGit = $false
try {
    git rev-parse --is-inside-work-tree 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { $inGit = $true }
} catch {}

if (-not $inGit) {
    Write-Host "Git is not initialized yet. Run these 3 commands once to host your installer on GitHub & GHCR:" -ForegroundColor Yellow
    Write-Host "  git init && git branch -M main && git add -A && git commit -m 'Initial immich-sync setup'" -ForegroundColor Cyan
    Write-Host "  git remote add origin https://github.com/YOUR_GITHUB_USERNAME/immich-sync.git" -ForegroundColor Cyan
    Write-Host "  git push -u origin main" -ForegroundColor Cyan
    exit 1
}

$origin = ""
try { $origin = (git remote get-url origin 2>$null) } catch {}
if (-not $origin) {
    Write-Host "No 'origin' git remote configured. Add your GitHub repo and push once:" -ForegroundColor Yellow
    Write-Host "  git remote add origin https://github.com/YOUR_GITHUB_USERNAME/immich-sync.git" -ForegroundColor Cyan
    Write-Host "  git push -u origin main" -ForegroundColor Cyan
    exit 1
}

$status = git status --porcelain
if ($status) {
    Write-Host "Staging and committing local changes..." -ForegroundColor Cyan
    git add -A
    git commit -m $Message
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} else {
    Write-Host "Working tree clean; pushing existing commits..." -ForegroundColor Cyan
}

git push -u origin HEAD
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Pushed to $origin! GitHub Actions is now building and publishing the updated container image." -ForegroundColor Green
Write-Host "Once the workflow finishes, click 'Check for Updates' -> 'Apply Update' in the Unraid Docker tab." -ForegroundColor Green
