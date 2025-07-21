# FESCO Container Tracking - Простой запуск
# Использование: .\start_fesco.ps1 [test|file|db|monitor]

param(
    [string]$Mode = "db",
    [string]$FilePath = "",
    [int]$BatchSize = 100
)

# Получаем путь к скрипту
$ScriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptPath

# Активируем виртуальное окружение
$VenvActivate = ".\\.venv\\Scripts\\Activate.ps1"
if (Test-Path $VenvActivate) {
    & $VenvActivate
}

# Формируем команду запуска
$Arguments = @($Mode)

switch ($Mode) {
    "file" { if ($FilePath) { $Arguments += $FilePath } }
    "db" { $Arguments += "--batch-size"; $Arguments += $BatchSize }
}

# Запускаем main.py
& python main.py @Arguments

# Завершаем работу
exit $LASTEXITCODE