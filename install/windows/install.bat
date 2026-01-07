@echo off
setlocal

set CLI_NAME=enso
set BINARY_NAME=enso.exe
set INSTALL_DIR=%USERPROFILE%\AppData\Local\Programs\%CLI_NAME%
set RELEASE_URL=https://github.com/aether-flux/ensodb/releases/latest/download/%BINARY_NAME%

echo - Installing %CLI_NAME%...

:: Create install dir if it doesn't exist
if not exist "%INSTALL_DIR%" (
  mkdir "%INSTALL_DIR%"
)

:: Download binary
echo - Downloading latest release...
powershell -Command "Invoke-WebRequest -Uri '%RELEASE_URL%' -OutFile '%INSTALL_DIR%\%BINARY_NAME%'"

:: Create shim as enso.cmd
echo - Creating enso.cmd shim...
> "%INSTALL_DIR%\enso.cmd" echo @echo off
>> "%INSTALL_DIR%\enso.cmd" echo "%%~dp0%BINARY_NAME%" %%*

:: Add install dir to PATH (user) if not already there
echo - Adding %INSTALL_DIR% to User PATH...
powershell -Command "$oldPath = [Environment]::GetEnvironmentVariable('PATH', 'User'); if (-not ($oldPath -split ';' | Where-Object { $_ -eq '%INSTALL_DIR%' })) { $newPath = $oldPath + ';%INSTALL_DIR%'; [Environment]::SetEnvironmentVariable('PATH', $newPath, 'User'); Write-Output '- Added to PATH.' } else { Write-Output '- Already in PATH.' }"

:: Also update PATH for current session so enso works immediately
set PATH=%INSTALL_DIR%;%PATH%

echo --- Done! You can now run "%CLI_NAME%" from any new terminal window.
echo --- If it's not working, reboot your system and try again.
p
