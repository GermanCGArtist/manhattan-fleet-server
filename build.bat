@echo off
rem Builds manhattan-fleet-server. Output: build\libs\manhattan-fleet-server-<version>.jar
set "ROOT=%~dp0"
for /d %%D in ("%ROOT%tools\jdk-25*") do set "JAVA_HOME=%%~fD"
if not defined JAVA_HOME (
    for /d %%D in ("%ROOT%..\Manhattan\tools\jdk-25*") do set "JAVA_HOME=%%~fD"
)
if not exist "%ROOT%tmp" mkdir "%ROOT%tmp"
set "TMP=%ROOT%tmp"
set "TEMP=%ROOT%tmp"
set "JAVA_TOOL_OPTIONS=-Djdk.net.unixdomain.tmpdir=%ROOT%tmp"
call "%~dp0gradlew.bat" build %*
