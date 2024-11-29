@echo off
REM Use this script to run your program LOCALLY on Windows.
REM
REM Note: Changing this script WILL NOT affect how CodeCrafters runs your program.
REM
REM Learn more: https://codecrafters.io/program-interface

REM Change to repository directory
cd /d "%~dp0"

REM Compile the program (copied from .codecrafters/compile.sh)
call mvn -B package -Ddir=C:\tmp\codecrafters-build-redis-java

REM Run the program (copied from .codecrafters/run.sh) 
java -jar C:\tmp\codecrafters-build-redis-java\codecrafters-redis.jar %*

exit /b %ERRORLEVEL%
