@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
set "ROOT_DIR=%SCRIPT_DIR%"
set "AGENT_DIR=%ROOT_DIR%\scarffold\.agent"
set "TASKS_FILE=%AGENT_DIR%\tasks.json"

if not defined MAX_LOOPS set "MAX_LOOPS=20"
if not defined DANGEROUS_SKIP_PERMISSIONS set "DANGEROUS_SKIP_PERMISSIONS=1"
set "USE_DANGEROUS=%DANGEROUS_SKIP_PERMISSIONS%"
if not defined CLAUDE_STREAM set "CLAUDE_STREAM=0"
if not defined CLAUDE_VERBOSE set "CLAUDE_VERBOSE=1"
if not defined CLAUDE_DEBUG set "CLAUDE_DEBUG=0"

set "CLAUDE_ARGS="
if "%CLAUDE_VERBOSE%"=="1" set "CLAUDE_ARGS=%CLAUDE_ARGS% --verbose"
if "%CLAUDE_STREAM%"=="1" set "CLAUDE_ARGS=%CLAUDE_ARGS% --output-format stream-json --include-partial-messages"
if "%CLAUDE_DEBUG%"=="1" (
  set "LOG_DIR=%AGENT_DIR%\logs"
  if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
  set "DEBUG_FILE=%LOG_DIR%\run_coder_%RANDOM%%RANDOM%.log"
  set "CLAUDE_ARGS=%CLAUDE_ARGS% --debug-file ""%DEBUG_FILE%"""
)

if not exist "%AGENT_DIR%" call :fail "Missing agent directory: %AGENT_DIR%"
if not exist "%TASKS_FILE%" call :fail "Missing tasks file: %TASKS_FILE%"

where claude >nul 2>nul || call :fail "Missing required command: claude"
call :detect_python
if not defined PYTHON_EXE call :fail "Missing required command: python3 (or py/python)"

call :write_helper_scripts

set /a COUNT=0
:loop
if %COUNT% GEQ %MAX_LOOPS% goto max_loops_reached

call :validate_tasks_file
if errorlevel 1 goto cleanup_fail

call :has_pending_tasks
if errorlevel 1 goto cleanup_fail
if /I "%PENDING%"=="no" (
  call :log "All tasks are complete."
  goto cleanup_ok
)

set /a ITER=COUNT+1
call :log "Starting iteration !ITER!/%MAX_LOOPS%"
if defined DEBUG_FILE call :log "Debug log file: !DEBUG_FILE!"
call :log "Claude args:%CLAUDE_ARGS%"
if "%USE_DANGEROUS%"=="1" (
  claude "严格遵循 CLAUDE.md 的 SOP。只执行一个下一个待完成任务，完成验证，更新状态，提交代码，然后立即退出。" --dangerously-skip-permissions %CLAUDE_ARGS%
) else (
  claude "严格遵循 CLAUDE.md 的 SOP。只执行一个下一个待完成任务，完成验证，更新状态，提交代码，然后立即退出。" %CLAUDE_ARGS%
)
if errorlevel 1 goto cleanup_fail

set /a COUNT+=1
timeout /t 2 /nobreak >nul
goto loop

:max_loops_reached
call :fail "Reached MAX_LOOPS=%MAX_LOOPS% before all tasks were completed"
goto cleanup_fail

:detect_python
set "PYTHON_EXE="
set "PYTHON_ARGS="
where py >nul 2>nul && (
  set "PYTHON_EXE=py"
  set "PYTHON_ARGS=-3"
  exit /b 0
)
where python >nul 2>nul && (
  set "PYTHON_EXE=python"
  set "PYTHON_ARGS="
  exit /b 0
)
exit /b 0

:write_helper_scripts
set "VALIDATE_PY=%TEMP%\run_coder_validate_%RANDOM%%RANDOM%.py"
set "PENDING_PY=%TEMP%\run_coder_pending_%RANDOM%%RANDOM%.py"

(
  echo import json
  echo import sys
  echo.
  echo path = sys.argv[1]
  echo with open(path, "r", encoding="utf-8") as f:
  echo     data = json.load(f)
  echo.
  echo if not isinstance(data, list):
  echo     raise SystemExit("tasks.json must be a JSON array")
  echo.
  echo for idx, item in enumerate(data, 1):
  echo     if not isinstance(item, dict):
  echo         raise SystemExit(f"task #{idx}: each item must be an object")
  echo     keys = set(item.keys())
  echo     allowed = {"id", "task", "passes", "deps"}
  echo     if keys != allowed:
  echo         raise SystemExit(f"task #{idx}: keys must be exactly {sorted(allowed)}")
  echo     if not isinstance(item["id"], int) or item["id"] ^< 1:
  echo         raise SystemExit(f"task #{idx}: id must be integer ^>= 1")
  echo     if not isinstance(item["task"], str) or not item["task"].strip():
  echo         raise SystemExit(f"task #{idx}: task must be non-empty string")
  echo     if not isinstance(item["passes"], bool):
  echo         raise SystemExit(f"task #{idx}: passes must be boolean")
  echo     if not isinstance(item["deps"], list) or not all(isinstance(d, int) and d ^>= 1 for d in item["deps"]):
  echo         raise SystemExit(f"task #{idx}: deps must be integer array ^>= 1")
) > "%VALIDATE_PY%"

(
  echo import json
  echo import sys
  echo.
  echo with open(sys.argv[1], "r", encoding="utf-8") as f:
  echo     data = json.load(f)
  echo.
  echo pending = any(not item.get("passes", True) for item in data)
  echo print("yes" if pending else "no")
) > "%PENDING_PY%"

exit /b 0

:validate_tasks_file
"%PYTHON_EXE%" %PYTHON_ARGS% "%VALIDATE_PY%" "%TASKS_FILE%"
exit /b %errorlevel%

:has_pending_tasks
set "PENDING="
for /f "usebackq delims=" %%I in (`"%PYTHON_EXE%" %PYTHON_ARGS% "%PENDING_PY%" "%TASKS_FILE%"`) do set "PENDING=%%I"
if not defined PENDING exit /b 1
exit /b 0

:log
echo [run-coder] %~1
exit /b 0

:cleanup_ok
call :cleanup
exit /b 0

:cleanup_fail
call :cleanup
exit /b 1

:cleanup
if defined VALIDATE_PY if exist "%VALIDATE_PY%" del /q "%VALIDATE_PY%" >nul 2>nul
if defined PENDING_PY if exist "%PENDING_PY%" del /q "%PENDING_PY%" >nul 2>nul
exit /b 0

:fail
echo [run-coder][error] %~1 1>&2
exit /b 1
