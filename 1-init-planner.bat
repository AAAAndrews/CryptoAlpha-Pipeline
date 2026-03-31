@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
set "ROOT_DIR=%SCRIPT_DIR%"
set "AGENT_DIR=%ROOT_DIR%\scarffold\.agent"
set "REQ_FILE=%ROOT_DIR%\scarffold\requirements.md"
set "ARCH_FILE=%AGENT_DIR%\architecture.md"
set "TASKS_FILE=%AGENT_DIR%\tasks.json"

if not exist "%REQ_FILE%" call :fail "Missing requirements file: %REQ_FILE%"
if not exist "%AGENT_DIR%" call :fail "Missing agent directory: %AGENT_DIR%"
if not exist "%ARCH_FILE%" call :fail "Missing architecture placeholder: %ARCH_FILE%"
if not exist "%TASKS_FILE%" call :fail "Missing tasks file: %TASKS_FILE%"

where claude >nul 2>nul || call :fail "Missing required command: claude"
where git >nul 2>nul || call :fail "Missing required command: git"

if not defined CLAUDE_STREAM set "CLAUDE_STREAM=0"
if not defined CLAUDE_VERBOSE set "CLAUDE_VERBOSE=1"
if not defined CLAUDE_DEBUG set "CLAUDE_DEBUG=0"

set "CLAUDE_ARGS="
if "%CLAUDE_VERBOSE%"=="1" set "CLAUDE_ARGS=%CLAUDE_ARGS% --verbose"
if "%CLAUDE_STREAM%"=="1" set "CLAUDE_ARGS=%CLAUDE_ARGS% --output-format stream-json --include-partial-messages"
if "%CLAUDE_DEBUG%"=="1" (
	set "LOG_DIR=%AGENT_DIR%\logs"
	if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
	set "DEBUG_FILE=%LOG_DIR%\init_planner_%RANDOM%%RANDOM%.log"
	set "CLAUDE_ARGS=%CLAUDE_ARGS% --debug-file ""%DEBUG_FILE%"""
	call :log "Debug log file: %DEBUG_FILE%"
)

set "PROMPT=读取 scarffold/requirements.md，只完成规划/初始化任务：1. 生成或更新 scarffold/.agent/architecture.md（技术栈、目录结构、核心流程）。2. 将需求拆分为细粒度任务（每个任务控制在 10 分钟以内）。3. 生成 scarffold/.agent/tasks.json，严格使用该结构：[{\"id\":1,\"task\":\"...\",\"passes\":false,\"deps\":[]}, ...]。4. 如有需要生成依赖引导文件（requirements.txt 或 package.json）。硬规则：本阶段禁止实现业务功能代码。"

call :log "Starting planner agent..."
call :log "Claude args:%CLAUDE_ARGS%"
claude  "%PROMPT%" %CLAUDE_ARGS%
if errorlevel 1 call :fail "Planner agent execution failed"
call :log "Planner phase completed. Review scarffold/.agent/architecture.md and scarffold/.agent/tasks.json before execution phase."
exit /b 0

:log
echo [init-planner] %~1
exit /b 0

:fail
echo [init-planner][error] %~1 1>&2
exit /b 1
