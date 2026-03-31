import os
import sys
import json
import time
import random
import subprocess
import shutil
from pathlib import Path

def log(msg):
    print(f"[run-coder] {msg}")

def fail(msg):
    print(f"[run-coder][error] {msg}", file=sys.stderr)
    sys.exit(1)

def validate_tasks_file(tasks_file):
    with open(tasks_file, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            fail("tasks.json is not valid JSON")

    if not isinstance(data, list):
        fail("tasks.json must be a JSON array")

    for idx, item in enumerate(data, 1):
        if not isinstance(item, dict):
            fail(f"task #{idx}: each item must be an object")
        keys = set(item.keys())
        allowed = {"id", "task", "passes", "deps"}
        if keys != allowed:
            fail(f"task #{idx}: keys must be exactly {sorted(allowed)}")
        if not isinstance(item["id"], int) or item["id"] < 1:
            fail(f"task #{idx}: id must be integer >= 1")
        if not isinstance(item["task"], str) or not item["task"].strip():
            fail(f"task #{idx}: task must be non-empty string")
        if not isinstance(item["passes"], bool):
            fail(f"task #{idx}: passes must be boolean")
        if not isinstance(item["deps"], list) or not all(isinstance(d, int) and d >= 1 for d in item["deps"]):
            fail(f"task #{idx}: deps must be integer array >= 1")

def has_pending_tasks(tasks_file):
    with open(tasks_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    return any(not item.get("passes", True) for item in data)

def main():
    root_dir = Path(__file__).resolve().parent
    agent_dir = root_dir / "scarffold" / ".agent"
    tasks_file = agent_dir / "tasks.json"

    max_loops = int(os.environ.get("MAX_LOOPS", "20"))
    use_dangerous = os.environ.get("DANGEROUS_SKIP_PERMISSIONS", "1") == "1"
    claude_stream = os.environ.get("CLAUDE_STREAM", "0")
    claude_verbose = os.environ.get("CLAUDE_VERBOSE", "1")
    claude_debug = os.environ.get("CLAUDE_DEBUG", "0")

    if not agent_dir.exists():
        fail(f"Missing agent directory: {agent_dir}")
    if not tasks_file.exists():
        fail(f"Missing tasks file: {tasks_file}")

    if not shutil.which("claude"):
        fail("Missing required command: claude")

    claude_args = []
    if claude_verbose == "1":
        claude_args.append("--verbose")
    if claude_stream == "1":
        claude_args.extend(["--output-format", "stream-json", "--include-partial-messages"])
    if claude_debug == "1":
        log_dir = agent_dir / "logs"
        log_dir.mkdir(exist_ok=True)
        debug_file = log_dir / f"run_coder_{random.randint(1000, 999999)}.log"
        claude_args.extend(["--debug-file", str(debug_file)])
        log(f"Debug log file: {debug_file}")

    prompt = "严格遵循 CLAUDE.md 的 SOP 只执行一个下一个待完成任务,完成验证,更新状态,提交代码,然后立即退出."
    
    for count in range(max_loops):
        validate_tasks_file(tasks_file)
        
        if not has_pending_tasks(tasks_file):
            log("All tasks are complete.")
            sys.exit(0)
            
        iter_num = count + 1
        log(f"Starting iteration {iter_num}/{max_loops}")
        log(f"Claude args: {' '.join(claude_args)}")
        
        cmd = ["claude", prompt]
        if use_dangerous:
            cmd.append("--dangerously-skip-permissions")
        cmd.extend(claude_args)
        
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError:
            fail("Claude execution failed")
            
        time.sleep(2)
        
    fail(f"Reached MAX_LOOPS={max_loops} before all tasks were completed")

if __name__ == "__main__":
    main()