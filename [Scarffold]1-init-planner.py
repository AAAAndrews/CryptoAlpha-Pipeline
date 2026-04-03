import os
import sys
import subprocess
import shutil
from pathlib import Path

def log(msg):
    print(f"[init-planner] {msg}")

def fail(msg):
    print(f"[init-planner][error] {msg}", file=sys.stderr)
    sys.exit(1)

def main():
    root_dir = Path(__file__).resolve().parent
    agent_dir = root_dir / "scarffold" / ".agent"
    req_file = root_dir / "scarffold" / "requirements.md"
    arch_file = agent_dir / "architecture.md"
    tasks_file = agent_dir / "tasks.json"

    if not req_file.exists():
        fail(f"Missing requirements file: {req_file}")
    if not agent_dir.exists():
        fail(f"Missing agent directory: {agent_dir}")
    if not arch_file.exists():
        fail(f"Missing architecture placeholder: {arch_file}")
    if not tasks_file.exists():
        fail(f"Missing tasks file: {tasks_file}")

    if not shutil.which("claude"):
        fail("Missing required command: claude")
    if not shutil.which("git"):
        fail("Missing required command: git")
        

    claude_stream = os.environ.get("CLAUDE_STREAM", "0")
    claude_verbose = os.environ.get("CLAUDE_VERBOSE", "1")
    claude_debug = os.environ.get("CLAUDE_DEBUG", "0")

    claude_args = []
    if claude_verbose == "1":
        claude_args.append("--verbose")
    if claude_stream == "1":
        claude_args.extend(["--output-format", "stream-json", "--include-partial-messages"])
    if claude_debug == "1":
        log_dir = agent_dir / "logs"
        log_dir.mkdir(exist_ok=True)
        import random
        debug_file = log_dir / f"init_planner_{random.randint(1000, 999999)}.log"
        claude_args.extend(["--debug-file", str(debug_file)])
        log(f"Debug log file: {debug_file}")

    prompt = (
        "读取 scarffold/requirements.md并理解,只完成规划/初始化任务："
        "它可能是既有项目的新需求，或者是一个全新项目的需求。无论是哪种情况，你都需要根据需求进行规划和初始化，确保项目能够顺利进行到下一个阶段。请按照以下步骤进行："
        "1. 生成或更新 scarffold/.agent/architecture.md（技术栈、目录结构、核心流程）.更改文件前请务必向用户进行确认"
        "2. 将需求拆分为细粒度任务（每个任务控制在 10 分钟以内）"
        "3. 根据你对项目理解，在细颗粒度任务之间完成一个或多个功能的测试任务，以逐步验证项目功能正常运转，无比保证序号正确"
        "4. 检查progress.md是否存在且内容是否与旧任务有关联，并向用户确认是进行续写还是新开一个progress.md,如果是新开，请把旧文档移到scarffold/.agent/history目录下并重命名为progress_时间戳.md"
        "5. 检查task.json是否存在,内容是否是旧任务，如果是旧任务，向用户确认进行续写还是归档并新开一个task.json,如果是新开，请把旧的task.json移到scarffold/.agent/history目录下并重命名为tasks_时间戳.json"
        "6. 生成或完善 scarffold/.agent/tasks.json,严格使用该结构：[{\"id\":1,\"task\":\"...\",\"passes\":false,\"deps\":[]}, ...]."
        "7. 如有需要生成依赖引导文件（requirements.txt 或 package.json）.硬规则：本阶段禁止实现业务功能代码."
    )

    log("Starting planner agent...")
    log(f"Claude args: {' '.join(claude_args)}")
    
    cmd = ["claude", prompt] + claude_args
    
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        fail("Planner agent execution failed")
    
    log("Planner phase completed. Review scarffold/.agent/architecture.md and scarffold/.agent/tasks.json before execution phase.")

if __name__ == "__main__":
    main()
