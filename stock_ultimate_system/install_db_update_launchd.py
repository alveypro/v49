from __future__ import annotations

import argparse
import os
import plistlib
import subprocess
import sys
from pathlib import Path


def build_plist(
    project_root: Path,
    hour: int,
    minute: int,
    post_universe_size: int,
    post_top_n: int,
    post_research_profiles: list[str],
    python_exec: str | None = None,
) -> dict:
    label = "com.stockultimate.dbupdate"
    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    resolved_python_exec = python_exec or sys.executable
    script = project_root / "run_update_database.py"
    return {
        "Label": label,
        "ProgramArguments": [
            resolved_python_exec,
            str(script),
            "--config-dir",
            "config",
            "--post-top-candidates",
            "--post-universe-size",
            str(int(post_universe_size)),
            "--post-top-n",
            str(int(post_top_n)),
            "--post-daily-research",
        ] + (["--post-research-profiles", *post_research_profiles] if post_research_profiles else []),
        "WorkingDirectory": str(project_root),
        "StartCalendarInterval": {
            "Hour": int(hour),
            "Minute": int(minute),
        },
        "RunAtLoad": True,
        "StandardOutPath": str(logs_dir / "db_update.launchd.log"),
        "StandardErrorPath": str(logs_dir / "db_update.launchd.err"),
    }


def install_launchd(
    project_root: Path,
    hour: int,
    minute: int,
    post_universe_size: int,
    post_top_n: int,
    post_research_profiles: list[str],
    python_exec: str | None = None,
) -> Path:
    launch_agents = Path.home() / "Library" / "LaunchAgents"
    launch_agents.mkdir(parents=True, exist_ok=True)
    plist_path = launch_agents / "com.stockultimate.dbupdate.plist"
    existing_python_exec = _existing_python_exec(plist_path)
    plist = build_plist(
        project_root,
        hour,
        minute,
        post_universe_size,
        post_top_n,
        post_research_profiles,
        python_exec=python_exec or existing_python_exec,
    )
    with plist_path.open("wb") as f:
        plistlib.dump(plist, f)
    return plist_path


def _existing_python_exec(plist_path: Path) -> str | None:
    if not plist_path.exists():
        return None
    try:
        with plist_path.open("rb") as f:
            plist = plistlib.load(f)
    except Exception:
        return None
    args = plist.get("ProgramArguments") if isinstance(plist, dict) else None
    if not isinstance(args, list) or not args:
        return None
    python_exec = str(args[0] or "").strip()
    if python_exec and Path(python_exec).exists():
        return python_exec
    return None


def reload_launchd(plist_path: Path) -> None:
    domain = f"gui/{os.getuid()}"
    subprocess.run(["launchctl", "bootout", domain, str(plist_path)], check=False)
    subprocess.run(["launchctl", "bootstrap", domain, str(plist_path)], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="安装17:30数据库更新 launchd 定时任务")
    parser.add_argument("--hour", type=int, default=17)
    parser.add_argument("--minute", type=int, default=30)
    parser.add_argument("--post-universe-size", type=int, default=300, help="更新后自动候选股票池规模（0=全市场）")
    parser.add_argument("--post-top-n", type=int, default=10, help="更新后自动候选输出数量")
    parser.add_argument(
        "--post-research-profiles",
        nargs="*",
        default=["short", "medium"],
        choices=["short", "medium", "long"],
        help="更新后自动每日研究的 profile 序列",
    )
    parser.add_argument("--python-exec", help="launchd 使用的 Python 解释器；默认沿用现有 plist，否则使用当前解释器")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    plist_path = install_launchd(
        project_root,
        args.hour,
        args.minute,
        args.post_universe_size,
        args.post_top_n,
        args.post_research_profiles,
        args.python_exec,
    )
    reload_launchd(plist_path)
    print(f"已安装并加载定时任务: {plist_path}")
    print(f"Python: {build_plist(project_root, args.hour, args.minute, args.post_universe_size, args.post_top_n, args.post_research_profiles, args.python_exec or _existing_python_exec(plist_path))['ProgramArguments'][0]}")
    print(f"执行时间: 每天 {args.hour:02d}:{args.minute:02d}")
    print(f"更新后自动候选：top_n={args.post_top_n}, universe_size={args.post_universe_size}")
    print(f"更新后自动每日研究：profiles={args.post_research_profiles}")
    print("注意：脚本内部会按交易日历判断，非交易日自动跳过。")


if __name__ == "__main__":
    main()
