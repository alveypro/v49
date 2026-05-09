import plistlib
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from install_candidate_quality_daily_closure_launchd import LABEL, build_plist, install_launchd


def test_build_candidate_quality_daily_closure_plist_does_not_generate_candidates(tmp_path):
    plist = build_plist(tmp_path, hour=16, minute=10, python_exec="/usr/bin/python3")

    args = plist["ProgramArguments"]
    assert plist["Label"] == LABEL
    assert plist["WorkingDirectory"] == str(tmp_path)
    assert plist["StartCalendarInterval"] == {"Hour": 16, "Minute": 10}
    assert args[:2] == ["/usr/bin/python3", str(tmp_path / "scripts" / "run_candidate_quality_daily_closure.py")]
    assert "--json" in args
    assert "--generate-candidates" not in args
    assert "--expanded-universe-size" not in args
    assert plist["RunAtLoad"] is False


def test_install_candidate_quality_daily_closure_launchd_writes_plist_to_given_dir(tmp_path):
    launch_agents = tmp_path / "LaunchAgents"
    plist_path = install_launchd(
        tmp_path,
        hour=17,
        minute=5,
        python_exec="/usr/bin/python3",
        launch_agents_dir=launch_agents,
    )

    assert plist_path == launch_agents / f"{LABEL}.plist"
    with plist_path.open("rb") as f:
        plist = plistlib.load(f)
    assert plist["StartCalendarInterval"] == {"Hour": 17, "Minute": 5}
    assert plist["ProgramArguments"][0] == "/usr/bin/python3"
