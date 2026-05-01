from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_release_gate_has_explicit_opt_in_fact_readiness_gate():
    script = (ROOT / "tools" / "release_gate.sh").read_text(encoding="utf-8")

    assert "AIRIVO_ENABLE_RELEASE_FACT_GATE" in script
    assert "AIRIVO_RELEASE_DB_PATH" in script
    assert "RELEASE_READINESS_PAYLOAD_FILE" in script
    assert "--operator release_gate" in script
    assert "RELEASE_GATE_FACT_STATUS" in script


def test_release_gate_fact_readiness_gate_is_blocking_when_enabled():
    script = (ROOT / "tools" / "release_gate.sh").read_text(encoding="utf-8")
    match = re.search(
        r"tools/release_dry_run_audit\.py(?P<body>.*?)--operator release_gate",
        script,
        flags=re.DOTALL,
    )

    assert match is not None
    assert "--db \"$AIRIVO_RELEASE_DB_PATH\"" in match.group("body")
    assert "--non-blocking" not in match.group("body")
    assert "exit 1" in script
