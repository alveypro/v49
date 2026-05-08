from pathlib import Path


def test_daily_operations_scoreboard_service_runs_refresh_orchestrator():
    project_root = Path(__file__).resolve().parents[1]
    service = project_root / "deploy" / "aliyun" / "airivo-apex-daily-operations-scoreboard.service"

    text = service.read_text(encoding="utf-8")

    assert "scripts/refresh_primary_result_operations_artifacts.py" in text
    assert "primary_result_operations_refresh_latest.json" in text
    assert "build_primary_result_daily_operations_scoreboard.py" not in text


def test_deploy_doc_describes_operations_refresh_outputs():
    project_root = Path(__file__).resolve().parents[1]
    deploy_doc = project_root / "deploy" / "aliyun" / "DEPLOY.md"

    text = deploy_doc.read_text(encoding="utf-8")

    assert "primary_result_operations_refresh_latest.json" in text
    assert "只刷新本地证据 summary" in text
