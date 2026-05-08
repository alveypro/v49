from pathlib import Path

from src.daily_research_models import ResearchPaths


def test_research_paths_include_status_path():
    summary = Path("/tmp/daily_research_latest.md")
    paths = ResearchPaths.from_summary_path(summary)
    assert paths.history_path.name == "daily_research_history.jsonl"
    assert paths.health_csv_path.name == "daily_health_trend_latest.csv"
    assert paths.status_path.name == "daily_research_status_latest.json"
