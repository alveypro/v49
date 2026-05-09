from pathlib import Path

from src.daily_research_models import AlertRecord, HealthSnapshot, ProfileRunRecord, ResearchPaths


def test_research_paths_from_summary_path():
    paths = ResearchPaths.from_summary_path(Path('/tmp/daily_research_latest.md'))
    assert paths.history_path == Path('/tmp/daily_research_history.jsonl')
    assert paths.health_csv_path == Path('/tmp/daily_health_trend_latest.csv')


def test_alert_record_to_dict():
    alert = AlertRecord(level='warning', category='health', message='x')
    assert alert.to_dict() == {'level': 'warning', 'category': 'health', 'message': 'x'}


def test_health_snapshot_roundtrip_and_trend_entry():
    snapshot = HealthSnapshot.from_dict({
        'score': 88.5,
        'success_rate': 0.9,
        'failed_count': 1.0,
        'success_component': 90.0,
        'failure_penalty': 5.0,
        'category_penalty': 2.0,
    })
    assert snapshot.to_dict()['score'] == 88.5
    trend = snapshot.trend_entry(alerts_count=2, generated_at='2026-03-18T10:00:00')
    assert trend['alerts_count'] == 2
    assert trend['generated_at'] == '2026-03-18T10:00:00'


def test_profile_run_record_roundtrip():
    run = ProfileRunRecord.from_payload(
        profile='short',
        status='ok',
        attempts=2,
        duration_sec=1.25,
        payload={'top_result': {'run_id': 'r1'}, 'error': ''},
    )
    assert run.profile == 'short'
    assert run.top_result['run_id'] == 'r1'
    assert run.to_dict()['attempts'] == 2
