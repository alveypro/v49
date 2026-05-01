from __future__ import annotations

from openclaw.runtime.v49_strategy_evolution_entry import (
    is_v49_strategy_evolution_route,
    render_v49_strategy_evolution_entry,
)


def _recorder(calls, name, return_value=None):
    def _inner(*args, **kwargs):
        calls.append((name, args, kwargs))
        return return_value

    return _inner


def test_v49_strategy_evolution_route_predicate_freezes_scope():
    assert is_v49_strategy_evolution_route({"root": "生产后台", "production": "策略演进"})
    assert not is_v49_strategy_evolution_route({"root": "生产后台", "production": "今日决策"})


def test_render_v49_strategy_evolution_entry_loads_snapshot_when_missing():
    calls = []
    result = render_v49_strategy_evolution_entry(
        routes={"root": "生产后台", "production": "策略演进"},
        permanent_db_path="/tmp/db.sqlite",
        airivo_snapshot={},
        render_airivo_production_dashboard=_recorder(calls, "dashboard", {"snapshot": True}),
        render_airivo_strategy_evolution=_recorder(calls, "evolution"),
    )

    assert result == {"snapshot": True}
    assert [call[0] for call in calls] == ["dashboard", "evolution"]


def test_render_v49_strategy_evolution_entry_skips_non_matching_route():
    calls = []
    result = render_v49_strategy_evolution_entry(
        routes={"root": "研究后台", "research": "股票池分析"},
        permanent_db_path="/tmp/db.sqlite",
        airivo_snapshot={"old": True},
        render_airivo_production_dashboard=_recorder(calls, "dashboard"),
        render_airivo_strategy_evolution=_recorder(calls, "evolution"),
    )

    assert result == {"old": True}
    assert calls == []
