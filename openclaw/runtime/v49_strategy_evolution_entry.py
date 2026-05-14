from __future__ import annotations

from typing import Any, Callable, Dict


def is_v49_strategy_evolution_route(routes: Dict[str, str]) -> bool:
    return routes.get("root") == "生产后台" and routes.get("production") == "策略演进"


def render_v49_strategy_evolution_entry(
    *,
    routes: Dict[str, str],
    permanent_db_path: str,
    airivo_snapshot: Dict[str, Any],
    render_airivo_production_dashboard: Callable[[str], Any],
    render_airivo_strategy_evolution: Callable[..., Any],
) -> Dict[str, Any]:
    if not is_v49_strategy_evolution_route(routes):
        return airivo_snapshot
    if not airivo_snapshot:
        airivo_snapshot = render_airivo_production_dashboard(permanent_db_path)
    render_airivo_strategy_evolution(permanent_db_path, airivo_snapshot if isinstance(airivo_snapshot, dict) else {})
    return airivo_snapshot if isinstance(airivo_snapshot, dict) else {}
