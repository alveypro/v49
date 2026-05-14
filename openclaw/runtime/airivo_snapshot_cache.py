from __future__ import annotations

from typing import Any, Tuple

import pandas as pd
import streamlit as st

from openclaw.services.airivo_dashboard_snapshot_service import (
    data_freshness_snapshot as service_data_freshness_snapshot,
    latest_candidate_snapshot as service_latest_candidate_snapshot,
)
from openclaw.services.airivo_execution_read_service import (
    feedback_snapshot as service_feedback_snapshot,
)


@st.cache_data(ttl=60, show_spinner=False)
def data_freshness_snapshot_cached(db_path: str, db_mtime: float) -> dict[str, Any]:
    return service_data_freshness_snapshot(db_path)


@st.cache_data(ttl=60, show_spinner=False)
def latest_candidate_snapshot_cached(
    db_path: str,
    db_mtime: float,
    limit: int = 5,
) -> Tuple[pd.DataFrame, str]:
    return service_latest_candidate_snapshot(db_path, limit=limit)


@st.cache_data(ttl=60, show_spinner=False)
def feedback_snapshot_cached(db_path: str, db_mtime: float) -> dict[str, Any]:
    return service_feedback_snapshot(db_path)
