"""Minimal Streamlit stub for offline strategy module loading.

This allows importing legacy Streamlit-heavy strategy files in headless jobs
where streamlit is not installed (e.g., openclaw scan runtime).
"""

from __future__ import annotations

import sys
import types
from typing import Any, Callable


class _DummyContext:
    def __enter__(self) -> "_DummyContext":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def __iter__(self):
        return iter(())

    def __call__(self, *args: Any, **kwargs: Any) -> "_DummyContext":
        return self

    def __bool__(self) -> bool:
        return False


class _DummyStreamlit(types.ModuleType):
    def __init__(self) -> None:
        super().__init__("streamlit")
        self.session_state = {}

    def __getattr__(self, name: str) -> Any:
        if name == "cache_data" or name == "cache_resource":
            return self._decorator
        if name in {"tabs", "columns"}:
            return self._multi_context
        if name in {"expander", "sidebar", "spinner", "container", "form"}:
            return self._context
        if name == "stop":
            return self._stop
        if name == "rerun":
            return self._noop
        if name in {"button", "checkbox", "toggle"}:
            return lambda *a, **k: False
        if name in {"radio", "selectbox"}:
            return lambda *a, **k: (a[1][0] if len(a) > 1 and isinstance(a[1], (list, tuple)) and a[1] else None)
        if name == "slider":
            return lambda *a, **k: (a[2] if len(a) > 2 else k.get("value"))
        if name in {"number_input"}:
            return lambda *a, **k: k.get("value", 0)
        if name in {"text_input", "text_area", "chat_input"}:
            return lambda *a, **k: ""
        if name in {"file_uploader"}:
            return lambda *a, **k: None
        if name in {"dataframe", "table", "plotly_chart", "line_chart", "bar_chart"}:
            return self._noop
        if name in {"metric", "markdown", "write", "title", "header", "subheader", "caption", "code", "json"}:
            return self._noop
        if name in {"success", "info", "warning", "error"}:
            return self._noop
        return self._context

    @staticmethod
    def _noop(*args: Any, **kwargs: Any) -> None:
        return None

    @staticmethod
    def _stop(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("streamlit.stop() called in offline stub context")

    @staticmethod
    def _decorator(func: Callable | None = None, **kwargs: Any):
        def _wrap(f: Callable) -> Callable:
            return f

        if callable(func):
            return func
        return _wrap

    @staticmethod
    def _context(*args: Any, **kwargs: Any) -> _DummyContext:
        return _DummyContext()

    @staticmethod
    def _multi_context(*args: Any, **kwargs: Any):
        n = 0
        if args and isinstance(args[0], (list, tuple)):
            n = len(args[0])
        if n <= 0:
            n = int(kwargs.get("n", 2))
        return [_DummyContext() for _ in range(n)]


def install_streamlit_stub() -> None:
    if "streamlit" not in sys.modules:
        sys.modules["streamlit"] = _DummyStreamlit()

