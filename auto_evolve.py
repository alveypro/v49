#!/usr/bin/env python3
"""Compatibility shim for the migrated auto_evolve module.

Canonical implementation: openclaw/auto_evolve.py
"""

from __future__ import annotations

from openclaw.auto_evolve import *  # noqa: F401,F403


if __name__ == "__main__":
    from openclaw.auto_evolve import main

    raise SystemExit(main())
