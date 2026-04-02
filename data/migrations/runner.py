from __future__ import annotations

import re
from pathlib import Path
from typing import List, Tuple

from data.dao import db_conn


MIG_DIR = Path(__file__).resolve().parent
FILE_RE = re.compile(r"^(\d+)_(.+)\.sql$")


def _list_migrations() -> List[Tuple[int, str, Path]]:
    out: List[Tuple[int, str, Path]] = []
    for p in sorted(MIG_DIR.glob("*.sql")):
        m = FILE_RE.match(p.name)
        if not m:
            continue
        out.append((int(m.group(1)), m.group(2), p))
    return out


def apply_migrations(preferred_db_path: str | None = None) -> dict:
    migrations = _list_migrations()
    applied = []

    with db_conn(preferred_db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute("SELECT version FROM schema_version")
        existing = {int(r[0]) for r in cur.fetchall()}

        for version, name, path in migrations:
            if version in existing:
                continue
            sql = path.read_text(encoding="utf-8")
            cur.executescript(sql)
            cur.execute("INSERT INTO schema_version(version, name) VALUES(?, ?)", (version, name))
            applied.append({"version": version, "name": name, "file": str(path)})

        conn.commit()

    return {"applied": applied, "count": len(applied)}


if __name__ == "__main__":
    result = apply_migrations()
    print(result)
