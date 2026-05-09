from __future__ import annotations

from pathlib import Path


_STYLESHEET_TEMPLATE = Path(__file__).with_name(
    "stock_dashboard_styles.css"
).read_text(encoding="utf-8")

_T12_GOVERNANCE_SCOPE_CSS = """
    #t12-governance-summary.t12-governance-summary {
      border: 1px solid rgba(92, 59, 23, 0.12);
      border-radius: 8px;
      padding: 22px;
      background: linear-gradient(180deg, rgba(255, 251, 245, 0.96), rgba(255,255,255,0.94));
      box-shadow: var(--shadow-soft);
    }
    #t12-governance-summary .t12-governance-summary__header {
      margin-bottom: 16px;
    }
    #t12-governance-summary .t12-governance-summary__eyebrow {
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }
    #t12-governance-summary .t12-governance-summary__title {
      margin: 6px 0 8px;
      font-size: 28px;
      line-height: 1.15;
      color: #5a3a16;
    }
    #t12-governance-summary .t12-governance-summary__desc {
      margin: 0;
      font-size: 13px;
      color: var(--muted);
    }
    #t12-governance-summary .t12-governance-summary__grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
    }
    #t12-governance-summary .t12-governance-summary__item {
      border: 1px solid rgba(92, 59, 23, 0.10);
      border-radius: 8px;
      padding: 16px;
      background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(249,244,235,0.82));
    }
    #t12-governance-summary .t12-governance-summary__label {
      font-size: 12px;
      letter-spacing: 0.05em;
      color: var(--muted);
    }
    #t12-governance-summary .t12-governance-summary__value {
      margin-top: 8px;
      font-size: 24px;
      font-weight: 800;
      line-height: 1.2;
      color: #5a3a16;
      word-break: break-word;
    }
    #t12-governance-summary .t12-governance-summary__value--compact {
      font-size: 18px;
      font-weight: 700;
    }
    #t12-readonly-note.t12-readonly-note {
      border-color: rgba(55, 75, 95, 0.12);
    }
    #t12-readonly-note .t12-readonly-note__body {
      font-size: 14px;
      color: var(--muted-strong);
      line-height: 1.7;
    }
    @media (max-width: 900px) {
      #t12-governance-summary .t12-governance-summary__grid {
        grid-template-columns: 1fr;
      }
    }
""".strip("\n")

_FAIL_CLOSED_STYLESHEET = """
body { margin: 0; font-family: "Helvetica Neue", "PingFang SC", sans-serif; background: #f5f1ea; color: #1c2733; }
.shell { max-width: 920px; margin: 0 auto; padding: 40px 24px 56px; }
.panel { background: #fffaf2; border: 1px solid #d7c7b4; border-radius: 8px; padding: 28px; box-shadow: 0 18px 48px rgba(28, 39, 51, 0.08); }
.kicker { font-size: 12px; letter-spacing: 0.18em; text-transform: uppercase; color: #8d4b1f; margin-bottom: 12px; }
h1 { margin: 0 0 12px; font-size: 34px; }
p { line-height: 1.7; color: #4d5c69; }
ul { line-height: 1.8; color: #1c2733; }
.meta { margin-top: 18px; font-size: 14px; color: #6a7783; }
a { color: #0b5f58; text-decoration: none; }
""".strip("\n")


def compose_dashboard_stylesheet(*, is_t12_scope: bool) -> str:
    return _STYLESHEET_TEMPLATE.replace(
        "__T12_GOVERNANCE_SCOPE_CSS__",
        _T12_GOVERNANCE_SCOPE_CSS if is_t12_scope else "",
    )


def compose_fail_closed_stylesheet() -> str:
    return _FAIL_CLOSED_STYLESHEET


def compose_inline_style_tag(stylesheet: str) -> str:
    return f"<style>\n{stylesheet}\n  </style>"


def compose_dashboard_script_tag(*scripts: str) -> str:
    body = "\n".join(script for script in scripts if script)
    return f"<script>\n{body}\n  </script>"
