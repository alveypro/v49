from __future__ import annotations

import html


def compose_fail_closed_page_html(
    *,
    fail_closed_style_tag: str,
    problems_html: str,
    primary_result_api_href: str,
) -> str:
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>/stock Fail Closed</title>
  {fail_closed_style_tag}
</head>
<body>
  <main class="shell">
    <section class="panel">
      <div class="kicker">Fail Closed</div>
      <h1>/stock 当前禁止输出主结果</h1>
      <p>入口校验发现主链不完整。根据严格执行标准，页面不能在 pointer 或 lifecycle evidence 缺失时继续展示看似合理的主结论。</p>
      <ul>{problems_html}</ul>
      <div class="meta">当前模式：只允许人工复核和链路修复，不允许继续发布主结果。 API 也会返回 blocked 状态。</div>
      <div class="meta"><a href="{html.escape(primary_result_api_href, quote=True)}">查看 primary result API</a></div>
    </section>
  </main>
</body>
</html>"""
