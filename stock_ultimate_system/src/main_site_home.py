from __future__ import annotations

from src.main_site_conversion_events import build_main_site_conversion_event_definitions


MAIN_SITE_HOME_PAGE_VERSION = "main_site_home.v1"
MAIN_SITE_CONVERSION_EVENTS = build_main_site_conversion_event_definitions()


def _normalize_base_path(base_path: str) -> str:
    raw = (base_path or "").strip()
    if not raw or raw == "/":
        return ""
    return "/" + raw.strip("/")


def _href(base_path: str, suffix: str) -> str:
    base = _normalize_base_path(base_path)
    if not base:
        return suffix
    resolved = base + "/" + suffix.lstrip("/")
    return resolved


def render_main_site_home(base_path: str = "") -> str:
    normalized_base_path = _normalize_base_path(base_path)
    is_apex_namespace = normalized_base_path.lower() == "/apex"
    stock_href = _href(base_path, "/stock/")
    # Apex landing must not herd users into /apex/T12 (internal-validation mirror).
    # Governance entry stays the canonical prod path /T12/ (same nginx server).
    t12_href = "/T12/" if is_apex_namespace else _href(base_path, "/T12/")
    public_root = "airivo.online" + (normalized_base_path or "")
    public_stock = public_root + "/stock"
    public_t12 = "airivo.online/T12" if is_apex_namespace else public_root + "/T12"
    hero_kicker = "Internal Validation" if is_apex_namespace else "Product Matrix"
    hero_title = (
        "供内部验证与预发布使用的受控命名空间"
        if is_apex_namespace
        else "以 /stock 为核心结果系统的产品矩阵首页"
    )
    hero_summary = (
        "Apex 只提供内部验证、预发布、旁路复核入口，不承担对外正式产品职责。"
        " 这里保留 main site、/stock、/T12 的镜像路径，只用于验证同一条正式主链。"
        if is_apex_namespace
        else "Airivo 主站只承担平台表达、产品矩阵组织、入口分发与成熟度信任表达。"
        " 业务主结果只在 /stock 产生，治理与边界只在 /T12 承担。"
    )
    primary_cta_label = "进入 /stock 内部验证入口" if is_apex_namespace else "进入 /stock 主结果系统"
    secondary_cta_label = "查看 /T12 内部只读入口" if is_apex_namespace else "查看 /T12 治理与边界系统"
    hero_note = (
        "Apex 不是新的正式产品系统，不参与对外正式产品叙事，不替代 airivo.online/stock。"
        if is_apex_namespace
        else "主站不输出业务主判断，不替代 /stock，不把 /T12 治理摘要拉回主结果入口。"
    )
    side_label = "Internal Use" if is_apex_namespace else "Maturity"
    side_title = (
        "内部验证环境，不新增产品职责"
        if is_apex_namespace
        else "单轨 canonical 运行，围绕 /stock 累积长期竞争力"
    )
    side_copy = (
        "当前入口只表示内部验证空间，继续服务正式三系统，不与正式主站或 /stock 并列为第二正式面。"
        if is_apex_namespace
        else "当前产品矩阵已明确：主站负责入口，/stock 负责核心结果，/T12 负责治理边界。"
    )
    root_card_copy = (
        "内部验证入口，不承载新的产品职责，也不构成对外正式产品面。"
        if is_apex_namespace
        else "品牌、平台、导航与信任表达，不承载业务主判断。"
    )
    stock_card_link_label = "进入 /stock 内部验证入口" if is_apex_namespace else "进入主系统"
    t12_card_link_label = "查看 /T12 内部只读入口" if is_apex_namespace else "查看治理系统"
    return """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Airivo</title>
  <style>
    :root {
      --ink: #112033;
      --muted: #546476;
      --line: rgba(17, 32, 51, 0.12);
      --panel: rgba(255, 255, 255, 0.86);
      --accent: #0f766e;
      --accent-strong: #0b5f58;
      --warm: #f2ede4;
      --sky: #dfeef3;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: "Helvetica Neue", "PingFang SC", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(15, 118, 110, 0.12), transparent 32%),
        linear-gradient(180deg, var(--warm) 0%, #fcfaf6 46%, var(--sky) 100%);
    }
    .main-site-shell { max-width: 1180px; margin: 0 auto; padding: 40px 24px 56px; }
    .main-site-topbar { display: flex; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 28px; }
    .main-site-brand { font-size: 14px; letter-spacing: 0.22em; text-transform: uppercase; color: var(--muted); }
    .main-site-links { display: flex; gap: 12px; flex-wrap: wrap; }
    .main-site-links a { text-decoration: none; color: var(--ink); font-size: 14px; }
    .main-site-hero { display: grid; grid-template-columns: minmax(0, 1.45fr) minmax(320px, 0.85fr); gap: 24px; margin-bottom: 28px; }
    .main-site-hero-panel, .main-site-side-panel, .main-site-card, .main-site-trust {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 28px;
      backdrop-filter: blur(10px);
      box-shadow: 0 14px 40px rgba(17, 32, 51, 0.08);
    }
    .main-site-hero-panel { padding: 34px; }
    .main-site-kicker { font-size: 13px; letter-spacing: 0.18em; text-transform: uppercase; color: var(--accent); margin-bottom: 14px; }
    .main-site-title { margin: 0 0 14px; font-size: clamp(38px, 5vw, 64px); line-height: 0.96; }
    .main-site-summary { margin: 0 0 24px; max-width: 720px; font-size: 17px; line-height: 1.6; color: var(--muted); }
    .main-site-cta-row { display: flex; gap: 14px; flex-wrap: wrap; margin-bottom: 24px; }
    .main-site-cta, .main-site-cta-secondary { text-decoration: none; border-radius: 999px; padding: 14px 22px; font-weight: 600; }
    .main-site-cta { background: var(--accent); color: white; }
    .main-site-cta:hover { background: var(--accent-strong); }
    .main-site-cta-secondary { border: 1px solid var(--line); color: var(--ink); background: rgba(255, 255, 255, 0.72); }
    .main-site-hero-note { color: var(--muted); font-size: 14px; line-height: 1.7; }
    .main-site-side-panel { padding: 28px; display: grid; gap: 18px; align-content: start; }
    .main-site-side-label { font-size: 13px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.16em; }
    .main-site-side-title { margin: 0; font-size: 24px; line-height: 1.2; }
    .main-site-side-copy { margin: 0; color: var(--muted); line-height: 1.7; font-size: 15px; }
    .main-site-matrix { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 18px; margin-bottom: 28px; }
    .main-site-card { padding: 24px; display: grid; gap: 12px; }
    .main-site-card--primary { border-color: rgba(15, 118, 110, 0.34); box-shadow: 0 18px 46px rgba(15, 118, 110, 0.12); }
    .main-site-card-kicker { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.16em; }
    .main-site-card-title { margin: 0; font-size: 24px; }
    .main-site-card-copy { margin: 0; color: var(--muted); line-height: 1.7; font-size: 15px; }
    .main-site-card-link { margin-top: 8px; text-decoration: none; color: var(--accent-strong); font-weight: 600; }
    .main-site-trust-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 18px; }
    .main-site-trust { padding: 24px; }
    .main-site-trust-title { margin: 0 0 8px; font-size: 18px; }
    .main-site-trust-copy { margin: 0; color: var(--muted); line-height: 1.7; font-size: 15px; }
    @media (max-width: 960px) {
      .main-site-hero, .main-site-matrix, .main-site-trust-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main class="main-site-shell" id="main-site-home">
    <div class="main-site-topbar">
      <div class="main-site-brand">Airivo</div>
      <nav class="main-site-links" aria-label="产品矩阵导航">
        <a href="{stock_href}">/stock</a>
        <a href="{t12_href}">/T12</a>
      </nav>
    </div>
    <section class="main-site-hero" data-airivo-hook="main-site-hero">
      <div class="main-site-hero-panel" data-airivo-hook="main-site-hero-panel">
        <div class="main-site-kicker">{hero_kicker}</div>
        <h1 class="main-site-title">{hero_title}</h1>
        <p class="main-site-summary">
          {hero_summary}
        </p>
        <div class="main-site-cta-row" data-airivo-hook="main-site-cta-row">
          <a class="main-site-cta" id="main-site-primary-cta" data-airivo-hook="main-site-primary-cta" href="{stock_href}">{primary_cta_label}</a>
          <a class="main-site-cta-secondary" data-airivo-hook="main-site-secondary-cta" href="{t12_href}">{secondary_cta_label}</a>
        </div>
        <div class="main-site-hero-note">
          {hero_note}
        </div>
      </div>
      <aside class="main-site-side-panel">
        <div class="main-site-side-label">{side_label}</div>
        <h2 class="main-site-side-title">{side_title}</h2>
        <p class="main-site-side-copy">
          {side_copy}
        </p>
      </aside>
    </section>
    <section class="main-site-matrix" id="main-site-product-matrix" data-airivo-hook="main-site-product-matrix">
      <article class="main-site-card">
        <div class="main-site-card-kicker">Platform</div>
        <h3 class="main-site-card-title">{public_root}</h3>
        <p class="main-site-card-copy">{root_card_copy}</p>
      </article>
      <article class="main-site-card main-site-card--primary" id="main-site-stock-card" data-airivo-hook="main-site-stock-card">
        <div class="main-site-card-kicker">Core System</div>
        <h3 class="main-site-card-title">{public_stock}</h3>
        <p class="main-site-card-copy">主分析、主结果、解释链与持续优化能力的中心系统。</p>
        <a class="main-site-card-link" data-airivo-hook="main-site-stock-card-link" href="{stock_href}">{stock_card_link_label}</a>
      </article>
      <article class="main-site-card" id="main-site-t12-card" data-airivo-hook="main-site-t12-card">
        <div class="main-site-card-kicker">Governance</div>
        <h3 class="main-site-card-title">{public_t12}</h3>
        <p class="main-site-card-copy">只读治理摘要、制度状态与边界说明，不扩成交互控制台。</p>
        <a class="main-site-card-link" data-airivo-hook="main-site-t12-card-link" href="{t12_href}">{t12_card_link_label}</a>
      </article>
    </section>
    <section class="main-site-trust-grid" id="main-site-trust">
      <div class="main-site-trust">
        <h3 class="main-site-trust-title">结果中心</h3>
        <p class="main-site-trust-copy">主站把用户导向 /stock，而不是在首页替代结果判断。</p>
      </div>
      <div class="main-site-trust">
        <h3 class="main-site-trust-title">单轨稳定</h3>
        <p class="main-site-trust-copy">/stock 已进入 canonical 单轨运行，主结果契约、布局契约与内容质量持续固定。</p>
      </div>
      <div class="main-site-trust">
        <h3 class="main-site-trust-title">Benchmark 化</h3>
        <p class="main-site-trust-copy">围绕 benchmark 与 golden set 建立持续验证体系，确保主结果越来越强。</p>
      </div>
    </section>
  </main>
</body>
</html>""".replace("{stock_href}", stock_href).replace("{t12_href}", t12_href).replace(
        "{public_root}", public_root
    ).replace("{public_stock}", public_stock).replace("{public_t12}", public_t12).replace(
        "{hero_kicker}", hero_kicker
    ).replace("{hero_title}", hero_title).replace("{hero_summary}", hero_summary).replace(
        "{primary_cta_label}", primary_cta_label
    ).replace("{secondary_cta_label}", secondary_cta_label).replace("{hero_note}", hero_note).replace(
        "{side_label}", side_label
    ).replace("{side_title}", side_title).replace("{side_copy}", side_copy).replace(
        "{root_card_copy}", root_card_copy
    ).replace("{stock_card_link_label}", stock_card_link_label).replace(
        "{t12_card_link_label}", t12_card_link_label
    )
