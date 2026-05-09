from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_nginx_routes_formal_surface_to_current_productized_topology():
    config = (PROJECT_ROOT / "deploy" / "aliyun" / "nginx.airivo.online.conf").read_text(encoding="utf-8")

    assert "server_name airivo.online www.airivo.online;" in config
    assert "root /opt/airivo/public;" in config
    assert "include /etc/nginx/snippets/airivo-apex.locations.conf;" in config
    assert "location ^~ /stock/" in config
    assert "proxy_pass http://airivo_stock_surface;" in config
    assert "location ^~ /T12/api/stock-ai-runner" in config
    assert "location ^~ /T12/ops/stock-ai-runner" in config
    assert "return 404;" in config
    assert "location = /T12/" in config
    assert "root /opt/t12agent/app/templates;" in config
    assert "location /api/t12/" in config
    assert "proxy_pass http://127.0.0.1:8000;" in config
    assert "proxy_pass http://127.0.0.1:8764;" not in config
    assert "proxy_pass http://127.0.0.1:8766;" not in config


def test_formal_canonical_services_focus_on_stock_surface_and_entry_guard():
    stock = (PROJECT_ROOT / "deploy" / "aliyun" / "stock-ultimate-dashboard.service").read_text(encoding="utf-8")
    guard = (PROJECT_ROOT / "deploy" / "aliyun" / "stock-ultimate-entry-guard.service").read_text(encoding="utf-8")
    guard_timer = (PROJECT_ROOT / "deploy" / "aliyun" / "stock-ultimate-entry-guard.timer").read_text(encoding="utf-8")

    assert "--port 8765 --base-path /stock" in stock
    assert "scripts/run_stock_entry_guard.py" in guard
    assert "artifacts/stock_entry_guard_latest.json" in guard
    assert "OnUnitActiveSec=10min" in guard_timer
    assert "dashboard.err.log" in stock
    assert "entry_guard.err.log" in guard


def test_airivo_apex_sidecar_routes_do_not_reuse_existing_ports_or_paths():
    nginx = (PROJECT_ROOT / "deploy" / "aliyun" / "nginx.airivo-apex.locations.conf").read_text(encoding="utf-8")
    main_site = (PROJECT_ROOT / "deploy" / "aliyun" / "airivo-apex-main-site.service").read_text(encoding="utf-8")
    stock = (PROJECT_ROOT / "deploy" / "aliyun" / "airivo-apex-stock.service").read_text(encoding="utf-8")
    t12 = (PROJECT_ROOT / "deploy" / "aliyun" / "airivo-apex-t12.service").read_text(encoding="utf-8")
    guard = (PROJECT_ROOT / "deploy" / "aliyun" / "airivo-apex-entry-guard.service").read_text(encoding="utf-8")
    guard_timer = (PROJECT_ROOT / "deploy" / "aliyun" / "airivo-apex-entry-guard.timer").read_text(encoding="utf-8")

    assert "location /apex/" in nginx
    assert "Copy to /etc/nginx/snippets/airivo-apex.locations.conf" in nginx
    assert "Do not place this file directly under /etc/nginx/conf.d/*.conf" in nginx
    assert "location ^~ /apex/stock/" in nginx
    assert "return 410;" in nginx
    assert "location /apex/T12/" in nginx
    assert "proxy_pass http://127.0.0.1:18764;" in nginx
    assert "proxy_pass http://127.0.0.1:18765;" not in nginx
    assert "proxy_pass http://127.0.0.1:18766;" in nginx
    assert "/stock/" not in nginx.replace("/apex/stock/", "")
    assert "/T12/" not in nginx.replace("/apex/T12/", "")
    assert "--port 18764 --base-path /apex" in main_site
    assert "--port 18765 --base-path /apex/stock" in stock
    assert "--port 18766 --base-path /apex/T12" in t12
    assert "/var/log/airivo-apex/" in main_site
    assert "/var/log/airivo-apex/" in stock
    assert "/var/log/airivo-apex/" in t12
    assert "scripts/run_stock_entry_guard.py" in guard
    assert "artifacts/stock_entry_guard_latest.json" in guard
    assert "OnUnitActiveSec=10min" in guard_timer
