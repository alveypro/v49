from run_dashboard import _render_dashboard


def test_main_site_home_executes_as_product_matrix_entry():
    html_text = _render_dashboard(root=None, base_path="")
    assert 'id="main-site-home"' in html_text
    assert 'id="main-site-primary-cta"' in html_text
    assert 'id="main-site-product-matrix"' in html_text
    assert 'href="/stock/"' in html_text
    assert 'href="/T12/"' in html_text
    assert "进入 /stock 主结果系统" in html_text
    assert "查看 /T12 治理与边界系统" in html_text
    assert "airivo.online/stock" in html_text
    assert "Core System" in html_text
    assert "主站不输出业务主判断" in html_text
    assert "统一结果对象" not in html_text
    assert "当前推进状态" not in html_text


def test_main_site_home_executes_as_apex_sidecar_entry():
    html_text = _render_dashboard(root=None, base_path="/apex")

    assert 'id="main-site-home"' in html_text
    assert 'href="/apex/stock/"' in html_text
    assert 'href="/T12/"' in html_text
    assert 'href="/apex/T12/"' not in html_text
    assert "Internal Validation" in html_text
    assert "内部验证环境，不新增产品职责" in html_text
    assert "不参与对外正式产品叙事" in html_text
    assert "airivo.online/apex" in html_text
    assert "airivo.online/apex/stock" in html_text
    assert "airivo.online/T12" in html_text
    assert "进入 /stock 内部验证入口" in html_text
    assert "查看 /T12 内部只读入口" in html_text
    assert "进入主系统" not in html_text
    assert "查看治理系统" not in html_text
