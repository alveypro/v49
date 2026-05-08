from src.main_site_home import render_main_site_home


def test_main_site_home_contract_is_stable():
    html_text = render_main_site_home()

    assert 'id="main-site-home"' in html_text
    assert 'id="main-site-primary-cta"' in html_text
    assert 'id="main-site-product-matrix"' in html_text
    assert 'id="main-site-stock-card"' in html_text
    assert 'id="main-site-t12-card"' in html_text
    assert 'data-airivo-hook="main-site-hero"' in html_text
    assert 'data-airivo-hook="main-site-primary-cta"' in html_text
    assert 'data-airivo-hook="main-site-product-matrix"' in html_text
    assert 'data-airivo-hook="main-site-stock-card"' in html_text
    assert 'data-airivo-hook="main-site-t12-card"' in html_text
    assert 'href="/stock/"' in html_text
    assert 'href="/T12/"' in html_text
    assert "进入 /stock 主结果系统" in html_text
    assert "airivo.online/stock" in html_text
    assert "Core System" in html_text
    assert "airivo.online/T12" in html_text
    assert "Governance" in html_text
    assert "主站不输出业务主判断" in html_text
    assert "统一结果对象" not in html_text
    assert "当前推进状态" not in html_text
