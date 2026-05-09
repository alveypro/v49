from scripts.prepare_server_sync_manifest import build_server_sync_manifest


def test_prepare_server_sync_manifest_allows_code_and_blocks_runtime_state(tmp_path):
    project_root = tmp_path / "stock_ultimate_system"
    (project_root / "src").mkdir(parents=True)
    (project_root / "scripts").mkdir()
    (project_root / "config").mkdir()
    (project_root / "deploy" / "aliyun").mkdir(parents=True)
    (project_root / "tests").mkdir()
    (project_root / "tools").mkdir()
    (project_root / "data" / "experiments").mkdir(parents=True)
    (project_root / "artifacts" / "baselines").mkdir(parents=True)
    (project_root / "__pycache__").mkdir()

    (project_root / "README.md").write_text("# readme\n", encoding="utf-8")
    (project_root / "src" / "app.py").write_text("x = 1\n", encoding="utf-8")
    (project_root / "scripts" / "tool.py").write_text("x = 1\n", encoding="utf-8")
    (project_root / "config" / "settings.yaml").write_text("x: 1\n", encoding="utf-8")
    (project_root / "deploy" / "aliyun" / "service.timer").write_text("[Timer]\n", encoding="utf-8")
    (project_root / "tests" / "test_app.py").write_text("def test_ok(): pass\n", encoding="utf-8")
    (project_root / "tools" / "check.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    (project_root / "requirements.txt").write_text("pytest\n", encoding="utf-8")
    (project_root / "requirements.stock-scoped.txt").write_text("pyyaml\n", encoding="utf-8")
    (project_root / "data" / "__init__.py").write_text("", encoding="utf-8")
    (project_root / "data" / "experiments" / "latest.json").write_text("{}", encoding="utf-8")
    (project_root / "artifacts" / "baselines" / "current.json").write_text("{}", encoding="utf-8")
    (project_root / ".tushare_token").write_text("secret\n", encoding="utf-8")
    (project_root / "__pycache__" / "app.cpython-310.pyc").write_text("cache\n", encoding="utf-8")

    manifest = build_server_sync_manifest(project_root)
    denied = {item["path"]: item["reason"] for item in manifest["denied_files"]}

    assert "README.md" in manifest["allowed_files"]
    assert "src/app.py" in manifest["allowed_files"]
    assert "scripts/tool.py" in manifest["allowed_files"]
    assert "config/settings.yaml" in manifest["allowed_files"]
    assert "deploy/aliyun/service.timer" in manifest["allowed_files"]
    assert "tests/test_app.py" in manifest["allowed_files"]
    assert "tools/check.sh" in manifest["allowed_files"]
    assert "requirements.txt" in manifest["allowed_files"]
    assert "requirements.stock-scoped.txt" in manifest["allowed_files"]
    assert "data/__init__.py" in manifest["allowed_files"]
    assert denied["data/experiments/latest.json"] == "stateful_runtime_data"
    assert denied["artifacts/baselines/current.json"] == "stateful_runtime_data"
    assert denied[".tushare_token"] == "local_secret_or_machine_file"
    assert denied["__pycache__/app.cpython-310.pyc"] == "cache_or_runtime_directory"


def test_prepare_server_sync_manifest_blocks_nested_runtime_data_directories(tmp_path):
    project_root = tmp_path / "stock_ultimate_system"
    (project_root / "stock_ultimate_system" / "data" / "experiments").mkdir(parents=True)

    nested_runtime = project_root / "stock_ultimate_system" / "data" / "experiments" / "latest.json"
    nested_runtime.write_text("{}", encoding="utf-8")

    manifest = build_server_sync_manifest(project_root)
    denied = {item["path"]: item["reason"] for item in manifest["denied_files"]}

    assert denied["stock_ultimate_system/data/experiments/latest.json"] == "stateful_runtime_data"
