from pathlib import Path

from src.airivo_scope_registry import (
    AIRIVO_NAMESPACE_REGISTRY,
    AIRIVO_NAMESPACE_REGISTRY_VERSION,
    AIRIVO_SCOPE_REGISTRY,
    AIRIVO_SCOPE_REGISTRY_VERSION,
    build_airivo_scope_release_readiness_matrix,
    get_airivo_namespace,
    get_airivo_scope,
    resolve_airivo_namespace_scope,
)


def test_airivo_scope_registry_defines_three_entry_scopes():
    assert set(AIRIVO_SCOPE_REGISTRY.keys()) == {"main_site", "stock", "t12"}
    assert get_airivo_scope("main_site").route == "/"
    assert get_airivo_scope("stock").route == "/stock"
    assert get_airivo_scope("t12").route == "/T12"


def test_airivo_scope_registry_keeps_scope_roles_separate():
    main_site = get_airivo_scope("main_site")
    stock = get_airivo_scope("stock")
    t12 = get_airivo_scope("t12")

    assert "stock_business_primary_judgement" in main_site.forbidden_capabilities
    assert "governance_primary_summary" in stock.forbidden_capabilities
    assert "stock_business_primary_judgement" in t12.forbidden_capabilities
    assert "governance_writeback_action" in t12.forbidden_capabilities
    assert "baseline_promotion" in stock.allowed_capabilities
    assert "readonly_governance_summary" in t12.allowed_capabilities
    assert "main_site_runtime_event_envelope" in main_site.allowed_capabilities


def test_airivo_scope_registry_references_existing_modules_and_tests():
    project_root = Path(__file__).resolve().parents[1]
    for scope in AIRIVO_SCOPE_REGISTRY.values():
        for module_path in scope.owning_modules:
            assert (project_root / module_path).exists(), module_path
        for test_path in scope.smoke_required_tests:
            assert (project_root / test_path).exists(), test_path
        for test_path in scope.required_tests:
            assert (project_root / test_path).exists(), test_path


def test_airivo_scope_release_readiness_matrix_is_machine_readable():
    matrix = build_airivo_scope_release_readiness_matrix()

    assert matrix["registry_version"] == AIRIVO_SCOPE_REGISTRY_VERSION
    assert matrix["namespace_registry_version"] == AIRIVO_NAMESPACE_REGISTRY_VERSION
    assert matrix["scopes"]["main_site"]["role"] == "brand_entry_and_product_matrix"
    assert matrix["namespaces"]["apex"]["role"] == "internal_validation_namespace"
    assert matrix["namespaces"]["apex"]["scope_routes"]["stock"] == "/apex/stock"
    assert "tests/test_artifact_registry.py" in matrix["scopes"]["stock"]["smoke_required_tests"]
    assert "tests/test_main_chain_authenticity_integration.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "tests/test_main_chain_recovery_integration.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "tests/test_candidate_quality_evaluation.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "tests/test_candidate_quality_diff.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "tests/test_candidate_quality_multiwindow_source.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "tests/test_candidate_quality_multiwindow.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "tests/test_register_candidate_quality_baseline.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "tests/test_primary_result_failure_attribution.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "tests/test_primary_result_failure_attribution_ledger.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "tests/test_primary_result_feedback_loop.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "tests/test_run_stock_release_pipeline_fast.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "tests/test_run_stock_release_pipeline_functional.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "tests/test_run_stock_release_pipeline_integration.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "tests/test_run_stock_release_pipeline_e2e.py" in matrix["scopes"]["stock"]["required_tests"]
    assert "release_evidence_bundle" in matrix["scopes"]["stock"]["release_evidence_expectations"]
    assert "no_action_writeback" in matrix["scopes"]["t12"]["release_evidence_expectations"]


def test_airivo_namespace_registry_keeps_apex_as_namespace_not_fourth_system():
    assert set(AIRIVO_NAMESPACE_REGISTRY.keys()) == {"production", "apex"}
    assert get_airivo_namespace("production").route_prefix == ""
    assert get_airivo_namespace("production").role == "formal_public_entry_namespace"
    assert get_airivo_namespace("apex").route_prefix == "/apex"
    assert get_airivo_namespace("apex").role == "internal_validation_namespace"
    assert get_airivo_namespace("apex").scope_routes["main_site"] == "/apex"
    assert get_airivo_namespace("apex").scope_routes["stock"] == "/apex/stock"
    assert get_airivo_namespace("apex").scope_routes["t12"] == "/apex/T12"


def test_airivo_namespace_scope_resolution_uses_registry_as_single_route_source():
    production_namespace, main_site_scope = resolve_airivo_namespace_scope("/")
    apex_namespace, apex_stock_scope = resolve_airivo_namespace_scope("/apex/stock")
    apex_t12_namespace, apex_t12_scope = resolve_airivo_namespace_scope("/apex/T12")

    assert production_namespace.namespace_id == "production"
    assert main_site_scope.scope_id == "main_site"
    assert apex_namespace.namespace_id == "apex"
    assert apex_stock_scope.scope_id == "stock"
    assert apex_t12_namespace.namespace_id == "apex"
    assert apex_t12_scope.scope_id == "t12"


def test_airivo_namespace_registry_maps_only_three_formal_system_scopes():
    formal_scope_ids = set(AIRIVO_SCOPE_REGISTRY.keys())
    assert formal_scope_ids == {"main_site", "stock", "t12"}
    for namespace in AIRIVO_NAMESPACE_REGISTRY.values():
        assert set(namespace.scope_routes.keys()) == formal_scope_ids
