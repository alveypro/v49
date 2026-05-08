from __future__ import annotations

from dataclasses import dataclass


AIRIVO_SCOPE_REGISTRY_VERSION = "airivo_scope_registry.v1"
AIRIVO_NAMESPACE_REGISTRY_VERSION = "airivo_namespace_registry.v1"


@dataclass(frozen=True)
class AirivoScopeDefinition:
    scope_id: str
    route: str
    role: str
    allowed_capabilities: tuple[str, ...]
    forbidden_capabilities: tuple[str, ...]
    owning_modules: tuple[str, ...]
    smoke_required_tests: tuple[str, ...]
    required_tests: tuple[str, ...]
    release_evidence_expectations: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "scope_id": self.scope_id,
            "route": self.route,
            "role": self.role,
            "allowed_capabilities": list(self.allowed_capabilities),
            "forbidden_capabilities": list(self.forbidden_capabilities),
            "owning_modules": list(self.owning_modules),
            "smoke_required_tests": list(self.smoke_required_tests),
            "required_tests": list(self.required_tests),
            "release_evidence_expectations": list(self.release_evidence_expectations),
        }


@dataclass(frozen=True)
class AirivoNamespaceDefinition:
    namespace_id: str
    route_prefix: str
    role: str
    description: str
    scope_routes: dict[str, str]

    def as_dict(self) -> dict[str, object]:
        return {
            "namespace_id": self.namespace_id,
            "route_prefix": self.route_prefix,
            "role": self.role,
            "description": self.description,
            "scope_routes": dict(self.scope_routes),
        }


AIRIVO_SCOPE_REGISTRY: dict[str, AirivoScopeDefinition] = {
    "main_site": AirivoScopeDefinition(
        scope_id="main_site",
        route="/",
        role="brand_entry_and_product_matrix",
        allowed_capabilities=(
            "platform_positioning",
            "product_matrix_navigation",
            "main_site_conversion_payload",
            "main_site_runtime_event_envelope",
            "local_jsonl_event_sink",
        ),
        forbidden_capabilities=(
            "stock_business_primary_judgement",
            "governance_primary_summary",
            "trading_or_governance_action_console",
        ),
        owning_modules=(
            "src/main_site_home.py",
            "src/main_site_conversion_events.py",
            "src/main_site_event_envelope.py",
            "src/main_site_event_bridge.py",
            "src/main_site_event_sink.py",
        ),
        smoke_required_tests=(
            "tests/test_main_site_home.py",
            "tests/test_main_site_conversion_events.py",
            "tests/test_main_site_event_bridge.py",
        ),
        required_tests=(
            "tests/test_main_site_home.py",
            "tests/test_main_site_home_contract.py",
            "tests/test_main_site_conversion_events.py",
            "tests/test_main_site_conversion_event_contract.py",
            "tests/test_main_site_event_envelope.py",
            "tests/test_main_site_event_bridge.py",
            "tests/test_main_site_event_sink_jsonl.py",
        ),
        release_evidence_expectations=(
            "main_site_home_contract",
            "main_site_conversion_payload_contract",
            "main_site_runtime_event_protocol",
        ),
    ),
    "stock": AirivoScopeDefinition(
        scope_id="stock",
        route="/stock",
        role="core_business_primary_result",
        allowed_capabilities=(
            "stock_research_primary_result",
            "canonical_view_model",
            "benchmark_report",
            "benchmark_diff",
            "release_pipeline",
            "baseline_promotion",
            "artifact_registry",
        ),
        forbidden_capabilities=(
            "platform_brand_homepage",
            "governance_primary_summary",
            "t12_readonly_system_embed",
        ),
        owning_modules=(
            "src/stock_primary_result.py",
            "src/stock_primary_result_benchmark_report.py",
            "src/stock_primary_result_benchmark_diff.py",
            "src/stock_baseline_registry.py",
            "src/release_pipeline_steps.py",
            "src/artifact_registry.py",
        ),
        smoke_required_tests=(
            "tests/test_stock_primary_result.py",
            "tests/test_stock_primary_result_benchmark_report.py",
            "tests/test_artifact_registry.py",
        ),
        required_tests=(
            "tests/test_stock_primary_result.py",
            "tests/test_stock_primary_result_benchmarks.py",
            "tests/test_stock_primary_result_benchmark_report.py",
            "tests/test_stock_primary_result_benchmark_diff.py",
            "tests/test_candidate_quality_evaluation.py",
            "tests/test_candidate_quality_diff.py",
            "tests/test_candidate_quality_multiwindow_source.py",
            "tests/test_candidate_quality_multiwindow.py",
            "tests/test_register_candidate_quality_baseline.py",
            "tests/test_primary_result_failure_attribution.py",
            "tests/test_primary_result_failure_attribution_ledger.py",
            "tests/test_primary_result_feedback_loop.py",
            "tests/test_main_chain_authenticity_integration.py",
            "tests/test_main_chain_recovery_integration.py",
            "tests/test_promote_stock_baseline_success.py",
            "tests/test_baseline_registry_current_pointer.py",
            "tests/test_run_stock_release_pipeline_fast.py",
            "tests/test_run_stock_release_pipeline_functional.py",
            "tests/test_run_stock_release_pipeline_integration.py",
            "tests/test_run_stock_release_pipeline_e2e.py",
            "tests/test_artifact_registry.py",
        ),
        release_evidence_expectations=(
            "benchmark_report",
            "benchmark_diff",
            "release_gates",
            "release_evidence_bundle",
            "release_pipeline_manifest",
            "artifact_registry",
            "baseline_snapshot_when_promoted",
        ),
    ),
    "t12": AirivoScopeDefinition(
        scope_id="t12",
        route="/T12",
        role="governance_readonly_boundary",
        allowed_capabilities=(
            "readonly_governance_summary",
            "readonly_system_status",
            "boundary_explanation",
            "shared_fact_consumption",
        ),
        forbidden_capabilities=(
            "stock_business_primary_judgement",
            "trading_action_console",
            "governance_writeback_action",
            "platform_homepage_replacement",
            "stock_primary_contract_override",
        ),
        owning_modules=(
            "src/t12_governance_summary.py",
            "src/t12_overview_card.py",
        ),
        smoke_required_tests=(
            "tests/test_t12_governance_summary.py",
            "tests/test_t12_overview_card.py",
        ),
        required_tests=(
            "tests/test_t12_governance_summary.py",
            "tests/test_t12_overview_card.py",
            "tests/test_run_dashboard_primary_result_api.py",
        ),
        release_evidence_expectations=(
            "t12_readonly_contract",
            "t12_scope_rendering",
            "no_action_writeback",
            "no_stock_primary_override",
        ),
    ),
}


AIRIVO_NAMESPACE_REGISTRY: dict[str, AirivoNamespaceDefinition] = {
    "production": AirivoNamespaceDefinition(
        namespace_id="production",
        route_prefix="",
        role="formal_public_entry_namespace",
        description="正式生产入口 namespace，对外承载主站、/stock、/T12 三系统。",
        scope_routes={
            "main_site": "/",
            "stock": "/stock",
            "t12": "/T12",
        },
    ),
    "apex": AirivoNamespaceDefinition(
        namespace_id="apex",
        route_prefix="/apex",
        role="internal_validation_namespace",
        description="内部验证与预发布 namespace，不新增系统职责，只为 main_site、stock、t12 提供受控验证路径空间。",
        scope_routes={
            "main_site": "/apex",
            "stock": "/apex/stock",
            "t12": "/apex/T12",
        },
    ),
}


def get_airivo_scope(scope_id: str) -> AirivoScopeDefinition:
    return AIRIVO_SCOPE_REGISTRY[scope_id]


def get_airivo_namespace(namespace_id: str) -> AirivoNamespaceDefinition:
    return AIRIVO_NAMESPACE_REGISTRY[namespace_id]


def _normalize_airivo_route_path(route: str) -> str:
    raw = str(route or "").strip()
    if not raw or raw == "/":
        return "/"
    return "/" + raw.strip("/")


def resolve_airivo_namespace_scope(route: str) -> tuple[AirivoNamespaceDefinition, AirivoScopeDefinition]:
    normalized = _normalize_airivo_route_path(route).lower()
    for namespace in AIRIVO_NAMESPACE_REGISTRY.values():
        for scope_id, scope_route in namespace.scope_routes.items():
            if _normalize_airivo_route_path(scope_route).lower() == normalized:
                return namespace, AIRIVO_SCOPE_REGISTRY[scope_id]
    raise KeyError(f"unregistered airivo route: {route}")


def build_airivo_scope_release_readiness_matrix() -> dict[str, object]:
    return {
        "registry_version": AIRIVO_SCOPE_REGISTRY_VERSION,
        "namespace_registry_version": AIRIVO_NAMESPACE_REGISTRY_VERSION,
        "scopes": {
            scope_id: scope.as_dict()
            for scope_id, scope in AIRIVO_SCOPE_REGISTRY.items()
        },
        "namespaces": {
            namespace_id: namespace.as_dict()
            for namespace_id, namespace in AIRIVO_NAMESPACE_REGISTRY.items()
        },
    }
