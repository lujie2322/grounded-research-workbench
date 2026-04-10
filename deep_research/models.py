from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SearchPlan:
    task: str
    intent: str
    keywords: list[str] = field(default_factory=list)
    structured_targets: list[str] = field(default_factory=list)
    unstructured_targets: list[str] = field(default_factory=list)
    required_agents: list[str] = field(default_factory=list)
    report_focus: list[str] = field(default_factory=list)


@dataclass
class ResearchTask:
    task: str
    mode: str = "report"
    market_scope: list[str] = field(default_factory=list)
    symbols: list[str] = field(default_factory=list)
    metrics: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    report_style: str = "professional"
    output_name: str = ""


@dataclass
class CollectedItem:
    item_id: str
    source_type: str
    source_name: str
    title: str
    summary: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)
    score: float = 0.0


@dataclass
class AnalysisArtifact:
    qualitative_findings: list[str] = field(default_factory=list)
    quantitative_findings: list[str] = field(default_factory=list)
    financial_comparison: list[str] = field(default_factory=list)
    sentiment_findings: list[str] = field(default_factory=list)
    policy_findings: list[str] = field(default_factory=list)
    policy_impact_chains: list[str] = field(default_factory=list)
    community_findings: list[str] = field(default_factory=list)
    scorecard_rows: list[dict[str, str]] = field(default_factory=list)
    sentiment_dashboard: list[dict[str, str]] = field(default_factory=list)
    consistency_checks: list[str] = field(default_factory=list)
    priority_evidence: dict[str, list[str]] = field(default_factory=dict)
    risks: list[str] = field(default_factory=list)
    opportunities: list[str] = field(default_factory=list)
    scenarios: list[str] = field(default_factory=list)
    evidence_map: dict[str, list[str]] = field(default_factory=dict)
    chart_paths: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass
class ReportArtifact:
    title: str
    markdown: str
    sections: dict[str, str] = field(default_factory=dict)
    references: list[str] = field(default_factory=list)
    chart_paths: list[str] = field(default_factory=list)


@dataclass
class WorkflowConfig:
    project_name: str
    outdir: str
    memory_path: str
    trace_path: str
    literature_csv: str = ""
    theme_memory_path: str = ""
    compact_context_path: str = ""
    local_text_paths: list[str] = field(default_factory=list)
    structured_data_paths: list[str] = field(default_factory=list)
    local_pdf_paths: list[str] = field(default_factory=list)
    workflow: dict[str, Any] = field(default_factory=dict)
    llm: dict[str, Any] = field(default_factory=dict)
    connectors: dict[str, Any] = field(default_factory=dict)
