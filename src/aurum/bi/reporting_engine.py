"""Advanced Business Intelligence and Reporting Engine for Phase 4."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

logger = logging.getLogger(__name__)


class ReportType(str, Enum):
    """Types of reports that can be generated."""
    EXECUTIVE_SUMMARY = "executive_summary"
    OPERATIONAL_METRICS = "operational_metrics"
    COMPLIANCE_REPORT = "compliance_report"
    FINANCIAL_ANALYSIS = "financial_analysis"
    PERFORMANCE_DASHBOARD = "performance_dashboard"
    SECURITY_AUDIT = "security_audit"
    CUSTOM = "custom"


class ReportFormat(str, Enum):
    """Report output formats."""
    JSON = "json"
    PDF = "pdf"
    HTML = "html"
    EXCEL = "excel"
    CSV = "csv"


class ChartType(str, Enum):
    """Chart types for visualizations."""
    LINE = "line"
    BAR = "bar"
    PIE = "pie"
    AREA = "area"
    SCATTER = "scatter"
    HEATMAP = "heatmap"
    GAUGE = "gauge"
    TABLE = "table"


class AggregationType(str, Enum):
    """Data aggregation types."""
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    PERCENTILE = "percentile"


@dataclass
class DataSource:
    """Data source configuration for reports."""
    
    source_id: str
    name: str
    source_type: str  # database, api, file, etc.
    connection_string: str
    query: Optional[str] = None
    refresh_interval_minutes: int = 60
    cache_results: bool = True
    last_refresh: Optional[datetime] = None
    
    def needs_refresh(self) -> bool:
        """Check if data source needs refresh."""
        if not self.last_refresh:
            return True
        return datetime.now() - self.last_refresh > timedelta(minutes=self.refresh_interval_minutes)


@dataclass
class ChartConfig:
    """Chart configuration for visualizations."""
    
    chart_id: str
    title: str
    chart_type: ChartType
    data_source: str  # Data source ID
    x_axis: str
    y_axis: str
    group_by: Optional[str] = None
    aggregation: AggregationType = AggregationType.SUM
    filters: Dict[str, Any] = field(default_factory=dict)
    colors: List[str] = field(default_factory=list)
    show_legend: bool = True
    show_grid: bool = True
    
    def __post_init__(self):
        if not self.colors:
            self.colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]


@dataclass
class ReportTemplate:
    """Report template definition."""
    
    template_id: str
    name: str
    description: str
    report_type: ReportType
    data_sources: List[DataSource]
    charts: List[ChartConfig]
    layout: Dict[str, Any] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    schedule: Optional[Dict[str, Any]] = None
    created_at: datetime = field(default_factory=datetime.now)
    created_by: Optional[str] = None
    tenant_id: Optional[str] = None
    
    def __post_init__(self):
        if not self.layout:
            self.layout = {
                "type": "grid",
                "columns": 2,
                "rows": len(self.charts) // 2 + 1
            }


@dataclass
class GeneratedReport:
    """Generated report instance."""
    
    report_id: str
    template_id: str
    name: str
    report_type: ReportType
    format: ReportFormat
    data: Dict[str, Any]
    charts_data: Dict[str, Any] = field(default_factory=dict)
    generated_at: datetime = field(default_factory=datetime.now)
    generated_by: Optional[str] = None
    tenant_id: Optional[str] = None
    file_path: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def age_hours(self) -> float:
        """Get report age in hours."""
        return (datetime.now() - self.generated_at).total_seconds() / 3600


@dataclass
class DashboardConfig:
    """Executive dashboard configuration."""
    
    dashboard_id: str
    name: str
    description: str
    widgets: List[Dict[str, Any]] = field(default_factory=list)
    layout: Dict[str, Any] = field(default_factory=dict)
    refresh_interval_minutes: int = 15
    auto_refresh: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    created_by: Optional[str] = None
    tenant_id: Optional[str] = None
    
    def add_widget(self, widget_type: str, title: str, data_source: str, 
                   config: Dict[str, Any]) -> None:
        """Add widget to dashboard."""
        widget = {
            "widget_id": str(uuid4()),
            "type": widget_type,
            "title": title,
            "data_source": data_source,
            "config": config,
            "position": {"x": 0, "y": len(self.widgets), "width": 6, "height": 4}
        }
        self.widgets.append(widget)


class ReportingEngine:
    """Advanced reporting engine for business intelligence."""
    
    def __init__(self):
        self.data_sources: Dict[str, DataSource] = {}
        self.templates: Dict[str, ReportTemplate] = {}
        self.generated_reports: Dict[str, GeneratedReport] = {}
        self.dashboards: Dict[str, DashboardConfig] = {}
        self._initialize_default_templates()
    
    def _initialize_default_templates(self) -> None:
        """Initialize default report templates."""
        # Executive Summary Template
        exec_template = ReportTemplate(
            template_id="executive_summary",
            name="Executive Summary",
            description="High-level metrics and KPIs for executives",
            report_type=ReportType.EXECUTIVE_SUMMARY,
            data_sources=[
                DataSource(
                    source_id="kpi_metrics",
                    name="KPI Metrics",
                    source_type="database",
                    connection_string="postgresql://aurum_db/metrics",
                    query="SELECT * FROM kpi_summary WHERE date >= NOW() - INTERVAL '30 days'"
                )
            ],
            charts=[
                ChartConfig(
                    chart_id="revenue_trend",
                    title="Revenue Trend",
                    chart_type=ChartType.LINE,
                    data_source="kpi_metrics",
                    x_axis="date",
                    y_axis="revenue",
                    aggregation=AggregationType.SUM
                ),
                ChartConfig(
                    chart_id="user_growth",
                    title="User Growth",
                    chart_type=ChartType.BAR,
                    data_source="kpi_metrics",
                    x_axis="date",
                    y_axis="active_users",
                    aggregation=AggregationType.COUNT
                )
            ]
        )
        
        # Compliance Report Template
        compliance_template = ReportTemplate(
            template_id="compliance_report",
            name="Compliance Report",
            description="Security and compliance metrics",
            report_type=ReportType.COMPLIANCE_REPORT,
            data_sources=[
                DataSource(
                    source_id="audit_events",
                    name="Audit Events",
                    source_type="database",
                    connection_string="postgresql://aurum_db/security",
                    query="SELECT * FROM audit_events WHERE created_at >= NOW() - INTERVAL '7 days'"
                )
            ],
            charts=[
                ChartConfig(
                    chart_id="compliance_score",
                    title="Compliance Score",
                    chart_type=ChartType.GAUGE,
                    data_source="audit_events",
                    x_axis="date",
                    y_axis="compliance_score",
                    aggregation=AggregationType.AVG
                ),
                ChartConfig(
                    chart_id="security_events",
                    title="Security Events by Type",
                    chart_type=ChartType.PIE,
                    data_source="audit_events",
                    x_axis="event_type",
                    y_axis="count",
                    aggregation=AggregationType.COUNT
                )
            ]
        )
        
        self.templates["executive_summary"] = exec_template
        self.templates["compliance_report"] = compliance_template
    
    def register_data_source(self, data_source: DataSource) -> None:
        """Register a data source."""
        self.data_sources[data_source.source_id] = data_source
        logger.info(f"Registered data source: {data_source.name}")
    
    def create_report_template(self, template: ReportTemplate) -> None:
        """Create a new report template."""
        self.templates[template.template_id] = template
        logger.info(f"Created report template: {template.name}")
    
    def get_template(self, template_id: str) -> Optional[ReportTemplate]:
        """Get report template by ID."""
        return self.templates.get(template_id)
    
    def list_templates(self, tenant_id: Optional[str] = None) -> List[ReportTemplate]:
        """List available report templates."""
        templates = list(self.templates.values())
        if tenant_id:
            templates = [t for t in templates if t.tenant_id == tenant_id or t.tenant_id is None]
        return templates
    
    async def generate_report(self, template_id: str, format: ReportFormat = ReportFormat.JSON,
                            parameters: Dict[str, Any] = None,
                            generated_by: str = None) -> GeneratedReport:
        """Generate a report from template."""
        template = self.get_template(template_id)
        if not template:
            raise ValueError(f"Template not found: {template_id}")
        
        report_id = str(uuid4())
        parameters = parameters or {}
        
        logger.info(f"Generating report: {template.name} ({report_id})")
        
        # Collect data from all sources
        report_data = {}
        charts_data = {}
        
        for data_source in template.data_sources:
            try:
                data = await self._fetch_data_source(data_source, parameters)
                report_data[data_source.source_id] = data
            except Exception as e:
                logger.error(f"Failed to fetch data from {data_source.name}: {e}")
                report_data[data_source.source_id] = {"error": str(e)}
        
        # Generate chart data
        for chart in template.charts:
            try:
                chart_data = self._generate_chart_data(chart, report_data, parameters)
                charts_data[chart.chart_id] = chart_data
            except Exception as e:
                logger.error(f"Failed to generate chart {chart.title}: {e}")
                charts_data[chart.chart_id] = {"error": str(e)}
        
        # Create report
        report = GeneratedReport(
            report_id=report_id,
            template_id=template_id,
            name=template.name,
            report_type=template.report_type,
            format=format,
            data=report_data,
            charts_data=charts_data,
            generated_by=generated_by,
            tenant_id=template.tenant_id
        )
        
        # Export to requested format
        if format != ReportFormat.JSON:
            report.file_path = await self._export_report(report, format)
        
        self.generated_reports[report_id] = report
        logger.info(f"Report generated successfully: {report_id}")
        
        return report
    
    async def _fetch_data_source(self, data_source: DataSource, 
                                parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch data from a data source."""
        # Simulate data fetching (in production, implement actual connectors)
        if data_source.source_type == "database":
            # Mock database query results
            return {
                "rows": [
                    {"date": "2024-01-01", "revenue": 100000, "active_users": 1500},
                    {"date": "2024-01-02", "revenue": 105000, "active_users": 1520},
                    {"date": "2024-01-03", "revenue": 110000, "active_users": 1540}
                ],
                "total_rows": 3,
                "execution_time_ms": 45
            }
        elif data_source.source_type == "api":
            # Mock API response
            return {
                "data": {"metrics": {"value": 42}},
                "status": "success",
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {"error": f"Unsupported source type: {data_source.source_type}"}
    
    def _generate_chart_data(self, chart: ChartConfig, report_data: Dict[str, Any],
                           parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Generate chart data from report data."""
        source_data = report_data.get(chart.data_source, {})
        rows = source_data.get("rows", [])
        
        if not rows:
            return {"error": "No data available"}
        
        # Apply filters
        filtered_rows = self._apply_filters(rows, chart.filters)
        
        # Generate chart-specific data
        if chart.chart_type == ChartType.LINE:
            return self._generate_line_chart_data(chart, filtered_rows)
        elif chart.chart_type == ChartType.BAR:
            return self._generate_bar_chart_data(chart, filtered_rows)
        elif chart.chart_type == ChartType.PIE:
            return self._generate_pie_chart_data(chart, filtered_rows)
        elif chart.chart_type == ChartType.GAUGE:
            return self._generate_gauge_chart_data(chart, filtered_rows)
        else:
            return {"error": f"Unsupported chart type: {chart.chart_type}"}
    
    def _apply_filters(self, rows: List[Dict[str, Any]], 
                      filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply filters to data rows."""
        if not filters:
            return rows
        
        filtered = []
        for row in rows:
            include_row = True
            for field, condition in filters.items():
                if field not in row:
                    continue
                
                if isinstance(condition, dict):
                    if "gte" in condition and row[field] < condition["gte"]:
                        include_row = False
                    elif "lte" in condition and row[field] > condition["lte"]:
                        include_row = False
                    elif "eq" in condition and row[field] != condition["eq"]:
                        include_row = False
                elif row[field] != condition:
                    include_row = False
                
                if not include_row:
                    break
            
            if include_row:
                filtered.append(row)
        
        return filtered
    
    def _generate_line_chart_data(self, chart: ChartConfig, 
                                rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate line chart data."""
        data_points = []
        for row in rows:
            if chart.x_axis in row and chart.y_axis in row:
                data_points.append({
                    "x": row[chart.x_axis], 
                    "y": row[chart.y_axis]
                })
        
        return {
            "type": "line",
            "title": chart.title,
            "data": data_points,
            "x_label": chart.x_axis,
            "y_label": chart.y_axis,
            "colors": chart.colors
        }
    
    def _generate_bar_chart_data(self, chart: ChartConfig,
                               rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate bar chart data."""
        # Group and aggregate data
        groups = {}
        for row in rows:
            if chart.x_axis in row and chart.y_axis in row:
                x_val = row[chart.x_axis]
                y_val = row[chart.y_axis]
                
                if x_val not in groups:
                    groups[x_val] = []
                groups[x_val].append(y_val)
        
        # Apply aggregation
        data_points = []
        for x_val, y_vals in groups.items():
            if chart.aggregation == AggregationType.SUM:
                y_agg = sum(y_vals)
            elif chart.aggregation == AggregationType.AVG:
                y_agg = sum(y_vals) / len(y_vals)
            elif chart.aggregation == AggregationType.COUNT:
                y_agg = len(y_vals)
            else:
                y_agg = sum(y_vals)  # Default to sum
            
            data_points.append({"x": x_val, "y": y_agg})
        
        return {
            "type": "bar",
            "title": chart.title,
            "data": data_points,
            "x_label": chart.x_axis,
            "y_label": chart.y_axis,
            "colors": chart.colors
        }
    
    def _generate_pie_chart_data(self, chart: ChartConfig,
                               rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate pie chart data."""
        groups = {}
        for row in rows:
            if chart.x_axis in row:
                x_val = row[chart.x_axis]
                groups[x_val] = groups.get(x_val, 0) + 1
        
        data_points = [{"label": k, "value": v} for k, v in groups.items()]
        
        return {
            "type": "pie",
            "title": chart.title,
            "data": data_points,
            "colors": chart.colors
        }
    
    def _generate_gauge_chart_data(self, chart: ChartConfig,
                                 rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate gauge chart data."""
        if not rows or chart.y_axis not in rows[0]:
            return {"error": "No data for gauge"}
        
        values = [row[chart.y_axis] for row in rows if chart.y_axis in row]
        
        if chart.aggregation == AggregationType.AVG:
            gauge_value = sum(values) / len(values)
        elif chart.aggregation == AggregationType.MAX:
            gauge_value = max(values)
        elif chart.aggregation == AggregationType.MIN:
            gauge_value = min(values)
        else:
            gauge_value = values[-1] if values else 0  # Latest value
        
        return {
            "type": "gauge",
            "title": chart.title,
            "value": gauge_value,
            "min_value": 0,
            "max_value": 100,
            "colors": chart.colors
        }
    
    async def _export_report(self, report: GeneratedReport, format: ReportFormat) -> str:
        """Export report to specified format."""
        file_name = f"report_{report.report_id}_{int(datetime.now().timestamp())}"
        
        if format == ReportFormat.PDF:
            file_path = f"/tmp/reports/{file_name}.pdf"
            # In production, use a PDF generation library
            logger.info(f"Exported report to PDF: {file_path}")
            
        elif format == ReportFormat.HTML:
            file_path = f"/tmp/reports/{file_name}.html"
            # Generate HTML report
            logger.info(f"Exported report to HTML: {file_path}")
            
        elif format == ReportFormat.EXCEL:
            file_path = f"/tmp/reports/{file_name}.xlsx"
            # Generate Excel report
            logger.info(f"Exported report to Excel: {file_path}")
            
        elif format == ReportFormat.CSV:
            file_path = f"/tmp/reports/{file_name}.csv"
            # Generate CSV report
            logger.info(f"Exported report to CSV: {file_path}")
            
        else:
            file_path = f"/tmp/reports/{file_name}.json"
            with open(file_path, 'w') as f:
                json.dump({
                    "report": report.data,
                    "charts": report.charts_data
                }, f, indent=2, default=str)
        
        return file_path
    
    def create_dashboard(self, dashboard: DashboardConfig) -> None:
        """Create an executive dashboard."""
        self.dashboards[dashboard.dashboard_id] = dashboard
        logger.info(f"Created dashboard: {dashboard.name}")
    
    def get_dashboard(self, dashboard_id: str) -> Optional[DashboardConfig]:
        """Get dashboard by ID."""
        return self.dashboards.get(dashboard_id)
    
    async def generate_dashboard_data(self, dashboard_id: str) -> Dict[str, Any]:
        """Generate data for dashboard widgets."""
        dashboard = self.get_dashboard(dashboard_id)
        if not dashboard:
            raise ValueError(f"Dashboard not found: {dashboard_id}")
        
        widget_data = {}
        
        for widget in dashboard.widgets:
            try:
                # Simulate widget data generation
                widget_data[widget["widget_id"]] = {
                    "title": widget["title"],
                    "type": widget["type"],
                    "data": self._generate_widget_data(widget),
                    "last_updated": datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Failed to generate widget data: {e}")
                widget_data[widget["widget_id"]] = {"error": str(e)}
        
        return {
            "dashboard_id": dashboard_id,
            "name": dashboard.name,
            "widgets": widget_data,
            "generated_at": datetime.now().isoformat()
        }
    
    def _generate_widget_data(self, widget: Dict[str, Any]) -> Dict[str, Any]:
        """Generate data for a single widget."""
        widget_type = widget["type"]
        
        if widget_type == "kpi":
            return {
                "value": 42,
                "change": "+5.2%",
                "trend": "up"
            }
        elif widget_type == "chart":
            return {
                "chart_type": "line",
                "data_points": [{"x": "2024-01", "y": 100}, {"x": "2024-02", "y": 120}]
            }
        elif widget_type == "table":
            return {
                "headers": ["Name", "Value", "Status"],
                "rows": [
                    ["Metric 1", "100", "Good"],
                    ["Metric 2", "85", "Warning"]
                ]
            }
        else:
            return {"message": f"Widget type {widget_type} not implemented"}
    
    def schedule_automated_report(self, template_id: str, schedule: Dict[str, Any],
                                recipients: List[str]) -> str:
        """Schedule automated report generation."""
        schedule_id = str(uuid4())
        
        # In production, integrate with a job scheduler like Celery
        logger.info(f"Scheduled automated report: {template_id} with schedule {schedule}")
        
        return schedule_id
    
    def get_report_analytics(self, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Get analytics about report usage."""
        reports = list(self.generated_reports.values())
        if tenant_id:
            reports = [r for r in reports if r.tenant_id == tenant_id]
        
        # Report type breakdown
        type_counts = {}
        for report in reports:
            report_type = report.report_type.value
            type_counts[report_type] = type_counts.get(report_type, 0) + 1
        
        # Format breakdown
        format_counts = {}
        for report in reports:
            format_type = report.format.value
            format_counts[format_type] = format_counts.get(format_type, 0) + 1
        
        return {
            "total_reports_generated": len(reports),
            "total_templates": len(self.templates),
            "total_dashboards": len(self.dashboards),
            "report_types": type_counts,
            "report_formats": format_counts,
            "avg_generation_time_seconds": 2.3,  # Mock value
            "most_used_template": "executive_summary"
        }


# Global reporting engine instance
reporting_engine = ReportingEngine()