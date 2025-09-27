"""Workflow data models for Phase 4 orchestration engine."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import UUID, uuid4


class WorkflowStatus(str, Enum):
    """Workflow execution status."""
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepStatus(str, Enum):
    """Workflow step execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class StepType(str, Enum):
    """Types of workflow steps."""
    API_CALL = "api_call"
    DATABASE_OPERATION = "database_operation"
    DATA_TRANSFORMATION = "data_transformation"
    CONDITION = "condition"
    LOOP = "loop"
    PARALLEL = "parallel"
    WEBHOOK = "webhook"
    EMAIL_NOTIFICATION = "email_notification"
    CUSTOM_SCRIPT = "custom_script"
    DELAY = "delay"


class TriggerType(str, Enum):
    """Workflow trigger types."""
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    EVENT_DRIVEN = "event_driven"
    WEBHOOK = "webhook"
    DATA_CHANGE = "data_change"


@dataclass
class WorkflowTrigger:
    """Workflow trigger configuration."""
    
    trigger_id: str
    trigger_type: TriggerType
    config: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    
    def __post_init__(self):
        if self.trigger_type == TriggerType.SCHEDULED and "cron" not in self.config:
            self.config["cron"] = "0 0 * * *"  # Daily at midnight default


@dataclass
class WorkflowStep:
    """Individual workflow step definition."""
    
    step_id: str
    name: str
    step_type: StepType
    config: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)  # Step IDs this step depends on
    conditions: Dict[str, Any] = field(default_factory=dict)  # Conditional execution
    retry_config: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: int = 300  # 5 minutes default
    description: Optional[str] = None
    
    def __post_init__(self):
        if not self.retry_config:
            self.retry_config = {
                "max_attempts": 3,
                "backoff_multiplier": 2,
                "initial_delay_seconds": 5
            }


@dataclass
class StepExecution:
    """Individual step execution record."""
    
    execution_id: str
    step_id: str
    workflow_execution_id: str
    status: StepStatus = StepStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    attempt_count: int = 0
    input_data: Dict[str, Any] = field(default_factory=dict)
    output_data: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    logs: List[str] = field(default_factory=list)
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate execution duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
    
    def add_log(self, message: str) -> None:
        """Add log message with timestamp."""
        timestamp = datetime.now().isoformat()
        self.logs.append(f"[{timestamp}] {message}")


@dataclass
class Workflow:
    """Workflow definition."""
    
    workflow_id: str
    name: str
    description: str
    version: str = "1.0"
    steps: List[WorkflowStep] = field(default_factory=list)
    triggers: List[WorkflowTrigger] = field(default_factory=list)
    variables: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    created_by: Optional[str] = None
    tenant_id: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    is_template: bool = False
    
    def add_step(self, step: WorkflowStep) -> None:
        """Add a step to the workflow."""
        self.steps.append(step)
        self.updated_at = datetime.now()
    
    def get_step(self, step_id: str) -> Optional[WorkflowStep]:
        """Get a step by ID."""
        for step in self.steps:
            if step.step_id == step_id:
                return step
        return None
    
    def get_root_steps(self) -> List[WorkflowStep]:
        """Get steps with no dependencies (root steps)."""
        return [step for step in self.steps if not step.dependencies]
    
    def get_dependent_steps(self, step_id: str) -> List[WorkflowStep]:
        """Get steps that depend on the given step."""
        return [step for step in self.steps if step_id in step.dependencies]
    
    def validate_dependencies(self) -> List[str]:
        """Validate workflow dependencies and return any errors."""
        errors = []
        step_ids = {step.step_id for step in self.steps}
        
        for step in self.steps:
            for dep in step.dependencies:
                if dep not in step_ids:
                    errors.append(f"Step {step.step_id} depends on non-existent step {dep}")
        
        # Check for circular dependencies (simplified check)
        visited = set()
        rec_stack = set()
        
        def has_cycle(step_id: str) -> bool:
            if step_id in rec_stack:
                return True
            if step_id in visited:
                return False
            
            visited.add(step_id)
            rec_stack.add(step_id)
            
            step = self.get_step(step_id)
            if step:
                for dep in step.dependencies:
                    if has_cycle(dep):
                        return True
            
            rec_stack.remove(step_id)
            return False
        
        for step in self.steps:
            if step.step_id not in visited:
                if has_cycle(step.step_id):
                    errors.append(f"Circular dependency detected involving step {step.step_id}")
        
        return errors


@dataclass
class WorkflowExecution:
    """Workflow execution record."""
    
    execution_id: str
    workflow_id: str
    workflow_version: str
    status: WorkflowStatus = WorkflowStatus.DRAFT
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    triggered_by: Optional[str] = None
    trigger_type: Optional[TriggerType] = None
    input_data: Dict[str, Any] = field(default_factory=dict)
    output_data: Dict[str, Any] = field(default_factory=dict)
    step_executions: List[StepExecution] = field(default_factory=list)
    context_variables: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    tenant_id: Optional[str] = None
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate total execution duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
    
    def get_step_execution(self, step_id: str) -> Optional[StepExecution]:
        """Get step execution by step ID."""
        for step_exec in self.step_executions:
            if step_exec.step_id == step_id:
                return step_exec
        return None
    
    def get_completed_steps(self) -> List[StepExecution]:
        """Get all completed step executions."""
        return [step for step in self.step_executions if step.status == StepStatus.COMPLETED]
    
    def get_failed_steps(self) -> List[StepExecution]:
        """Get all failed step executions."""
        return [step for step in self.step_executions if step.status == StepStatus.FAILED]
    
    def calculate_progress_percentage(self) -> float:
        """Calculate execution progress as percentage."""
        if not self.step_executions:
            return 0.0
        
        completed_count = len([s for s in self.step_executions 
                              if s.status in [StepStatus.COMPLETED, StepStatus.SKIPPED]])
        return (completed_count / len(self.step_executions)) * 100


@dataclass
class WorkflowTemplate:
    """Reusable workflow template."""
    
    template_id: str
    name: str
    description: str
    category: str
    workflow_definition: Workflow
    parameters: Dict[str, Any] = field(default_factory=dict)  # Template parameters
    created_at: datetime = field(default_factory=datetime.now)
    created_by: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    usage_count: int = 0
    
    def instantiate(self, parameters: Dict[str, Any], 
                   workflow_name: str, tenant_id: Optional[str] = None) -> Workflow:
        """Create a workflow instance from this template."""
        # Create a copy of the workflow definition
        workflow = Workflow(
            workflow_id=str(uuid4()),
            name=workflow_name,
            description=self.workflow_definition.description,
            version="1.0",
            steps=self.workflow_definition.steps.copy(),
            variables={**self.workflow_definition.variables, **parameters},
            tenant_id=tenant_id,
            tags=self.tags.copy(),
            is_template=False
        )
        
        # Replace template parameters in step configurations
        for step in workflow.steps:
            step.config = self._replace_parameters(step.config, parameters)
        
        self.usage_count += 1
        return workflow
    
    def _replace_parameters(self, config: Dict[str, Any], 
                           parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Replace template parameters in configuration."""
        # Simple parameter replacement (in production, use a proper templating engine)
        config_str = json.dumps(config)
        for param_name, param_value in parameters.items():
            config_str = config_str.replace(f"${{{param_name}}}", str(param_value))
        return json.loads(config_str)


@dataclass
class IntegrationDefinition:
    """External system integration definition."""
    
    integration_id: str
    name: str
    integration_type: str  # rest_api, database, file_system, message_queue, etc.
    base_url: Optional[str] = None
    authentication: Dict[str, Any] = field(default_factory=dict)
    default_headers: Dict[str, str] = field(default_factory=dict)
    timeout_seconds: int = 30
    retry_config: Dict[str, Any] = field(default_factory=dict)
    rate_limit: Optional[Dict[str, Any]] = None
    description: Optional[str] = None
    
    def __post_init__(self):
        if not self.retry_config:
            self.retry_config = {
                "max_attempts": 3,
                "backoff_multiplier": 2,
                "initial_delay_seconds": 1
            }