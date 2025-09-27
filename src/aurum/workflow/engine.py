"""Workflow orchestration engine for Phase 4 advanced automation."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set
from uuid import uuid4

from .models import (
    Workflow, WorkflowExecution, WorkflowStep, StepExecution,
    WorkflowStatus, StepStatus, StepType, TriggerType
)

logger = logging.getLogger(__name__)


class StepExecutor:
    """Executes individual workflow steps."""
    
    def __init__(self):
        self.step_handlers: Dict[StepType, Callable] = {
            StepType.API_CALL: self._execute_api_call,
            StepType.DATABASE_OPERATION: self._execute_database_operation,
            StepType.DATA_TRANSFORMATION: self._execute_data_transformation,
            StepType.CONDITION: self._execute_condition,
            StepType.WEBHOOK: self._execute_webhook,
            StepType.EMAIL_NOTIFICATION: self._execute_email_notification,
            StepType.CUSTOM_SCRIPT: self._execute_custom_script,
            StepType.DELAY: self._execute_delay,
        }
    
    async def execute_step(self, step: WorkflowStep, step_execution: StepExecution,
                          context: Dict[str, Any]) -> None:
        """Execute a single workflow step."""
        step_execution.status = StepStatus.RUNNING
        step_execution.started_at = datetime.now()
        step_execution.add_log(f"Starting execution of step: {step.name}")
        
        try:
            handler = self.step_handlers.get(step.step_type)
            if not handler:
                raise ValueError(f"No handler for step type: {step.step_type}")
            
            # Execute with timeout
            await asyncio.wait_for(
                handler(step, step_execution, context),
                timeout=step.timeout_seconds
            )
            
            step_execution.status = StepStatus.COMPLETED
            step_execution.completed_at = datetime.now()
            step_execution.add_log(f"Step completed successfully")
            
        except asyncio.TimeoutError:
            step_execution.status = StepStatus.FAILED
            step_execution.error_message = f"Step timed out after {step.timeout_seconds} seconds"
            step_execution.add_log(f"Step timed out")
            
        except Exception as e:
            step_execution.status = StepStatus.FAILED
            step_execution.error_message = str(e)
            step_execution.add_log(f"Step failed: {str(e)}")
            logger.error(f"Step {step.step_id} failed: {e}")
        
        finally:
            if not step_execution.completed_at:
                step_execution.completed_at = datetime.now()
    
    async def _execute_api_call(self, step: WorkflowStep, step_execution: StepExecution,
                               context: Dict[str, Any]) -> None:
        """Execute API call step."""
        config = step.config
        url = config.get("url", "")
        method = config.get("method", "GET").upper()
        headers = config.get("headers", {})
        payload = config.get("payload", {})
        
        # Replace variables in URL and payload
        url = self._replace_variables(url, context)
        payload = self._replace_variables_in_dict(payload, context)
        
        step_execution.add_log(f"Making {method} request to {url}")
        
        # Simulate API call (in production, use aiohttp or similar)
        await asyncio.sleep(0.1)  # Simulate network delay
        
        # Mock response
        response_data = {
            "status_code": 200,
            "data": {"result": "success", "timestamp": datetime.now().isoformat()}
        }
        
        step_execution.output_data = response_data
        step_execution.add_log(f"API call completed with status {response_data['status_code']}")
    
    async def _execute_database_operation(self, step: WorkflowStep, step_execution: StepExecution,
                                        context: Dict[str, Any]) -> None:
        """Execute database operation step."""
        config = step.config
        operation = config.get("operation", "select")
        query = config.get("query", "")
        parameters = config.get("parameters", {})
        
        # Replace variables in query and parameters
        query = self._replace_variables(query, context)
        parameters = self._replace_variables_in_dict(parameters, context)
        
        step_execution.add_log(f"Executing {operation} query")
        
        # Simulate database operation
        await asyncio.sleep(0.05)
        
        # Mock result
        result = {
            "rows_affected": 1 if operation in ["insert", "update", "delete"] else 0,
            "data": [{"id": 1, "name": "sample"}, {"id": 2, "name": "example"}] if operation == "select" else []
        }
        
        step_execution.output_data = result
        step_execution.add_log(f"Database operation completed")
    
    async def _execute_data_transformation(self, step: WorkflowStep, step_execution: StepExecution,
                                         context: Dict[str, Any]) -> None:
        """Execute data transformation step."""
        config = step.config
        input_data = step_execution.input_data
        transformation_type = config.get("transformation_type", "filter")
        
        step_execution.add_log(f"Applying {transformation_type} transformation")
        
        # Mock transformation
        if transformation_type == "filter":
            # Filter data based on criteria
            criteria = config.get("criteria", {})
            transformed_data = input_data  # Simplified
        elif transformation_type == "map":
            # Map/transform fields
            field_mapping = config.get("field_mapping", {})
            transformed_data = input_data  # Simplified
        else:
            transformed_data = input_data
        
        step_execution.output_data = {"transformed_data": transformed_data}
        step_execution.add_log("Data transformation completed")
    
    async def _execute_condition(self, step: WorkflowStep, step_execution: StepExecution,
                                context: Dict[str, Any]) -> None:
        """Execute conditional step."""
        config = step.config
        condition = config.get("condition", "")
        
        # Simple condition evaluation (in production, use a proper expression evaluator)
        condition_result = self._evaluate_condition(condition, context)
        
        step_execution.output_data = {"condition_result": condition_result}
        step_execution.add_log(f"Condition '{condition}' evaluated to {condition_result}")
    
    async def _execute_webhook(self, step: WorkflowStep, step_execution: StepExecution,
                              context: Dict[str, Any]) -> None:
        """Execute webhook step."""
        config = step.config
        url = config.get("url", "")
        payload = config.get("payload", {})
        
        url = self._replace_variables(url, context)
        payload = self._replace_variables_in_dict(payload, context)
        
        step_execution.add_log(f"Sending webhook to {url}")
        
        # Simulate webhook call
        await asyncio.sleep(0.1)
        
        step_execution.output_data = {"webhook_sent": True, "timestamp": datetime.now().isoformat()}
        step_execution.add_log("Webhook sent successfully")
    
    async def _execute_email_notification(self, step: WorkflowStep, step_execution: StepExecution,
                                        context: Dict[str, Any]) -> None:
        """Execute email notification step."""
        config = step.config
        recipients = config.get("recipients", [])
        subject = config.get("subject", "Workflow Notification")
        body = config.get("body", "")
        
        # Replace variables
        subject = self._replace_variables(subject, context)
        body = self._replace_variables(body, context)
        
        step_execution.add_log(f"Sending email to {len(recipients)} recipients")
        
        # Simulate email sending
        await asyncio.sleep(0.2)
        
        step_execution.output_data = {
            "email_sent": True,
            "recipients_count": len(recipients),
            "timestamp": datetime.now().isoformat()
        }
        step_execution.add_log("Email notification sent successfully")
    
    async def _execute_custom_script(self, step: WorkflowStep, step_execution: StepExecution,
                                   context: Dict[str, Any]) -> None:
        """Execute custom script step."""
        config = step.config
        script_content = config.get("script", "")
        script_type = config.get("script_type", "python")
        
        step_execution.add_log(f"Executing {script_type} script")
        
        # In production, this would execute the script in a sandboxed environment
        # For now, just simulate execution
        await asyncio.sleep(0.5)
        
        step_execution.output_data = {
            "script_executed": True,
            "script_type": script_type,
            "execution_time": datetime.now().isoformat()
        }
        step_execution.add_log("Custom script executed successfully")
    
    async def _execute_delay(self, step: WorkflowStep, step_execution: StepExecution,
                           context: Dict[str, Any]) -> None:
        """Execute delay step."""
        config = step.config
        delay_seconds = config.get("delay_seconds", 5)
        
        step_execution.add_log(f"Waiting for {delay_seconds} seconds")
        await asyncio.sleep(delay_seconds)
        
        step_execution.output_data = {"delay_completed": True, "delay_seconds": delay_seconds}
        step_execution.add_log("Delay completed")
    
    def _replace_variables(self, text: str, context: Dict[str, Any]) -> str:
        """Replace variables in text with context values."""
        for key, value in context.items():
            text = text.replace(f"${{{key}}}", str(value))
        return text
    
    def _replace_variables_in_dict(self, data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Replace variables in dictionary values."""
        if isinstance(data, dict):
            return {k: self._replace_variables_in_dict(v, context) for k, v in data.items()}
        elif isinstance(data, str):
            return self._replace_variables(data, context)
        else:
            return data
    
    def _evaluate_condition(self, condition: str, context: Dict[str, Any]) -> bool:
        """Evaluate a simple condition (simplified implementation)."""
        # In production, use a proper expression evaluator
        # For now, handle basic conditions like "variable == value"
        try:
            condition = self._replace_variables(condition, context)
            return eval(condition)  # WARNING: In production, use a safe evaluator
        except:
            return False


class WorkflowExecutor:
    """Executes complete workflows."""
    
    def __init__(self):
        self.step_executor = StepExecutor()
        self.active_executions: Dict[str, WorkflowExecution] = {}
    
    async def execute_workflow(self, workflow: Workflow, 
                             input_data: Dict[str, Any] = None,
                             triggered_by: str = None,
                             trigger_type: TriggerType = TriggerType.MANUAL) -> WorkflowExecution:
        """Execute a workflow."""
        execution_id = str(uuid4())
        
        # Create workflow execution record
        execution = WorkflowExecution(
            execution_id=execution_id,
            workflow_id=workflow.workflow_id,
            workflow_version=workflow.version,
            status=WorkflowStatus.ACTIVE,
            started_at=datetime.now(),
            triggered_by=triggered_by,
            trigger_type=trigger_type,
            input_data=input_data or {},
            tenant_id=workflow.tenant_id
        )
        
        # Initialize context with workflow variables and input data
        execution.context_variables = {
            **workflow.variables,
            **execution.input_data,
            "workflow_id": workflow.workflow_id,
            "execution_id": execution_id
        }
        
        # Create step executions
        for step in workflow.steps:
            step_execution = StepExecution(
                execution_id=str(uuid4()),
                step_id=step.step_id,
                workflow_execution_id=execution_id,
                input_data={}
            )
            execution.step_executions.append(step_execution)
        
        self.active_executions[execution_id] = execution
        
        try:
            await self._execute_workflow_steps(workflow, execution)
            execution.status = WorkflowStatus.COMPLETED
            execution.completed_at = datetime.now()
            logger.info(f"Workflow {workflow.workflow_id} completed successfully")
            
        except Exception as e:
            execution.status = WorkflowStatus.FAILED
            execution.error_message = str(e)
            execution.completed_at = datetime.now()
            logger.error(f"Workflow {workflow.workflow_id} failed: {e}")
        
        finally:
            if execution_id in self.active_executions:
                del self.active_executions[execution_id]
        
        return execution
    
    async def _execute_workflow_steps(self, workflow: Workflow, execution: WorkflowExecution) -> None:
        """Execute workflow steps in dependency order."""
        completed_steps: Set[str] = set()
        
        while len(completed_steps) < len(workflow.steps):
            # Find steps that can be executed (all dependencies completed)
            ready_steps = []
            for step in workflow.steps:
                if (step.step_id not in completed_steps and 
                    all(dep in completed_steps for dep in step.dependencies)):
                    ready_steps.append(step)
            
            if not ready_steps:
                # Check if we're stuck due to failed dependencies
                remaining_steps = [s for s in workflow.steps if s.step_id not in completed_steps]
                failed_steps = [s for s in execution.get_failed_steps()]
                
                if failed_steps:
                    raise Exception(f"Workflow blocked by failed steps: {[s.step_id for s in failed_steps]}")
                else:
                    raise Exception("Workflow deadlock detected - no steps can be executed")
            
            # Execute ready steps (can be done in parallel)
            execution_tasks = []
            for step in ready_steps:
                step_execution = execution.get_step_execution(step.step_id)
                if step_execution:
                    # Check conditions before execution
                    if self._should_execute_step(step, execution.context_variables):
                        task = self._execute_step_with_retry(step, step_execution, execution.context_variables)
                        execution_tasks.append(task)
                    else:
                        step_execution.status = StepStatus.SKIPPED
                        step_execution.add_log("Step skipped due to conditions")
                    
                    completed_steps.add(step.step_id)
            
            # Wait for all tasks to complete
            if execution_tasks:
                await asyncio.gather(*execution_tasks, return_exceptions=True)
            
            # Update context with step outputs
            self._update_context_from_outputs(execution)
    
    def _should_execute_step(self, step: WorkflowStep, context: Dict[str, Any]) -> bool:
        """Check if step should be executed based on conditions."""
        conditions = step.conditions
        if not conditions:
            return True
        
        # Simple condition checking
        if "when" in conditions:
            condition = conditions["when"]
            try:
                return self.step_executor._evaluate_condition(condition, context)
            except:
                return True  # Default to execute if condition evaluation fails
        
        return True
    
    async def _execute_step_with_retry(self, step: WorkflowStep, step_execution: StepExecution,
                                     context: Dict[str, Any]) -> None:
        """Execute step with retry logic."""
        retry_config = step.retry_config
        max_attempts = retry_config.get("max_attempts", 1)
        backoff_multiplier = retry_config.get("backoff_multiplier", 2)
        initial_delay = retry_config.get("initial_delay_seconds", 5)
        
        for attempt in range(max_attempts):
            step_execution.attempt_count = attempt + 1
            
            if attempt > 0:
                delay = initial_delay * (backoff_multiplier ** (attempt - 1))
                step_execution.add_log(f"Retrying in {delay} seconds (attempt {attempt + 1}/{max_attempts})")
                await asyncio.sleep(delay)
                step_execution.status = StepStatus.RETRYING
            
            await self.step_executor.execute_step(step, step_execution, context)
            
            if step_execution.status == StepStatus.COMPLETED:
                break
            elif attempt == max_attempts - 1:
                step_execution.add_log(f"Step failed after {max_attempts} attempts")
    
    def _update_context_from_outputs(self, execution: WorkflowExecution) -> None:
        """Update execution context with step outputs."""
        for step_exec in execution.step_executions:
            if step_exec.status == StepStatus.COMPLETED and step_exec.output_data:
                # Add step outputs to context with step_id prefix
                for key, value in step_exec.output_data.items():
                    execution.context_variables[f"{step_exec.step_id}.{key}"] = value


class WorkflowEngine:
    """Main workflow orchestration engine."""
    
    def __init__(self):
        self.workflows: Dict[str, Workflow] = {}
        self.executions: Dict[str, WorkflowExecution] = {}
        self.executor = WorkflowExecutor()
        self.scheduler_running = False
    
    def register_workflow(self, workflow: Workflow) -> None:
        """Register a workflow definition."""
        validation_errors = workflow.validate_dependencies()
        if validation_errors:
            raise ValueError(f"Workflow validation failed: {validation_errors}")
        
        self.workflows[workflow.workflow_id] = workflow
        logger.info(f"Registered workflow: {workflow.name} ({workflow.workflow_id})")
    
    def get_workflow(self, workflow_id: str) -> Optional[Workflow]:
        """Get workflow by ID."""
        return self.workflows.get(workflow_id)
    
    async def execute_workflow(self, workflow_id: str, 
                             input_data: Dict[str, Any] = None,
                             triggered_by: str = None) -> WorkflowExecution:
        """Execute a workflow by ID."""
        workflow = self.get_workflow(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow not found: {workflow_id}")
        
        execution = await self.executor.execute_workflow(
            workflow, input_data, triggered_by, TriggerType.MANUAL
        )
        
        self.executions[execution.execution_id] = execution
        return execution
    
    def get_execution(self, execution_id: str) -> Optional[WorkflowExecution]:
        """Get workflow execution by ID."""
        return self.executions.get(execution_id)
    
    def get_workflow_executions(self, workflow_id: str) -> List[WorkflowExecution]:
        """Get all executions for a workflow."""
        return [exec for exec in self.executions.values() if exec.workflow_id == workflow_id]
    
    def get_active_executions(self) -> List[WorkflowExecution]:
        """Get all active workflow executions."""
        return [exec for exec in self.executions.values() if exec.status == WorkflowStatus.ACTIVE]
    
    async def cancel_execution(self, execution_id: str) -> bool:
        """Cancel a running workflow execution."""
        execution = self.get_execution(execution_id)
        if not execution or execution.status != WorkflowStatus.ACTIVE:
            return False
        
        execution.status = WorkflowStatus.CANCELLED
        execution.completed_at = datetime.now()
        logger.info(f"Cancelled workflow execution: {execution_id}")
        return True


# Global workflow engine instance
workflow_engine = WorkflowEngine()