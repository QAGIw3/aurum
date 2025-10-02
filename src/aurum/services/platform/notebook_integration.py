"""Notebook Integration Service.

This service handles Jupyter notebook integration, kernel management,
code execution, and output handling for developer workspaces.

Extracted from the monolithic developer_workspace_service.py as part of the 
service layer decomposition initiative.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from uuid import uuid4
from pathlib import Path

from pydantic import BaseModel, Field

from src.aurum.services.base import BaseService
from src.aurum.data.repositories.base import BaseRepository


class NotebookTemplate(BaseModel):
    """Notebook template for common use cases."""
    
    template_id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: str
    category: str  # "data_exploration", "ml_training", "api_testing", etc.
    notebook_content: Dict[str, Any]  # Jupyter notebook JSON format
    required_packages: List[str] = Field(default_factory=list)
    required_environment_variables: List[str] = Field(default_factory=list)
    example_data: Optional[Dict[str, Any]] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CodeSnippet(BaseModel):
    """Reusable code snippet for common operations."""
    
    snippet_id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    description: str
    language: str = "python"
    code: str
    category: str
    imports: List[str] = Field(default_factory=list)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    example_usage: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str = "system"


class NotebookExecution(BaseModel):
    """Represents a notebook execution result."""
    
    execution_id: str = Field(default_factory=lambda: str(uuid4()))
    session_id: str
    notebook_path: str
    cells_executed: int = 0
    cells_total: int = 0
    status: str = "pending"  # pending, running, completed, failed
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    outputs: List[Dict[str, Any]] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class NotebookSnapshot(BaseModel):
    """Snapshot of notebook state."""
    
    snapshot_id: str = Field(default_factory=lambda: str(uuid4()))
    session_id: str
    notebook_path: str
    content: Dict[str, Any]  # Jupyter notebook JSON
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class NotebookRepository(BaseRepository):
    """Repository interface for notebook operations."""
    
    async def save_template(self, template: NotebookTemplate) -> NotebookTemplate:
        """Save or update a notebook template."""
        raise NotImplementedError
    
    async def get_template(self, template_id: str) -> Optional[NotebookTemplate]:
        """Get a template by ID."""
        raise NotImplementedError
    
    async def list_templates(
        self,
        category: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> List[NotebookTemplate]:
        """List templates with optional filters."""
        raise NotImplementedError
    
    async def save_snippet(self, snippet: CodeSnippet) -> CodeSnippet:
        """Save or update a code snippet."""
        raise NotImplementedError
    
    async def list_snippets(
        self,
        category: Optional[str] = None,
        language: Optional[str] = None
    ) -> List[CodeSnippet]:
        """List code snippets."""
        raise NotImplementedError
    
    async def save_execution(self, execution: NotebookExecution) -> NotebookExecution:
        """Save notebook execution record."""
        raise NotImplementedError
    
    async def save_snapshot(self, snapshot: NotebookSnapshot) -> NotebookSnapshot:
        """Save notebook snapshot."""
        raise NotImplementedError
    
    async def list_snapshots(
        self,
        session_id: str,
        limit: int = 10
    ) -> List[NotebookSnapshot]:
        """List snapshots for a session."""
        raise NotImplementedError


class NotebookIntegrationService(BaseService):
    """
    Jupyter notebook integration service.
    
    This service handles notebook templates, code execution, kernel management,
    and integration with the Jupyter ecosystem.
    """
    
    def __init__(
        self,
        repository: Optional[NotebookRepository] = None,
        jupyter_client: Optional[Any] = None,  # Interface to Jupyter server
        cache_enabled: bool = True,
        cache_ttl: int = 600  # 10 minutes for templates
    ):
        """
        Initialize the notebook integration service.
        
        Args:
            repository: Repository for data persistence
            jupyter_client: Client to interact with Jupyter servers
            cache_enabled: Enable caching for read operations
            cache_ttl: Cache time-to-live in seconds
        """
        super().__init__(cache_enabled=cache_enabled, cache_ttl=cache_ttl)
        self.repository = repository or self._get_default_repository()
        self.jupyter_client = jupyter_client  # In real impl, inject from DI
        self.logger = logging.getLogger(__name__)
        
        # Template and snippet caches
        self._templates: Dict[str, NotebookTemplate] = {}
        self._snippets: Dict[str, CodeSnippet] = {}
        
        # Active executions
        self._active_executions: Dict[str, NotebookExecution] = {}
        
        # Initialize default templates
        self._initialize_default_templates()
        self._initialize_default_snippets()
    
    def _get_default_repository(self) -> NotebookRepository:
        """Get default repository from DI container."""
        class MockRepository(NotebookRepository):
            def __init__(self):
                self.templates = {}
                self.snippets = {}
                self.executions = {}
                self.snapshots = {}
            
            async def save_template(self, template: NotebookTemplate) -> NotebookTemplate:
                self.templates[template.template_id] = template
                return template
            
            async def get_template(self, template_id: str) -> Optional[NotebookTemplate]:
                return self.templates.get(template_id)
            
            async def list_templates(self, **kwargs) -> List[NotebookTemplate]:
                templates = list(self.templates.values())
                if kwargs.get('category'):
                    templates = [t for t in templates if t.category == kwargs['category']]
                return templates
            
            async def save_snippet(self, snippet: CodeSnippet) -> CodeSnippet:
                self.snippets[snippet.snippet_id] = snippet
                return snippet
            
            async def list_snippets(self, **kwargs) -> List[CodeSnippet]:
                snippets = list(self.snippets.values())
                if kwargs.get('category'):
                    snippets = [s for s in snippets if s.category == kwargs['category']]
                if kwargs.get('language'):
                    snippets = [s for s in snippets if s.language == kwargs['language']]
                return snippets
            
            async def save_execution(self, execution: NotebookExecution) -> NotebookExecution:
                self.executions[execution.execution_id] = execution
                return execution
            
            async def save_snapshot(self, snapshot: NotebookSnapshot) -> NotebookSnapshot:
                if snapshot.session_id not in self.snapshots:
                    self.snapshots[snapshot.session_id] = []
                self.snapshots[snapshot.session_id].append(snapshot)
                return snapshot
            
            async def list_snapshots(self, session_id: str, limit: int = 10) -> List[NotebookSnapshot]:
                snapshots = self.snapshots.get(session_id, [])
                return sorted(snapshots, key=lambda s: s.created_at, reverse=True)[:limit]
        
        return MockRepository()
    
    def _initialize_default_templates(self):
        """Initialize default notebook templates."""
        templates = [
            {
                "name": "Data Exploration Starter",
                "category": "data_exploration",
                "description": "Basic template for exploring Aurum data",
                "notebook_content": self._create_data_exploration_notebook(),
                "required_packages": ["pandas", "matplotlib", "seaborn"],
                "tags": ["starter", "data", "visualization"]
            },
            {
                "name": "API Testing Notebook",
                "category": "api_testing",
                "description": "Template for testing Aurum API endpoints",
                "notebook_content": self._create_api_testing_notebook(),
                "required_packages": ["requests", "pandas"],
                "tags": ["api", "testing", "integration"]
            },
            {
                "name": "ML Model Training",
                "category": "ml_training",
                "description": "Template for training ML models with Aurum data",
                "notebook_content": self._create_ml_training_notebook(),
                "required_packages": ["scikit-learn", "xgboost", "pandas", "numpy"],
                "tags": ["ml", "training", "modeling"]
            }
        ]
        
        for template_data in templates:
            template = NotebookTemplate(**template_data)
            self._templates[template.template_id] = template
            asyncio.create_task(self.repository.save_template(template))
    
    def _initialize_default_snippets(self):
        """Initialize default code snippets."""
        snippets = [
            {
                "name": "Aurum API Client Setup",
                "category": "api",
                "description": "Initialize Aurum API client with authentication",
                "code": """from aurum.api.client import AurumClient

# Initialize client
client = AurumClient(
    base_url=os.getenv('AURUM_API_URL', 'http://localhost:8095'),
    api_key=os.getenv('AURUM_API_KEY')
)

# Test connection
health = client.health_check()
print(f"API Status: {health['status']}")""",
                "imports": ["os"],
                "tags": ["api", "client", "setup"]
            },
            {
                "name": "Load Curve Data",
                "category": "data_loading",
                "description": "Load curve data for analysis",
                "code": """# Load curve data
curves = client.curves.list(
    iso='{iso}',
    market='{market}',
    start_date='{start_date}',
    end_date='{end_date}',
    limit=1000
)

# Convert to DataFrame
df = pd.DataFrame(curves['data'])
df['datetime'] = pd.to_datetime(df['datetime'])
df.set_index('datetime', inplace=True)

print(f"Loaded {len(df)} curve records")
df.head()""",
                "parameters": {
                    "iso": "PJM",
                    "market": "DA",
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31"
                },
                "imports": ["pandas as pd"],
                "tags": ["curves", "data", "loading"]
            },
            {
                "name": "Train Price Prediction Model",
                "category": "ml",
                "description": "Train a simple price prediction model",
                "code": """from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

# Prepare features
X = df[['hour', 'day_of_week', 'month', 'load_mw', 'temperature']]
y = df['lmp']

# Split data
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Train model
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Evaluate
y_pred = model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"MSE: {mse:.2f}")
print(f"R²: {r2:.3f}")""",
                "imports": [],
                "tags": ["ml", "training", "prediction"]
            }
        ]
        
        for snippet_data in snippets:
            snippet = CodeSnippet(**snippet_data)
            self._snippets[snippet.snippet_id] = snippet
            asyncio.create_task(self.repository.save_snippet(snippet))
    
    def _create_data_exploration_notebook(self) -> Dict[str, Any]:
        """Create a data exploration notebook template."""
        return {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["# Data Exploration with Aurum\n\n",
                             "This notebook demonstrates basic data exploration using the Aurum API."]
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": ["# Import required libraries\n",
                             "import pandas as pd\n",
                             "import matplotlib.pyplot as plt\n",
                             "import seaborn as sns\n",
                             "from aurum.api.client import AurumClient\n",
                             "\n",
                             "# Set up plotting\n",
                             "plt.style.use('seaborn')\n",
                             "%matplotlib inline"]
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": ["# Initialize Aurum client\n",
                             "client = AurumClient()\n",
                             "\n",
                             "# Load sample data\n",
                             "# TODO: Add your data loading code here"]
                }
            ],
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }
    
    def _create_api_testing_notebook(self) -> Dict[str, Any]:
        """Create an API testing notebook template."""
        return {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["# Aurum API Testing\n\n",
                             "Test and explore Aurum API endpoints interactively."]
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": ["import requests\n",
                             "import json\n",
                             "import pandas as pd\n",
                             "\n",
                             "# API configuration\n",
                             "BASE_URL = 'http://localhost:8095'\n",
                             "API_KEY = 'your-api-key-here'"]
                }
            ],
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }
    
    def _create_ml_training_notebook(self) -> Dict[str, Any]:
        """Create an ML training notebook template."""
        return {
            "cells": [
                {
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["# ML Model Training with Aurum Data\n\n",
                             "Train machine learning models using Aurum energy data."]
                },
                {
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": ["import pandas as pd\n",
                             "import numpy as np\n",
                             "from sklearn.model_selection import train_test_split\n",
                             "from sklearn.ensemble import RandomForestRegressor\n",
                             "import matplotlib.pyplot as plt"]
                }
            ],
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }
    
    async def get_template(self, template_id: str) -> Optional[NotebookTemplate]:
        """
        Get a notebook template by ID.
        
        Args:
            template_id: Template identifier
            
        Returns:
            NotebookTemplate if found
        """
        # Check memory cache
        if template_id in self._templates:
            return self._templates[template_id]
        
        # Check persistent cache
        cache_key = f"notebook_template:{template_id}"
        if self.cache_enabled:
            cached = await self._get_from_cache(cache_key)
            if cached:
                template = NotebookTemplate(**cached)
                self._templates[template_id] = template
                return template
        
        # Load from repository
        template = await self.repository.get_template(template_id)
        if template:
            self._templates[template_id] = template
            if self.cache_enabled:
                await self._set_cache(cache_key, template.dict(), ttl=self.cache_ttl)
        
        return template
    
    async def list_templates(
        self,
        category: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> List[NotebookTemplate]:
        """
        List available notebook templates.
        
        Args:
            category: Filter by category
            tags: Filter by tags
            
        Returns:
            List of NotebookTemplate instances
        """
        templates = await self.repository.list_templates(
            category=category,
            tags=tags
        )
        
        # Include default templates
        for template in self._templates.values():
            if template.template_id not in [t.template_id for t in templates]:
                if not category or template.category == category:
                    if not tags or any(tag in template.tags for tag in tags):
                        templates.append(template)
        
        return templates
    
    async def create_notebook_from_template(
        self,
        template_id: str,
        session_id: str,
        notebook_name: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a new notebook from a template.
        
        Args:
            template_id: Template to use
            session_id: Session to create notebook in
            notebook_name: Name for the new notebook
            parameters: Parameters to substitute in template
            
        Returns:
            Path to created notebook
        """
        template = await self.get_template(template_id)
        if not template:
            raise ValueError(f"Template {template_id} not found")
        
        # Clone notebook content
        notebook_content = json.loads(json.dumps(template.notebook_content))
        
        # Substitute parameters if provided
        if parameters:
            notebook_json = json.dumps(notebook_content)
            for key, value in parameters.items():
                notebook_json = notebook_json.replace(f"{{{key}}}", str(value))
            notebook_content = json.loads(notebook_json)
        
        # In real implementation, would save to Jupyter server
        notebook_path = f"/notebooks/{session_id}/{notebook_name}.ipynb"
        
        self.logger.info(
            f"Created notebook from template",
            extra={
                "template_id": template_id,
                "session_id": session_id,
                "notebook_path": notebook_path
            }
        )
        
        # Emit metric
        await self._emit_metric(
            "notebook_created_from_template",
            tags={"template": template.name.replace(" ", "_").lower()}
        )
        
        return notebook_path
    
    async def list_code_snippets(
        self,
        category: Optional[str] = None,
        language: str = "python"
    ) -> List[CodeSnippet]:
        """
        List available code snippets.
        
        Args:
            category: Filter by category
            language: Filter by language
            
        Returns:
            List of CodeSnippet instances
        """
        snippets = await self.repository.list_snippets(
            category=category,
            language=language
        )
        
        # Include default snippets
        for snippet in self._snippets.values():
            if snippet.snippet_id not in [s.snippet_id for s in snippets]:
                if (not category or snippet.category == category) and snippet.language == language:
                    snippets.append(snippet)
        
        return snippets
    
    async def execute_notebook(
        self,
        session_id: str,
        notebook_path: str,
        kernel_name: str = "python3",
        timeout: int = 300
    ) -> NotebookExecution:
        """
        Execute a notebook asynchronously.
        
        Args:
            session_id: Session containing the notebook
            notebook_path: Path to notebook
            kernel_name: Kernel to use
            timeout: Execution timeout in seconds
            
        Returns:
            NotebookExecution with results
        """
        execution = NotebookExecution(
            session_id=session_id,
            notebook_path=notebook_path,
            status="running",
            started_at=datetime.utcnow()
        )
        
        # Save execution
        execution = await self.repository.save_execution(execution)
        self._active_executions[execution.execution_id] = execution
        
        # Start execution asynchronously
        asyncio.create_task(
            self._execute_notebook_async(execution.execution_id, kernel_name, timeout)
        )
        
        self.logger.info(
            f"Started notebook execution",
            extra={
                "execution_id": execution.execution_id,
                "notebook_path": notebook_path
            }
        )
        
        return execution
    
    async def _execute_notebook_async(
        self,
        execution_id: str,
        kernel_name: str,
        timeout: int
    ):
        """Execute notebook asynchronously (simulated)."""
        try:
            # Simulate execution
            await asyncio.sleep(5)
            
            execution = self._active_executions.get(execution_id)
            if not execution:
                return
            
            # Simulate successful execution
            execution.status = "completed"
            execution.completed_at = datetime.utcnow()
            execution.cells_executed = 10
            execution.cells_total = 10
            execution.outputs = [
                {"cell_index": 0, "output_type": "stream", "text": "Hello from notebook!"}
            ]
            
            await self.repository.save_execution(execution)
            
        except Exception as e:
            execution = self._active_executions.get(execution_id)
            if execution:
                execution.status = "failed"
                execution.error_message = str(e)
                execution.completed_at = datetime.utcnow()
                await self.repository.save_execution(execution)
    
    async def create_snapshot(
        self,
        session_id: str,
        notebook_path: str,
        description: Optional[str] = None,
        created_by: str = "system"
    ) -> NotebookSnapshot:
        """
        Create a snapshot of a notebook.
        
        Args:
            session_id: Session containing the notebook
            notebook_path: Path to notebook
            description: Snapshot description
            created_by: User creating snapshot
            
        Returns:
            Created NotebookSnapshot
        """
        # In real implementation, would read notebook from Jupyter
        notebook_content = {
            "cells": [],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 4
        }
        
        snapshot = NotebookSnapshot(
            session_id=session_id,
            notebook_path=notebook_path,
            content=notebook_content,
            description=description,
            created_by=created_by
        )
        
        # Save snapshot
        snapshot = await self.repository.save_snapshot(snapshot)
        
        self.logger.info(
            f"Created notebook snapshot",
            extra={
                "snapshot_id": snapshot.snapshot_id,
                "session_id": session_id,
                "notebook_path": notebook_path
            }
        )
        
        # Emit metric
        await self._emit_metric("notebook_snapshot_created")
        
        return snapshot
    
    async def list_snapshots(
        self,
        session_id: str,
        limit: int = 10
    ) -> List[NotebookSnapshot]:
        """
        List snapshots for a session.
        
        Args:
            session_id: Session ID
            limit: Maximum snapshots to return
            
        Returns:
            List of NotebookSnapshot instances
        """
        return await self.repository.list_snapshots(session_id, limit)
    
    async def export_notebook(
        self,
        session_id: str,
        notebook_path: str,
        format: str = "ipynb"
    ) -> bytes:
        """
        Export a notebook in various formats.
        
        Args:
            session_id: Session containing the notebook
            notebook_path: Path to notebook
            format: Export format (ipynb, html, pdf, py)
            
        Returns:
            Exported notebook data
        """
        # In real implementation, would use nbconvert
        if format == "ipynb":
            # Return JSON notebook
            notebook_data = json.dumps({
                "cells": [],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 4
            })
            return notebook_data.encode('utf-8')
        
        elif format == "py":
            # Return Python script
            return b"# Converted from notebook\nprint('Hello from notebook')"
        
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    async def _emit_metric(self, metric_name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None):
        """Emit a metric (placeholder for actual implementation)."""
        # TODO: Integrate with telemetry service
        self.logger.debug(f"Metric: {metric_name}={value}, tags={tags}")
    
    async def _get_from_cache(self, key: str) -> Optional[Dict[str, Any]]:
        """Get value from cache (placeholder)."""
        # TODO: Integrate with cache service
        return None
    
    async def _set_cache(self, key: str, value: Dict[str, Any], ttl: int):
        """Set value in cache (placeholder)."""
        # TODO: Integrate with cache service
        pass
