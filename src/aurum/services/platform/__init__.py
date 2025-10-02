"""Platform Services Module.

This module contains platform-related services extracted from
monolithic implementations as part of the service layer decomposition.

Services:
- WorkspaceManagerService: Developer workspace lifecycle management
- NotebookIntegrationService: Jupyter notebook integration
- ApiDocumentationService: API documentation and interactive testing
"""

from .workspace_manager import (
    WorkspaceManagerService,
    NotebookEnvironment,
    NotebookSession,
    WorkspaceRepository,
    StorageQuotaExceeded,
    SessionLimitExceeded
)

from .notebook_integration import (
    NotebookIntegrationService,
    NotebookTemplate,
    CodeSnippet,
    NotebookExecution,
    NotebookSnapshot,
    NotebookRepository
)

from .api_documentation import (
    ApiDocumentationService,
    ApiEndpoint,
    CodeExample,
    InteractiveTest,
    ApiDocumentationRepository
)

__all__ = [
    # Workspace Manager
    "WorkspaceManagerService",
    "NotebookEnvironment",
    "NotebookSession",
    "WorkspaceRepository",
    "StorageQuotaExceeded",
    "SessionLimitExceeded",
    
    # Notebook Integration
    "NotebookIntegrationService",
    "NotebookTemplate",
    "CodeSnippet",
    "NotebookExecution",
    "NotebookSnapshot",
    "NotebookRepository",
    
    # API Documentation
    "ApiDocumentationService",
    "ApiEndpoint",
    "CodeExample",
    "InteractiveTest",
    "ApiDocumentationRepository",
]