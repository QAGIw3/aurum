"""Parser-specific exceptions."""

from __future__ import annotations

from typing import Any, Dict, Optional

from ..api.exceptions import AurumAPIException


class ParserException(AurumAPIException):
    """Base exception for parser errors."""

    def __init__(
        self,
        parser: str,
        file_path: str,
        message: str,
        request_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        context = context or {}
        context.update({
            "parser": parser,
            "file_path": file_path,
        })
        super().__init__(
            status_code=500,
            detail=message,
            request_id=request_id,
            context=context,
        )


class ValidationException(AurumAPIException):
    """Exception raised when parser validation fails."""

    def __init__(
        self,
        field: str,
        message: str,
        request_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        context = context or {}
        context.update({"field": field})
        super().__init__(
            status_code=400,
            detail=message,
            request_id=request_id,
            context=context,
        )


class ProcessingException(AurumAPIException):
    """Exception raised when data processing fails."""

    def __init__(
        self,
        operation: str,
        message: str,
        request_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        context = context or {}
        context.update({"operation": operation})
        super().__init__(
            status_code=500,
            detail=message,
            request_id=request_id,
            context=context,
        )


class FileFormatException(ParserException):
    """Exception raised when file format is invalid."""

    def __init__(
        self,
        parser: str,
        file_path: str,
        format_issue: str,
        request_id: Optional[str] = None,
    ):
        message = f"Invalid file format: {format_issue}"
        super().__init__(
            parser=parser,
            file_path=file_path,
            message=message,
            request_id=request_id,
            context={"format_issue": format_issue},
        )


class DataQualityException(ParserException):
    """Exception raised when data quality issues are detected."""

    def __init__(
        self,
        parser: str,
        file_path: str,
        quality_issues: list,
        request_id: Optional[str] = None,
    ):
        message = f"Data quality issues detected: {', '.join(quality_issues[:3])}"
        if len(quality_issues) > 3:
            message += f" (and {len(quality_issues) - 3} more)"
        super().__init__(
            parser=parser,
            file_path=file_path,
            message=message,
            request_id=request_id,
            context={"quality_issues": quality_issues},
        )
