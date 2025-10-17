# utils/enhanced_errors.py

from typing import Dict, Any, Optional, List
import traceback
import json
import logging

logger = logging.getLogger(__name__)

class SAPAssistantError(Exception):
    """Base class for all SAP Assistant errors."""
    
    def __init__(self, message: str, code: str = "UNKNOWN_ERROR", 
                 details: Optional[Dict[str, Any]] = None, 
                 can_retry: bool = False, 
                 suggestions: Optional[List[str]] = None,
                 original_exception: Optional[Exception] = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}
        self.can_retry = can_retry
        self.suggestions = suggestions or []
        self.original_exception = original_exception
        self.traceback = traceback.format_exc() if original_exception else None
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to a standardized dictionary format."""
        return {
            "stage": self.code.split('_')[0].lower() if '_' in self.code else "general",
            "message": self.message,
            "code": self.code,
            "details": self.details,
            "can_retry": self.can_retry,
            "suggestions": self.suggestions,
            "user_message": self.get_user_message()
        }
        
    def get_user_message(self) -> str:
        """Generate a user-friendly error message."""
        base_message = self.message
        
        # Add suggestions if available
        if self.suggestions:
            suggestion_text = "\n\nSuggestions:\n"
            for i, suggestion in enumerate(self.suggestions, 1):
                suggestion_text += f"{i}. {suggestion}\n"
            base_message += suggestion_text
            
        return base_message
        
    def log(self, log_level=logging.ERROR):
        """Log error details at specified level."""
        error_dict = self.to_dict()
        logger.log(log_level, f"Error {self.code}: {self.message}")
        if self.details:
            logger.log(log_level, f"Details: {json.dumps(self.details, indent=2)}")
        if self.traceback:
            logger.log(log_level, f"Traceback: {self.traceback}")
        if self.suggestions:
            logger.log(log_level, f"Suggestions: {', '.join(self.suggestions)}")
        return error_dict

# Specific error types organized by domain

# Query Understanding Errors
class QueryUnderstandingError(SAPAssistantError):
    """Error in query understanding phase."""
    def __init__(self, message, **kwargs):
        kwargs.setdefault("code", "QUERY_UNDERSTANDING_ERROR")
        super().__init__(message, **kwargs)

class IntentRecognitionError(QueryUnderstandingError):
    """Failed to recognize intent from query."""
    def __init__(self, message, **kwargs):
        kwargs.setdefault("code", "QUERY_INTENT_ERROR")
        kwargs.setdefault("suggestions", ["Try rephrasing your query", 
                                          "Specify the entity type explicitly", 
                                          "Use more specific language"])
        super().__init__(message, **kwargs)

class EntityExtractionError(QueryUnderstandingError):
    """Failed to extract entities from query."""
    def __init__(self, message, **kwargs):
        kwargs.setdefault("code", "QUERY_ENTITY_ERROR")
        kwargs.setdefault("suggestions", ["Use more specific criteria", 
                                         "Specify field names explicitly", 
                                         "Check field name spelling"])
        super().__init__(message, **kwargs)

# URL Construction Errors
class URLConstructionError(SAPAssistantError):
    """Error in OData URL construction."""
    def __init__(self, message, **kwargs):
        kwargs.setdefault("code", "URL_CONSTRUCTION_ERROR")
        super().__init__(message, **kwargs)

class InvalidFilterError(URLConstructionError):
    """Invalid filter in OData URL."""
    def __init__(self, message, **kwargs):
        kwargs.setdefault("code", "URL_INVALID_FILTER")
        kwargs.setdefault("suggestions", ["Check the filter syntax", 
                                        "Verify field names exist", 
                                        "Ensure values are properly formatted"])
        super().__init__(message, **kwargs)

class EntityNotFoundError(URLConstructionError):
    """Entity type not found."""
    def __init__(self, message, entity_type=None, **kwargs):
        kwargs.setdefault("code", "URL_ENTITY_NOT_FOUND")
        kwargs.setdefault("details", {}).update({"entity_type": entity_type})
        
        # Suggest similar entities if available
        if "similar_entities" in kwargs:
            similar = kwargs.pop("similar_entities")
            suggestions = [f"Did you mean '{entity}'?" for entity in similar[:3]]
            kwargs.setdefault("suggestions", suggestions)
        
        super().__init__(message, **kwargs)

# Request Execution Errors
class RequestExecutionError(SAPAssistantError):
    """Error executing request to SAP API."""
    def __init__(self, message, **kwargs):
        kwargs.setdefault("code", "REQUEST_EXECUTION_ERROR")
        super().__init__(message, **kwargs)

class AuthenticationError(RequestExecutionError):
    """Authentication failed with SAP API."""
    def __init__(self, message, **kwargs):
        kwargs.setdefault("code", "REQUEST_AUTHENTICATION_ERROR")
        kwargs.setdefault("can_retry", True)
        kwargs.setdefault("suggestions", ["Check SAP credentials", 
                                         "Verify SAP service is available", 
                                         "Session may have expired, try again"])
        super().__init__(message, **kwargs)

class ConnectionError(RequestExecutionError):
    """Failed to connect to SAP API."""
    def __init__(self, message, **kwargs):
        kwargs.setdefault("code", "REQUEST_CONNECTION_ERROR")
        kwargs.setdefault("can_retry", True)
        kwargs.setdefault("suggestions", ["Verify SAP server is accessible", 
                                         "Check network connectivity", 
                                         "Try again later"])
        super().__init__(message, **kwargs)

class APIResponseError(RequestExecutionError):
    """Error in SAP API response."""
    def __init__(self, message, status_code=None, response_body=None, **kwargs):
        kwargs.setdefault("code", "REQUEST_API_ERROR")
        kwargs.setdefault("details", {}).update({
            "status_code": status_code,
            "response_snippet": str(response_body)[:200] if response_body else None
        })
        super().__init__(message, **kwargs)

# Result Formatting Errors
class FormattingError(SAPAssistantError):
    """Error formatting results."""
    def __init__(self, message, **kwargs):
        kwargs.setdefault("code", "FORMATTING_ERROR")
        super().__init__(message, **kwargs)

# URL Validation Errors
class URLValidationError(SAPAssistantError):
    """Error validating OData URL."""
    def __init__(self, message, url=None, validation_issues=None, **kwargs):
        kwargs.setdefault("code", "URL_VALIDATION_ERROR")
        kwargs.setdefault("details", {}).update({
            "url": url,
            "validation_issues": validation_issues or []
        })
        super().__init__(message, **kwargs)

# Function to uniformly format errors for response
def format_error_for_response(error: Exception) -> Dict[str, Any]:
    """Format any exception as a standardized error response."""
    if isinstance(error, SAPAssistantError):
        return error.to_dict()
    else:
        # Convert standard exceptions to our format
        return SAPAssistantError(
            message=str(error),
            code="UNEXPECTED_ERROR",
            details={"error_type": error.__class__.__name__},
            can_retry=False,
            suggestions=["Try simplifying your query", "Contact support if the issue persists"]
        ).to_dict()