# utils/exceptions.py

class SapODataError(Exception):
    """Base class for SAP OData API errors"""
    pass

class AuthenticationError(SapODataError):
    """Error during authentication"""
    pass

class QueryConstructionError(SapODataError):
    """Error constructing OData query"""
    pass

class ConnectionError(SapODataError):
    """Error connecting to SAP B1 service"""
    pass

class TimeoutError(SapODataError):
    """Request timed out"""
    pass

class RequestError(SapODataError):
    """Error in request execution"""
    pass

class EntityRegistryError(SapODataError):
    """Error in entity registry operations"""
    pass

class MetadataError(SapODataError):
    """Error in metadata management"""
    pass

# Add the new function below the exception classes
def format_user_friendly_error(error_data: dict) -> str:
    """
    Format an error in a user-friendly way.
    
    Args:
        error_data: Error data dictionary
        
    Returns:
        User-friendly error message
    """
    error_type = error_data.get("stage", "unknown")
    error_message = error_data.get("message", "Unknown error")
    error_details = error_data.get("details", {})
    
    # Mapping of technical error types to user-friendly messages
    error_messages = {
        "request_execution": "Unable to complete your request to the SAP system",
        "query_understanding": "I had trouble understanding your request",
        "odata_construction": "There was an issue creating the request",
        "parameter_handling": "There was an issue with the request parameters",
        "intent_extraction": "I couldn't determine what you're asking for",
        "query_orchestration": "I had trouble structuring your request",
        "error_recovery": "I couldn't recover from a previous error",
        "authentication": "Authentication error with SAP B1"
    }
    
    # Common error patterns and user-friendly interpretations
    common_errors = {
        "Invalid filter condition": "The search criteria appears to be incorrect",
        "not found": "The requested information couldn't be found",
        "unauthorized": "You don't have permission to access this information",
        "bad request": "The request format was invalid",
        "Not Found": "The requested entity doesn't exist",
        "timeout": "The request timed out. The server might be busy, please try again later"
    }
    
    # Start with a basic message based on error type
    friendly_message = error_messages.get(error_type, "An error occurred")
    
    # Look for common error patterns in the message
    for pattern, interpretation in common_errors.items():
        if pattern.lower() in error_message.lower():
            friendly_message += f": {interpretation}"
            break
    else:
        # If no common pattern matched, add a simplified version of the original message
        # Strip API-specific details and shorten long messages
        simplified_message = error_message
        if len(simplified_message) > 100:
            simplified_message = simplified_message[:100] + "..."
        
        # Remove technical prefixes/stack traces
        if ":" in simplified_message:
            parts = simplified_message.split(":", 1)
            if any(prefix in parts[0] for prefix in ["API", "Error", "SAP", "OData"]):
                simplified_message = parts[1].strip()
        
        friendly_message += f": {simplified_message}"
    
    # Add suggestions for recovery if applicable
    if error_type == "request_execution":
        if "authentication" in error_message.lower():
            friendly_message += ". Try checking your SAP credentials."
        elif "timeout" in error_message.lower():
            friendly_message += ". The server might be busy, please try again with a more specific query."
    elif error_type == "query_understanding":
        friendly_message += ". Try rephrasing your request with more specific details."
    
    return friendly_message

def handle_metadata_error(operation_name, error, logger, critical=False):
    """
    Helper function to handle metadata operation errors consistently.
    
    Args:
        operation_name: Name of the operation being performed
        error: The exception that occurred
        logger: Logger instance
        critical: If True, re-raises the exception
    """
    logger.error(f"Error in {operation_name}: {str(error)}")
    
    # Add debugging context
    import traceback
    logger.debug(f"Traceback for {operation_name}: {traceback.format_exc()}")
    
    # Log the stack trace
    logger.debug(f"Stack trace for {operation_name}:\n{traceback.format_exc()}")
    
    # Re-raise if critical
    if critical:
        raise error