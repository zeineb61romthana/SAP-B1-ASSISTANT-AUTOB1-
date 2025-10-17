# tools/parameter_handler.py

from typing import Dict, Any
from config import get_sap_credentials

class ParameterHandlerTool:
    
    def __init__(self):
        # The __init__ method is intentionally left empty because
        # this class does not require any initialization logic.
        pass
    
    def _prepare_auth_parameters(self) -> Dict[str, Any]:
        """Prepare authentication parameters for the SAP B1 request."""
        # Get credentials from config
        credentials = get_sap_credentials()
        
        # Format them for use in request
        auth_params = {
            "service_layer_url": credentials["service_layer_url"],
            "company_db": credentials["company_db"],
            "username": credentials["username"],
            "password": credentials["password"]
        }
        
        return auth_params
    
    def _prepare_request_parameters(self, structured_query: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare additional parameters for the request based on the structured query."""
        # Start with an empty parameter set
        params = {}
        
        # Add HTTP method (default is GET for data retrieval)
        params["method"] = "GET"
        
        # Add content type header
        params["headers"] = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # Add any custom parameters from the structured query
        if "custom_parameters" in structured_query:
            params["custom"] = structured_query["custom_parameters"]
        
        # Handle pagination parameters
        if "page_size" in structured_query:
            params["page_size"] = structured_query["page_size"]
        
        if "page_number" in structured_query:
            params["page_number"] = structured_query["page_number"]
        
        return params
    
    
    def invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare parameters for the SAP B1 OData request."""
        try:
            # Check if structured query is present
            if "structured_query" not in state:
                raise ValueError("Structured query not found in state")
            
            # Prepare authentication parameters
            auth_params = self._prepare_auth_parameters()
            
            # Prepare request parameters
            request_params = self._prepare_request_parameters(state["structured_query"])
            
            # Combine parameters
            state["parameters"] = {
                "auth": auth_params,
                "request": request_params
            }
            
            return state
            
        except Exception as e:
            state["error"] = {
                "stage": "parameter_handling",
                "message": str(e),
                "can_retry": False
            }
            return state