# tools/request_executor.py

from typing import Dict, Any, Optional
from integration.enhanced_sap_client import SAPB1EnhancedClient
import time
import re
from datetime import datetime, timedelta
from config import get_sap_credentials
from utils.exceptions import (
    SapODataError, 
    AuthenticationError, 
    RequestError,
    format_user_friendly_error,
    ConnectionError as SAPConnectionError
)

import logging

# Configure logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class RequestExecutorTool:
    def __init__(self, sap_client=None):
        # Accept an external SAP client or create our own
        self.sap_client = sap_client

        # Initialize credentials
        self.credentials = None
        try:
            self.credentials = get_sap_credentials()
            print("Initialized RequestExecutorTool with credentials")
        except Exception as e:
            print(f"Error loading credentials in RequestExecutorTool init: {str(e)}")
            # Set fallback credentials
            self.credentials = {
                "service_layer_url": "https://172.16.0.217:50000/b1s/v1",
                "company_db": "GOTO_TEST",
                "username": "manager",
                "password": "infor"
            }
        
        # Create a new SAP client if one wasn't provided
        if self.sap_client is None:
            # Try to use the enhanced client first, fall back to regular client
            try:
                self.sap_client = SAPB1EnhancedClient(
                    service_layer_url=self.credentials.get("service_layer_url"),
                    company_db=self.credentials.get("company_db"),
                    username=self.credentials.get("username"),
                    password=self.credentials.get("password")
                )
                print("Using enhanced SAP client")
            except ImportError:
                # Fall back to the regular client
                self.sap_client = SAPB1EnhancedClient(
                    service_layer_url=self.credentials.get("service_layer_url"), 
                    company_db=self.credentials.get("company_db")
                )
                print("Using regular SAP client")
        
        # Enhanced error correction patterns
        self.error_corrections = {
            "Property 'DocStatus' of 'Document'is invalid": 
                lambda url: url.replace("DocStatus", "DocumentStatus"),
            "Property 'DocDate' of 'BusinessPartner'is invalid": 
                lambda url: url.replace("DocDate", "CreateDate"),
            "Property 'RefDate' of 'JournalEntry'is invalid": 
                lambda url: url.replace("RefDate", "ReferenceDate"),
            "Property 'DocumentStatus' of 'ProductionOrder'is invalid": 
                lambda url: url.replace("DocumentStatus", "ProductionOrderStatus"),
            "Property 'None' of": 
                lambda url: url.replace(" eq None", " eq null").replace(" ne None", " ne null"),
            "the given value('now') of property": 
                self._fix_now_values,
            "the given value('open') of property 'Status' is not a NUMBER": 
                lambda url: url.replace("Status eq 'open'", "Status eq -1"),
            "Query std::string error - no matched single quote is found": 
                self._fix_string_escaping
        }
        self.dynamic_corrections = {}
        self.prevention_stats = {"attempted": 0, "successful": 0}

    def _fix_now_values(self, url: str) -> str:
        """Fix 'now' date values with actual dates"""
        now = datetime.now()
        
        fixes = [
            (r"ge\s+'now'", f"ge datetime'{now.strftime('%Y-%m-%d')}T00:00:00'"),
            (r"le\s+'now\+3m'", f"le datetime'{(now + timedelta(days=90)).strftime('%Y-%m-%d')}T23:59:59'"),
            (r"eq\s+'now'", f"eq datetime'{now.strftime('%Y-%m-%d')}T{now.strftime('%H:%M:%S')}'"),
        ]
        
        fixed_url = url
        for pattern, replacement in fixes:
            fixed_url = re.sub(pattern, replacement, fixed_url)
        return fixed_url

    def _fix_string_escaping(self, url: str) -> str:
        """Fix string escaping for names with apostrophes"""
        # Pattern to find quoted strings in filter conditions
        pattern = r"(\w+\s+eq\s+')([^']*(?:'[^']*)*)'(?=\s|&|$)"
        
        def escape_quotes(match):
            field_part = match.group(1)  # "CardName eq '"
            string_value = match.group(2)  # "O'Neill Inc."
            # Only escape if not already escaped
            if "'" in string_value and "''" not in string_value:
                escaped_value = string_value.replace("'", "''")
                return f"{field_part}{escaped_value}'"
            return match.group(0)
        
        return re.sub(pattern, escape_quotes, url)

    def add_dynamic_corrections(self, new_corrections):
        """Add dynamically learned correction rules"""
        self.dynamic_corrections.update(new_corrections)
        logger.info(f"Added {len(new_corrections)} dynamic correction rules")

    def _try_error_correction(self, error_message: str, failed_url: str) -> str:
        """Enhanced error correction with dynamic rules"""
        # First try dynamic corrections (learned patterns)
        for error_pattern, correction_func in self.dynamic_corrections.items():
            if error_pattern in error_message:
                try:
                    corrected_url = correction_func(failed_url)
                    if corrected_url != failed_url:
                        logger.info(f"🔧 Applied dynamic correction for: {error_pattern}")
                        return corrected_url
                except Exception as e:
                    logger.error(f"Error applying dynamic correction: {e}")

        # Fall back to your existing static corrections
        for error_pattern, correction_func in self.error_corrections.items():
            if error_pattern in error_message:
                try:
                    corrected_url = correction_func(failed_url)
                    if corrected_url != failed_url:
                        logger.info(f"🔧 Applied static correction for: {error_pattern}")
                        return corrected_url
                except Exception as e:
                    logger.error(f"Error applying static correction: {e}")
        return failed_url
    
    def invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the OData request with auto-correction and retry logic."""
        max_retries = 2
        attempt = 0
        
        while attempt <= max_retries:
            attempt += 1
            
            # Execute the request
            result = self._execute_single_request(state)
            
            # If successful, return
            if "error" not in result or not result["error"]:
                if attempt > 1:
                    print(f"✅ Request succeeded after {attempt} attempts")
                return result
            
            # If error and we can retry, try to correct it
            if attempt <= max_retries:
                error_message = result["error"].get("message", "")
                failed_url = state.get("odata_url", "")
                
                corrected_url = self._try_error_correction(error_message, failed_url)
                
                if corrected_url != failed_url:
                    print(f"🔄 Retrying with corrected URL (attempt {attempt + 1})")
                    state["odata_url"] = corrected_url
                    continue
                else:
                    print(f"❌ No correction available for: {error_message}")
                    break
            
        return result

    def _execute_single_request(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced request execution with prevention tracking"""
        try:
            # Start tracking execution time
            execution_start_time = time.time()
            
            # Check if URL is present
            if "odata_url" not in state:
                state["error"] = {
                    "stage": "request_execution",
                    "message": "OData URL not found in state",
                    "details": {"type": "configuration"},
                    "can_retry": False,
                    "user_message": "Missing required query information"
                }
                state["output"] = format_user_friendly_error(state["error"])
                return state
            
            # Extract URL path
            odata_url = state["odata_url"]
            if odata_url.startswith("http"):
                # Find the last occurrence of 'b1s/v1/' and extract everything after it
                parts = odata_url.split('b1s/v1/')
                if len(parts) > 1:
                    odata_path = parts[-1]
                else:
                    odata_path = odata_url
            else:
                odata_path = odata_url
                
            print(f"Using path for request: {odata_path}")
            
            # NEW: Track if this was a prevented high-risk query
            was_high_risk = "proactive_intervention" in state
            original_risk_score = 0.0
            if was_high_risk:
                original_risk_score = state["proactive_intervention"]["risk_score"]
                self.prevention_stats["attempted"] += 1
                logger.info(f"Executing previously high-risk query (risk: {original_risk_score:.3f})")
            
            # Execute request with error handling
            try:
                # Login if needed
                if isinstance(self.sap_client, SAPB1EnhancedClient):
                    login_success = self.sap_client.login()
                    if not login_success:
                        raise AuthenticationError("Failed to login to SAP B1 Service Layer")
                        
                # Execute request
                response = self.sap_client.execute_request(url=odata_path)
                
                # Logout if using original client (enhanced client manages its own session)
                if isinstance(self.sap_client, SAPB1EnhancedClient):
                    self.sap_client.logout()
                
                # Calculate execution time
                execution_time = time.time() - execution_start_time
                
                # Handle response
                if isinstance(response, dict) and "error" in response:
                    # If failed and was high-risk, count as prevention failure
                    if was_high_risk:
                        logger.warning(f"Prevention failed for high-risk query")
                        if "metadata_manager" in state and state["metadata_manager"]:
                            state["metadata_manager"].update_prevention_success(original_risk_score, False)
                    
                    error_message = response.get("error", "Unknown error")
                    state["error"] = {
                        "stage": "request_execution",
                        "message": error_message,
                        "details": {"type": "api_response"},
                        "can_retry": True if "auth" in str(error_message).lower() else False,
                        "user_message": f"SAP system returned an error response"
                    }
                    state["output"] = format_user_friendly_error(state["error"])
                    
                    # Learning from failures
                    if "metadata_manager" in state and state["metadata_manager"]:
                        metadata_manager = state["metadata_manager"]
                        
                        # Store error example
                        metadata_manager.store_error_example(
                            intent=state.get("intent", "unknown"),
                            endpoint=state.get("endpoint", "unknown"),
                            entities=state.get("structured_query", {}).get("filter_conditions", []),
                            query_pattern=state.get("odata_url", ""),
                            error_message=error_message,
                            error_type=state["error"].get("stage", "UnknownError")
                        )
                        
                        # Analyze error with LLM if available
                        if hasattr(metadata_manager, "analyze_error_with_llm"):
                            error_analysis = metadata_manager.analyze_error_with_llm(
                                query=state.get("query", ""),
                                error_message=error_message,
                                generated_url=state.get("odata_url", "")
                            )
                            
                            # Add error analysis to state if available
                            if error_analysis:
                                state["error"]["analysis"] = error_analysis
                                
                                # If correction is available, store it for retry
                                if "correction" in error_analysis and error_analysis["correction"]:
                                    state["error"]["corrected_url"] = error_analysis["correction"]
                                    state["error"]["can_retry"] = True
                        
                        logger.info(f"Analyzed and stored error for learning")
                else:
                    # SUCCESS CASE
                    state["response"] = response
                    
                    # If successful and was high-risk, count as prevention success
                    if was_high_risk:
                        self.prevention_stats["successful"] += 1
                        success_rate = self.prevention_stats["successful"] / self.prevention_stats["attempted"]
                        logger.info(f"Prevention success! Rate: {success_rate:.3f}")
                        
                        # Update metadata manager with prevention success
                        if "metadata_manager" in state and state["metadata_manager"]:
                            state["metadata_manager"].update_prevention_success(original_risk_score, True)
                    
                    # Success case - store and learn from successful query
                    if "metadata_manager" in state and state["metadata_manager"]:
                        metadata_manager = state["metadata_manager"]
                        
                        filter_conditions = state.get("structured_query", {}).get("filter_conditions", [])
                        entities = {}
                        for condition in filter_conditions:
                            if isinstance(condition, dict) and "field" in condition and "value" in condition:
                                entities[condition["field"]] = condition["value"]
                        
                        # Store successful query
                        metadata_manager.store_successful_query(
                            intent=state.get("intent", "unknown"),
                            endpoint=state.get("endpoint", "unknown"),
                            entities=entities,
                            query_pattern=state["odata_url"],
                            response_status=200,
                            response_time=execution_time
                        )
                        
                        # Extract and store query pattern
                        metadata_manager.extract_and_store_query_pattern(
                            query_url=state["odata_url"],
                            entity_type=state.get("endpoint", "unknown"),
                            execution_time=execution_time,
                            record_count=len(response.get("value", [])) if isinstance(response, dict) else 0,
                            successful=True
                        )
                        
                        logger.info(f"Learned from successful query: {state['odata_url']}")
                
                return state
                
            except AuthenticationError as e:
                # If failed and was high-risk, count as prevention failure
                if was_high_risk:
                    logger.warning(f"Prevention failed for high-risk query (auth error)")
                    if "metadata_manager" in state and state["metadata_manager"]:
                        state["metadata_manager"].update_prevention_success(original_risk_score, False)
                
                print(f"Authentication error: {str(e)}")
                state["error"] = {
                    "stage": "request_execution",
                    "message": str(e),
                    "details": {"type": "authentication"},
                    "can_retry": True,  # Auth errors can often be retried
                    "user_message": "Authentication error with SAP system. Please check your credentials."
                }
                state["output"] = format_user_friendly_error(state["error"])
                
                # Track error for learning
                if "metadata_manager" in state and state["metadata_manager"]:
                    metadata_manager = state["metadata_manager"]
                    metadata_manager.store_error_example(
                        intent=state.get("intent", "unknown"),
                        endpoint=state.get("endpoint", "unknown"),
                        entities=state.get("structured_query", {}).get("filter_conditions", []),
                        query_pattern=state.get("odata_url", ""),
                        error_message=str(e),
                        error_type="AuthenticationError"
                    )
                
                return state
                
            except SAPConnectionError as e:
                # If failed and was high-risk, count as prevention failure
                if was_high_risk:
                    logger.warning(f"Prevention failed for high-risk query (connection error)")
                    if "metadata_manager" in state and state["metadata_manager"]:
                        state["metadata_manager"].update_prevention_success(original_risk_score, False)
                
                print(f"Connection error: {str(e)}")
                state["error"] = {
                    "stage": "request_execution",
                    "message": str(e),
                    "details": {"type": "connection"},
                    "can_retry": True,  # Connection errors can often be retried
                    "user_message": "Unable to connect to the SAP system. Please check your network or server status."
                }
                state["output"] = format_user_friendly_error(state["error"])
                
                # Track error for learning
                if "metadata_manager" in state and state["metadata_manager"]:
                    metadata_manager = state["metadata_manager"]
                    metadata_manager.store_error_example(
                        intent=state.get("intent", "unknown"),
                        endpoint=state.get("endpoint", "unknown"),
                        entities=state.get("structured_query", {}).get("filter_conditions", []),
                        query_pattern=state.get("odata_url", ""),
                        error_message=str(e),
                        error_type="ConnectionError"
                    )
                
                return state
                
            except RequestError as e:
                # If failed and was high-risk, count as prevention failure
                if was_high_risk:
                    logger.warning(f"Prevention failed for high-risk query (request error)")
                    if "metadata_manager" in state and state["metadata_manager"]:
                        state["metadata_manager"].update_prevention_success(original_risk_score, False)
                
                print(f"Request error: {str(e)}")
                state["error"] = {
                    "stage": "request_execution",
                    "message": str(e),
                    "details": {"type": "request"},
                    "can_retry": False,  # Most request errors cannot be retried
                    "user_message": "There was an issue with the request format. The query may need to be reformulated."
                }
                state["output"] = format_user_friendly_error(state["error"])
                
                # Track error and analyze with LLM
                if "metadata_manager" in state and state["metadata_manager"]:
                    metadata_manager = state["metadata_manager"]
                    metadata_manager.store_error_example(
                        intent=state.get("intent", "unknown"),
                        endpoint=state.get("endpoint", "unknown"),
                        entities=state.get("structured_query", {}).get("filter_conditions", []),
                        query_pattern=state.get("odata_url", ""),
                        error_message=str(e),
                        error_type="RequestError"
                    )
                    
                    # This type of error is particularly good for LLM analysis
                    if hasattr(metadata_manager, "analyze_error_with_llm"):
                        error_analysis = metadata_manager.analyze_error_with_llm(
                            query=state.get("query", ""),
                            error_message=str(e),
                            generated_url=state.get("odata_url", "")
                        )
                        
                        if error_analysis:
                            state["error"]["analysis"] = error_analysis
                            
                            # If correction is available, store it for retry
                            if "correction" in error_analysis and error_analysis["correction"]:
                                state["error"]["corrected_url"] = error_analysis["correction"]
                                state["error"]["can_retry"] = True
                
                return state
                
            except SapODataError as e:
                # If failed and was high-risk, count as prevention failure
                if was_high_risk:
                    logger.warning(f"Prevention failed for high-risk query (OData error)")
                    if "metadata_manager" in state and state["metadata_manager"]:
                        state["metadata_manager"].update_prevention_success(original_risk_score, False)
                
                print(f"SAP API error: {str(e)}")
                state["error"] = {
                    "stage": "request_execution",
                    "message": str(e),
                    "details": {"type": "api"},
                    "can_retry": False,
                    "user_message": "The SAP system reported an error processing your request."
                }
                state["output"] = format_user_friendly_error(state["error"])
                
                # Track error for learning
                if "metadata_manager" in state and state["metadata_manager"]:
                    metadata_manager = state["metadata_manager"]
                    metadata_manager.store_error_example(
                        intent=state.get("intent", "unknown"),
                        endpoint=state.get("endpoint", "unknown"),
                        entities=state.get("structured_query", {}).get("filter_conditions", []),
                        query_pattern=state.get("odata_url", ""),
                        error_message=str(e),
                        error_type="SapODataError"
                    )
                
                return state
                
        except Exception as e:
            # If failed and was high-risk, count as prevention failure
            was_high_risk = "proactive_intervention" in state
            if was_high_risk:
                original_risk_score = state["proactive_intervention"]["risk_score"]
                logger.warning(f"Prevention failed for high-risk query (unexpected error)")
                if "metadata_manager" in state and state["metadata_manager"]:
                    state["metadata_manager"].update_prevention_success(original_risk_score, False)
            
            print(f"Error in request executor: {str(e)}")
            state["error"] = {
                "stage": "request_execution",
                "message": str(e),
                "details": {"type": "unexpected"},
                "can_retry": False,
                "user_message": "An unexpected error occurred while processing your request."
            }
            state["output"] = format_user_friendly_error(state["error"])
            
            # Track unexpected errors too
            if "metadata_manager" in state and state["metadata_manager"]:
                try:
                    metadata_manager = state["metadata_manager"]
                    metadata_manager.store_error_example(
                        intent=state.get("intent", "unknown"),
                        endpoint=state.get("endpoint", "unknown"),
                        entities=state.get("structured_query", {}).get("filter_conditions", []),
                        query_pattern=state.get("odata_url", ""),
                        error_message=str(e),
                        error_type="UnexpectedError"
                    )
                except Exception as logging_error:
                    print(f"Error while logging error: {str(logging_error)}")
            
            return state