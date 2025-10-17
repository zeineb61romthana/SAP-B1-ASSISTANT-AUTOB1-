# graph/enhanced_workflow.py

import os
from typing import Dict, Any, TypedDict, Optional, List, Union
from typing_extensions import TypedDict
from langchain.schema import BaseMessage
from langgraph.graph import StateGraph, END
import asyncio
from agents.query_understanding import QueryUnderstandingAgent
from agents.result_formatting import ResultFormattingAgent
from tools.query_orchestrator import QueryOrchestratorTool
from tools.odata_constructor import ODataConstructorTool
from tools.parameter_handler import ParameterHandlerTool
from tools.request_executor import RequestExecutorTool
from integration.enhanced_sap_client import SAPB1EnhancedClient
from integration.entity_registry_integration import EntityRegistryIntegration
from metadata.manager import MetadataManager
from utils.exceptions import format_user_friendly_error
# Import the enhanced error utilities
from utils.enhanced_errors import (
    SAPAssistantError, 
    format_error_for_response
)
from agents.gmail_invoice_agent import GmailInvoiceProcessingAgent
from tools.gmail_integration import GmailIntegrationTool, GmailMessage
from tools.sap_business_tools import SAPBusinessTools
from tools.support_tools import SupportToolsIntegration

import logging
import time
import re
logger = logging.getLogger(__name__)

# Define the state schema using TypedDict
class EnhancedSAPWorkflowState(TypedDict, total=False):
    query: str                     # Natural language query from user
    intent: str                    # Extracted intent (e.g., "BusinessPartners.FindCustomer")
    endpoint: str                  # SAP module/endpoint (e.g., "BusinessPartners")
    structured_query: Dict[str, Any]  # Structured representation of the query
    odata_url: str                 # Constructed OData URL
    parameters: Dict[str, Any]     # Request parameters
    response: Dict[str, Any]       # API response
    error: Dict[str, Any]          # Error information if any
    output: str                    # Formatted output for user
    output_format: str             # Desired output format (table, json, csv)
    retry_count: int               # Retry counter to prevent infinite loops
    metadata_manager: Any          # Reference to the metadata manager
    sap_client: Any                # Reference to the SAP client
    entity_registry: Any           # Reference to the entity registry

class EnhancedSAPDataWorkflow:
    
    def __init__(self):
        # Initialize core components
        self.metadata_manager = MetadataManager()
        self.sap_client = SAPB1EnhancedClient()
        self.entity_registry = EntityRegistryIntegration(self.sap_client)
        self.intent_recognition_manager = None  # Placeholder for intent recognition manager
        self.query_count = 0
        self.last_pattern_analysis = time.time()
        self.pattern_analysis_interval = 3600  # 1 hour in seconds
        
        # Set flag to track initialization
        self._initialized = False
        

        # Initialize basic tools that don't depend on entity registry
        self.odata_tool = ODataConstructorTool(entity_registry=self.entity_registry)
        self.param_tool = ParameterHandlerTool()
        self.request_tool = RequestExecutorTool(sap_client=self.sap_client)
        self.format_agent = ResultFormattingAgent()
        
        # Initialize Gmail components with LLM-only approach
        try:
            # Get OpenAI API key from environment
            openai_api_key = os.getenv("OPENAI_API_KEY")
            
            if not openai_api_key:
                logger.warning("OPENAI_API_KEY not found in environment - Gmail integration will not be available")
                raise ValueError("OpenAI API key required for Gmail integration")
            
            self.gmail_agent = GmailInvoiceProcessingAgent(
                sap_client=self.sap_client,
                entity_registry=self.entity_registry,
                openai_api_key=openai_api_key
            )
            self.gmail_tool = GmailIntegrationTool(openai_api_key=openai_api_key)
            self.sap_business_tools = SAPBusinessTools(self.sap_client, self.entity_registry)
            self.support_tools = SupportToolsIntegration()
            logger.info("Gmail integration with LLM-only classification initialized successfully")
        except Exception as e:
            logger.warning(f"Gmail integration not available: {str(e)}")
            self.gmail_agent = None
            self.gmail_tool = None
            self.sap_business_tools = None
            self.support_tools = None
        
        # Build the workflow graph - will be populated during initialization
        self.workflow = self._build_graph().compile()
    
    async def initialize(self):
        """Initialize all components asynchronously"""
        if self._initialized:
            return
            
        try:
            print("Initializing entity registry...")
            await self.entity_registry.initialize()
            print(f"Entity registry initialized with {len(await self.entity_registry.get_all_entity_types())} entity types")
            
            # Initialize components that depend on entity registry
            self.query_agent = QueryUnderstandingAgent(entity_registry_integration=self.entity_registry)
            self.query_orchestrator = QueryOrchestratorTool(
                entity_registry_integration=self.entity_registry
            )
            
            # Initialize the enhanced intent recognition manager
            print("Initializing enhanced intent recognition manager...")
            from agents.intent_recognition_manager import IntentRecognitionManager
            
            self.intent_recognition_manager = IntentRecognitionManager(
                entity_registry=self.entity_registry,
                sap_client=self.sap_client,
                llm=None,  # We'll use the LLM from query_orchestrator
                distilbert_model_path="sap_intent_model_trained"  # Adjust path as needed
            )
            
            print("Enhanced intent recognition manager initialized")
            
            # Update query orchestrator to use the intent recognition manager
            if hasattr(self.query_orchestrator, 'intent_recognition_manager'):
                self.query_orchestrator.intent_recognition_manager = self.intent_recognition_manager
            
            # Initialize zero-shot recognizer and metadata intent generator
            if hasattr(self.query_orchestrator, 'zero_shot_recognizer'):
                print("Initializing zero-shot intent recognizer...")
                # No explicit initialization needed
                
            if hasattr(self.query_orchestrator, 'metadata_intent_generator'):
                print("Initializing metadata-based intent generator...")
                # Provide SAP client to metadata generator
                if self.query_orchestrator.metadata_intent_generator:
                    self.query_orchestrator.metadata_intent_generator.sap_client = self.sap_client
                    try:
                        await self.query_orchestrator.metadata_intent_generator.initialize()
                    except Exception as e:
                        print(f"Warning: Metadata intent generator initialization failed: {str(e)}")
            
            # Mark as initialized
            self._initialized = True
            
        except Exception as e:
            print(f"Error during initialization: {str(e)}")
            print("Continuing with limited functionality")
            
            # Initialize with empty/minimal implementations even after error
            self.query_agent = QueryUnderstandingAgent(entity_registry_integration=self.entity_registry)
            self.query_orchestrator = QueryOrchestratorTool(
                entity_registry_integration=self.entity_registry
            )
            
            # Mark as initialized even after error to avoid repeated initialization attempts
            self._initialized = True
    
    def ensure_initialized(self):
        """Ensure the workflow is initialized before use"""
        if not self._initialized:
            # Use asyncio.run to run the async initialize method in a sync context
            asyncio.run(self.initialize())
    
    def _build_graph(self) -> StateGraph:
        # Initialize the graph with the TypedDict
        builder = StateGraph(EnhancedSAPWorkflowState)
        
        # Add nodes to the graph
        builder.add_node("extract_intent", self._extract_intent)
        builder.add_node("understand_query", self._understand_query)
        builder.add_node("orchestrate_query", self._orchestrate_query)
        builder.add_node("construct_odata", self.odata_tool.invoke)
        builder.add_node("handle_parameters", self.param_tool.invoke)
        builder.add_node("execute_request", self.request_tool.invoke)
        builder.add_node("format_result", self.format_agent.invoke)
        
        # Define the edges (workflow)
        builder.add_edge("extract_intent", "understand_query")
        builder.add_edge("understand_query", "orchestrate_query")
        
        
        # Conditional edge from orchestrate_query
        builder.add_conditional_edges(
            "orchestrate_query",
            self._check_orchestration_result,
            {
                "use_odata_constructor": "construct_odata",
                "direct_execution": "handle_parameters"  # Go directly to handle_parameters
            }
        )

        builder.add_edge("construct_odata", "handle_parameters")  # Go directly to handle_parameters
        builder.add_edge("handle_parameters", "execute_request")
        
        # Add conditional edges with retry limiting
        builder.add_conditional_edges(
            "execute_request",
            self._check_request_status,
            {
                "success": "format_result",
                "error": "format_result"  # Go directly to formatting on error
            }
        )
        
        builder.add_edge("format_result", END)
        
        # Set the entry point
        builder.set_entry_point("extract_intent")
        
        return builder
    
    
    def _extract_intent(self, state: EnhancedSAPWorkflowState) -> EnhancedSAPWorkflowState:
        """Simplified intent extraction using the new 2-method approach."""
        try:
            # Add shared objects to state
            state["metadata_manager"] = self.metadata_manager
            state["entity_registry"] = self.entity_registry
            
            # Use the simplified intent recognition manager
            if self.intent_recognition_manager:
                logger.info("Using simplified intent recognition...")
                import asyncio
                
                intent_result = asyncio.run(
                    self.intent_recognition_manager.recognize_intent(state["query"])
                )
                
                # Extract intent information
                state["intent"] = intent_result.get("intent", "unknown")
                state["confidence"] = intent_result.get("confidence", 0.5)
                
                # Set endpoint based on intent
                if "." in state["intent"]:
                    state["endpoint"] = state["intent"].split(".")[0]
                else:
                    state["endpoint"] = "unknown"
                
                # Log results
                method_used = intent_result.get("method_used", "unknown")
                logger.info(f"Intent: {state['intent']} | Method: {method_used} | Confidence: {state['confidence']:.3f}")
                
            else:
                # Fallback if manager not available
                logger.warning("Intent recognition manager not available, using fallback")
                state["intent"] = "unknown"
                state["endpoint"] = "unknown"
                state["confidence"] = 0.5
            
            # Initialize structured query
            if "structured_query" not in state:
                state["structured_query"] = {
                    "entity_type": state.get("endpoint", ""),
                    "filter_conditions": [],
                    "fields": [],
                    "top": 50,
                    "skip": 0,
                    "order_by": "",
                    "expand": []
                }
            
            return state
            
        except Exception as e:
            logger.error(f"Error in intent extraction: {str(e)}")
            state["error"] = {
                "stage": "intent_extraction",
                "message": str(e),
                "can_retry": False
            }
            state["output"] = f"Error understanding your query: {str(e)}"
            return state
    
    # get intent recognition statistics
    def get_intent_recognition_stats(self) -> Dict[str, Any]:
        """Get intent recognition statistics for monitoring."""
        if self.intent_recognition_manager:
            return self.intent_recognition_manager.get_usage_statistics()
        return {"error": "Intent recognition manager not available"}
    
    # compare intent recognition methods
    def compare_intent_methods(self, query: str) -> Dict[str, Any]:
        """Compare all intent recognition methods on a specific query."""
        if self.intent_recognition_manager:
            import asyncio
            return asyncio.run(
                self.intent_recognition_manager.recognize_intent(
                    query, 
                    strategy="adaptive", 
                    compare_methods=True
                )
            )
        return {"error": "Intent recognition manager not available"}
    
    def _understand_query(self, state: EnhancedSAPWorkflowState) -> EnhancedSAPWorkflowState:
        """Process the query through the query understanding agent with entity registry enhancement"""
        try:
            # Run the query understanding agent
            result = asyncio.run(self.query_agent.async_invoke(state))
            
            # NEW: Track learning progress
            if "method_used" in result and result["method_used"] == "dynamic_few_shot_llm":
                learning_stats = self.query_agent.get_learning_stats()
                print(f"Learning Stats: {learning_stats['total_examples']} examples, "
                    f"avg confidence: {learning_stats['avg_confidence']:.3f}")
            
            # Ensure date/time expressions are properly handled
            from utils.dynamic_time_resolver import extract_time_expressions
            time_entities = extract_time_expressions(result["query"])
            
            # If time expressions were found, update filter conditions
            if time_entities and "DocDate" in time_entities and "structured_query" in result:
                date_range = time_entities["DocDate"]
                
                # Remove any existing DocDate conditions
                filter_conditions = result["structured_query"].get("filter_conditions", [])
                filter_conditions = [c for c in filter_conditions if c.get("field") != "DocDate"]
                
                # Add the new date conditions
                if "range" in date_range and date_range["range"] != "exact":
                    filter_conditions.append({
                        "field": "DocDate",
                        "operator": "ge",
                        "value": date_range["start"]
                    })
                    filter_conditions.append({
                        "field": "DocDate",
                        "operator": "le",
                        "value": date_range["end"]
                    })
                else:
                    filter_conditions.append({
                        "field": "DocDate",
                        "operator": "eq",
                        "value": date_range["start"]
                    })
                
                # Update the structured query
                result["structured_query"]["filter_conditions"] = filter_conditions
            
            # Check if we need to enhance the structured query with the entity registry
            if self.entity_registry and "structured_query" in result:
                # Enrich the structured query with additional entity information
                result["structured_query"] = self.entity_registry.enrich_structured_query(
                    result["structured_query"], result["query"]
                )
            
            return result
        except Exception as e:
            print(f"Error in query understanding: {str(e)}")
            if "error" not in state:
                state["error"] = {
                    "stage": "query_understanding",
                    "message": str(e),
                    "can_retry": False
                }
                state["output"] = f"Error understanding your query: {str(e)}"
            return state
    
    def _orchestrate_query(self, state: EnhancedSAPWorkflowState) -> EnhancedSAPWorkflowState:
        """Process the query through the query orchestrator with entity registry enhancement"""
        try:
            # Run the query orchestrator
            result = asyncio.run(self.query_orchestrator.async_invoke(state))
            return result
        except Exception as e:
            print(f"Error in query orchestration: {str(e)}")
            if "error" not in state:
                state["error"] = {
                    "stage": "query_orchestration",
                    "message": str(e),
                    "can_retry": False
                }
                state["output"] = f"Error orchestrating your query: {str(e)}"
            return state
    
    def _check_orchestration_result(self, state: EnhancedSAPWorkflowState) -> str:
        """Determine next step based on orchestration result"""
        if "odata_url" in state and state["odata_url"]:
            # Query orchestrator successfully created URL, skip OData constructor
            return "direct_execution"
        else:
            # Need to use OData constructor as fallback
            return "use_odata_constructor"
    
    def _check_request_status(self, state: EnhancedSAPWorkflowState) -> str:
        """Determine next step based on request execution status (simplified)"""
        if "error" in state and state["error"]:
            # Simple error handling - no complex recovery
            print(f"Error detected: {state['error'].get('message', 'Unknown error')}")
            return "error"
        return "success"
            
    
    def process_gmail_invoice_request(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a Gmail message for invoice requests"""
        if not self.gmail_agent:
            return {
                "status": "error",
                "message": "Gmail integration not available. Please set OPENAI_API_KEY environment variable."
            }
        
        try:
            result = self.gmail_agent.process_single_message(message_data)
            return result
        except Exception as e:
            logger.error(f"Error processing Gmail message: {str(e)}")
            return {
                "status": "error",
                "message": f"Error processing Gmail message: {str(e)}"
            }
    
    def get_gmail_messages(self, query: str = "is:unread") -> Dict[str, Any]:
        """Get messages from Gmail"""
        if not self.gmail_tool:
            return {
                "status": "error",
                "message": "Gmail integration not available. Please set OPENAI_API_KEY environment variable."
            }
        
        try:
            messages = self.gmail_tool.get_messages(query)
            return {
                "status": "success",
                "message_count": len(messages),
                "messages": [
                    {
                        "message_id": msg.message_id,
                        "sender": msg.sender,
                        "subject": msg.subject,
                        "body": msg.body,
                        "received_at": msg.received_at.isoformat(),
                        "is_invoice_request": self.gmail_tool.is_invoice_request(msg)
                    }
                    for msg in messages
                ]
            }
        except Exception as e:
            logger.error(f"Error getting Gmail messages: {str(e)}")
            return {
                "status": "error", 
                "message": f"Error getting Gmail messages: {str(e)}"
            }
    
    def debug_gmail_classification(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """Debug method to test email classification"""
        if not self.gmail_tool:
            return {
                "status": "error",
                "message": "Gmail integration not available"
            }
        
        try:
            message = GmailMessage(
                message_id="debug",
                sender=message_data.get("sender", ""),
                subject=message_data.get("subject", ""),
                body=message_data.get("body", ""),
                received_at=datetime.now(),
                thread_id="debug"
            )
            
            debug_result = self.gmail_tool.debug_classification(message)
            return {
                "status": "success",
                "debug_info": debug_result
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Debug failed: {str(e)}"
            }
    
    def start_gmail_monitoring(self, check_interval: int = 60):
        """Start continuous Gmail monitoring (async)"""
        if not self.gmail_agent:
            return {
                "status": "error",
                "message": "Gmail integration not available. Please set OPENAI_API_KEY environment variable."
            }
        
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            logger.info(f"Starting Gmail monitoring with {check_interval} second intervals")
            loop.run_until_complete(
                self.gmail_agent.monitor_gmail_continuously(check_interval)
            )
            
        except KeyboardInterrupt:
            logger.info("Gmail monitoring stopped by user")
            return {"status": "stopped", "message": "Gmail monitoring stopped"}
        except Exception as e:
            logger.error(f"Error in Gmail monitoring: {str(e)}")
            return {"status": "error", "message": f"Error in Gmail monitoring: {str(e)}"}
    
    def lookup_sap_business_partner(self, email_address: str) -> Dict[str, Any]:
        """Look up business partner by email in SAP B1"""
        if not self.sap_business_tools:
            return {"status": "error", "message": "SAP business tools not available"}
        
        try:
            result = self.sap_business_tools.get_business_partner_from_mail(email_address)
            return result
        except Exception as e:
            logger.error(f"Error looking up business partner: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def get_customer_latest_order(self, email_address: str) -> Dict[str, Any]:
        """Get latest order for customer by email"""
        if not self.sap_business_tools:
            return {"status": "error", "message": "SAP business tools not available"}
        
        try:
            result = self.sap_business_tools.get_latest_order_for_business_partner(email_address)
            return result
        except Exception as e:
            logger.error(f"Error getting latest order: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def generate_invoice_report(self, invoice_id: str) -> Dict[str, Any]:
        """Generate Crystal Report for invoice"""
        if not self.support_tools:
            return {"status": "error", "message": "Support tools not available"}
        
        try:
            # Get invoice data first
            if self.sap_business_tools:
                invoice_data = self.sap_business_tools.get_invoice_by_id(invoice_id)
                if invoice_data["status"] == "found":
                    # Generate report with real data
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        result = loop.run_until_complete(
                            self.support_tools.generate_invoice_report(invoice_data)
                        )
                        return result
                    finally:
                        loop.close()
                else:
                    return invoice_data
            else:
                return {"status": "error", "message": "Cannot access SAP data"}
        except Exception as e:
            logger.error(f"Error generating invoice report: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def _maybe_trigger_pattern_analysis(self):
        """Trigger pattern analysis periodically or after certain number of queries"""
        self.query_count += 1
        current_time = time.time()
        
        # Trigger analysis every 20 queries OR every hour
        should_analyze = (
            self.query_count % 20 == 0 or 
            (current_time - self.last_pattern_analysis) > self.pattern_analysis_interval
        )
        
        if should_analyze and self.metadata_manager:
            logger.info("Triggering background pattern analysis...")
            self._perform_pattern_analysis()
            self.last_pattern_analysis = current_time

    def _perform_pattern_analysis(self):
        """Perform background analysis of error patterns"""
        try:
            patterns = self.metadata_manager.detect_recurring_error_patterns()
            
            if patterns:
                logger.info(f"Found {len(patterns)} recurring error patterns:")
                for pattern in patterns[:5]:  # Log top 5
                    logger.info(f"  - {pattern['error_type']}: {pattern['frequency']} occurrences")
                
                # Update dynamic correction rules in request executor
                self._update_dynamic_corrections(patterns)
            else:
                logger.info("No recurring error patterns detected")
                
        except Exception as e:
            logger.error(f"Error in pattern analysis: {str(e)}")

    def _update_dynamic_corrections(self, patterns):
        """Update the request executor with new dynamic correction rules"""
        if hasattr(self, 'request_tool') and hasattr(self.request_tool, 'add_dynamic_corrections'):
            dynamic_rules = {}
            
            for pattern in patterns:
                error_msg = pattern['error_message']
                suggested_fix = pattern['suggested_fix']
                
                # Create correction lambda based on the pattern
                if "DocStatus" in error_msg:
                    dynamic_rules[error_msg] = lambda url: url.replace("DocStatus", "DocumentStatus")
                elif "single quote" in error_msg.lower():
                    dynamic_rules[error_msg] = lambda url: re.sub(r"([^'])('(?:[^']|'')*')([^'])", r"\1\2\3", url)
            
            self.request_tool.add_dynamic_corrections(dynamic_rules)
            logger.info(f"Updated {len(dynamic_rules)} dynamic correction rules")
    
    
    def invoke(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the workflow with the given inputs (enhanced with Gmail support and pattern analysis)"""

        # Check if this is a Gmail-related request
        if "gmail_action" in inputs:
            gmail_action = inputs["gmail_action"]

            if gmail_action == "process_message":
                message_data = inputs.get("message_data", {})
                return self.process_gmail_invoice_request(message_data)

            elif gmail_action == "get_messages":
                query = inputs.get("query", "is:unread")
                return self.get_gmail_messages(query)

            elif gmail_action == "debug_classification":
                message_data = inputs.get("message_data", {})
                return self.debug_gmail_classification(message_data)

            elif gmail_action == "monitor":
                interval = inputs.get("check_interval", 60)
                return self.start_gmail_monitoring(interval)

            elif gmail_action == "lookup_partner":
                email = inputs.get("email_address", "")
                return self.lookup_sap_business_partner(email)

            elif gmail_action == "get_latest_order":
                email = inputs.get("email_address", "")
                return self.get_customer_latest_order(email)

            elif gmail_action == "generate_report":
                invoice_id = inputs.get("invoice_id", "")
                return self.generate_invoice_report(invoice_id)

        # NEW: Trigger pattern analysis before processing SAP workflow
        self._maybe_trigger_pattern_analysis()

        # Ensure initialization before processing SAP workflow
        self.ensure_initialized()

        # Prepare initial state with retry counter and common objects
        initial_state: EnhancedSAPWorkflowState = {
            "query": inputs.get("query", ""),
            "output_format": inputs.get("output_format", "table"),
            "retry_count": 0,
            "metadata_manager": self.metadata_manager,
            "sap_client": self.sap_client,
            "entity_registry": self.entity_registry
        }

        # Execute the SAP workflow with the given inputs
        try:
            print(f"Starting enhanced workflow with query: {initial_state['query']}")
            result = self.workflow.invoke(initial_state)
            print("Workflow completed successfully")
            return result
        except Exception as e:
            print(f"Workflow execution error: {str(e)}")
            # Return a graceful error message if the workflow fails
            return {
                "output": f"Error processing your query: {str(e)}\nPlease try a different query or contact support."
            }