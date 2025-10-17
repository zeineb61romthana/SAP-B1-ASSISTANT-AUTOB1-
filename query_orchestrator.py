# tools/query_orchestrator.py

import json
import logging
import traceback
import asyncio
from typing import Dict, Any, List, Optional
import requests
import re
from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain.schema import StrOutputParser

logger = logging.getLogger("QueryOrchestrator")

class QueryOrchestratorTool:
    
    
    def __init__(self, openai_api_key=None, entity_registry_integration=None):
        self.api_key = openai_api_key
        self.entity_registry = entity_registry_integration
        
        # Common query patterns for direct matching
        self.query_patterns = {
            "Orders.FindSpecificOrder": "/Orders?$filter=DocNum eq {{DocNum}}",
            "Orders.FindOrdersByCustomer": "/Orders?$filter=CardCode eq '{{CardCode}}' or CardName eq '{{CardName}}'",
            "Invoices.FindInvoice": "/Invoices?$filter=DocNum eq {{DocNum}}",
            "BusinessPartners.FindCustomer": "/BusinessPartners?$filter=CardType eq 'C' and CardName eq '{{CardName}}'",
            "Items.FindItem": "/Items?$filter=ItemCode eq '{{ItemCode}}' or ItemName eq '{{ItemName}}'",
            "Orders.FindByDateRange": "/Orders?$filter=DocDate ge '{{StartDate}}' and DocDate le '{{EndDate}}'",
            "Orders.FindByExactDate": "/Orders?$filter=DocDate eq '{{DocDate}}'",
            "Invoices.FindByLastMonth": "/Invoices?$filter=DocDate ge '{{StartDate}}' and DocDate le '{{EndDate}}'",
            "Documents.FindByQuarter": "/{{EntityType}}?$filter=DocDate ge '{{StartDate}}' and DocDate le '{{EndDate}}'",
            "Documents.FindByYear": "/{{EntityType}}?$filter=DocDate ge '{{Year}}-01-01' and DocDate le '{{Year}}-12-31'"
        }
        
        # Initialize the LLM
        try:
            self.llm = ChatOpenAI(model="gpt-4-turbo", temperature=0.2)
        except Exception:
            try:
                self.llm = ChatOpenAI(model="gpt-4", temperature=0.2)
            except Exception:
                # Fallback to GPT-3.5-turbo
                self.llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.2) 
        # Update the query patterns dictionary with more comprehensive patterns
        self.query_patterns.update({
            "Orders.FindOpenOrders": "/Orders?$filter=DocumentStatus eq 'bost_Open'",
            "BusinessPartners.ListVendors": "/BusinessPartners?$filter=CardType eq 'S'",
            "Items.FindByStockLevel": "/Items?$filter=QuantityOnStock lt {{QuantityOnStock}}",
            "Items.FindActive": "/Items?$filter=Active eq true",
            "Invoices.FindUnpaid": "/Invoices?$filter=Paid eq 'tNO'",
            "Invoices.ListByDateRange": "/Invoices?$filter=DocDate ge '{{StartDate}}' and DocDate le '{{EndDate}}'",
            "PurchaseOrders.FindByVendor": "/PurchaseOrders?$filter=CardCode eq '{{Car0.dCode}}'"
        })
    
    async def _get_entity_schema_for_prompt(self, entity_type: str) -> str:
        """Get entity schema information formatted for inclusion in a prompt"""
        if not self.entity_registry or not entity_type:
            return ""
        
        try:
            schema = await self.entity_registry.get_entity_schema(entity_type)
            
            # Format schema info for prompt
            info_parts = []
            
            # Add key fields
            key_fields = schema.get("key_fields", [])
            if key_fields:
                info_parts.append(f"Key fields: {', '.join(key_fields)}")
            
            # Add descriptive field if available
            descriptive_field = schema.get("descriptive_field")
            if descriptive_field:
                info_parts.append(f"Descriptive field: {descriptive_field}")
            
            # Add properties
            properties = schema.get("properties", [])
            prop_list = []
            
            # Handle different schema formats
            if isinstance(properties, list):
                if all(isinstance(prop, str) for prop in properties):
                    # Simple list of property names
                    prop_list = properties
                elif all(isinstance(prop, dict) for prop in properties):
                    # List of property objects
                    for prop in properties:
                        if "name" in prop:
                            prop_list.append(f"{prop['name']} ({prop.get('type', 'string')})")
            elif isinstance(properties, dict):
                # Dictionary of properties
                for prop_name, prop_value in properties.items():
                    prop_list.append(prop_name)
            
            if prop_list:
                info_parts.append(f"Properties: {', '.join(prop_list)}")
            
            # Add common filters if available
            common_filters = schema.get("common_filters", {})
            if common_filters:
                filter_parts = []
                for filter_name, filter_info in common_filters.items():
                    field = filter_info.get("field", "")
                    value = filter_info.get("value", "")
                    if field and value:
                        filter_parts.append(f"{filter_name}: {field} = '{value}'")
                
                if filter_parts:
                    info_parts.append(f"Common filters: {'; '.join(filter_parts)}")
            
            return "\n".join(info_parts)
        except Exception as e:
            logger.error(f"Error getting entity schema for prompt: {str(e)}")
            return ""
    
    def _construct_system_prompt(self, entity_type: str, entity_schema: str, metadata: Dict[str, Any], examples: List[Dict[str, Any]]) -> str:
        """Create a detailed system prompt with entity schema, metadata and examples"""
        
        # Build metadata section
        metadata_str = "Available Entity Types and Properties:\n"
        for entity_name, properties in metadata.items():
            metadata_str += f"- {entity_name}: {', '.join(properties)}\n"
        
        # Build examples section
        examples_str = "Examples of successful queries:\n"
        for i, example in enumerate(examples):
            examples_str += f"Example {i+1}:\n"
            examples_str += f"Intent: {example['intent']}\n"
            examples_str += f"Endpoint: {example.get('endpoint', '')}\n"
            examples_str += f"Entities: {json.dumps(example.get('entities', {}))}\n"
            examples_str += f"Query Pattern: {example['query_pattern']}\n\n"
        
        # Build entity schema section
        entity_schema_str = f"Entity Schema for {entity_type}:\n{entity_schema}\n"
        
        system_prompt = f"""
    You are an SAP B1 OData query construction expert. Your task is to generate precise OData query URLs based on user intents and entities.

    Follow these rules:
    1. Use the correct entity type based on the intent and endpoint
    2. Apply proper filter expressions for the provided entities
    3. Include relevant $select, $expand, or $orderby parameters when appropriate
    4. Format date values in ISO format (YYYY-MM-DD)
    5. String values must be enclosed in single quotes
    6. Numeric values should not have quotes
    7. The base URL path is already handled - start your URL with a forward slash followed by the entity type

    For filter conditions, use the following format:
    {{{{\"field\": \"FieldName\", \"operator\": \"eq\", \"value\": \"Value\"}}}}

    EXAMPLE: For a query "Get customer XYZ", use filter_conditions:
    [{{{{\"field\": \"CardCode\", \"operator\": \"eq\", \"value\": \"XYZ\"}}}}, {{{{\"field\": \"CardType\", \"operator\": \"eq\", \"value\": \"C\"}}}}]

    {entity_schema_str}

    {metadata_str}

    {examples_str}

    Provide your response as a JSON object with these fields:
    - url: The complete OData query path (without the base URL)
    - method: The HTTP method to use (GET, POST, PATCH, etc.)
    - reasoning: Your step-by-step reasoning for constructing this query
    - confidence: Your confidence score (0.0 to 1.0)
    """
        return system_prompt
    
    def _construct_user_prompt(self, intent: str, structured_query: Dict[str, Any]) -> str:
        """Create a user prompt with current request details"""
        # Extract relevant information from the structured query
        entity_type = structured_query.get('entity_type', '')
        filter_conditions = structured_query.get('filter_conditions', [])
        fields = structured_query.get('fields', [])
        
        # Create a more detailed user prompt
        user_prompt = f"""
Generate an OData query for the following request:

Intent: {intent}
Entity Type: {entity_type}
Filter Conditions: {json.dumps(filter_conditions, indent=2)}
Fields: {json.dumps(fields, indent=2)}
Additional Parameters: 
- Top: {structured_query.get('top', 50)}
- Skip: {structured_query.get('skip', 0)}
- Order By: {structured_query.get('order_by', '')}
- Expand: {json.dumps(structured_query.get('expand', []))}

Remember to:
1. Use the entity type provided
2. Include proper filter conditions based on the filter_conditions list
3. Handle string values (add quotes) and numeric values (no quotes) correctly
4. Use proper date formatting if date entities are provided
5. Start your URL with / followed by the entity type
"""
        return user_prompt
    
    def _clean_template_pattern(self, pattern):
        """
        Clean and normalize template patterns to prevent variable handling issues.
        Converts all formats to the standard {{variable}} format.
        """
        import re
        
        # First identify all variable patterns
        variable_pattern = r'\{\{([\s\n]*["\']?([^{}"\'\s\n]+)["\']?[\s\n]*)\}\}'
        matches = re.findall(variable_pattern, pattern)
        
        # Replace each match with the standardized version
        clean_pattern = pattern
        for full_match, var_name in matches:
            # Create replacement with standard format
            replacement = '{{' + var_name + '}}'
            # Replace the original with standardized version
            original = '{{' + full_match + '}}'
            clean_pattern = clean_pattern.replace(original, replacement)
        
        return clean_pattern
    
    def _get_time_specific_template(self, intent: str, time_entities: Dict[str, Any]) -> Optional[str]:
        """Get a time-specific URL template based on the intent and time entities."""
        entity_type = intent.split('.')[0] if '.' in intent else None
        
        if not entity_type or not time_entities:
            return None
            
        doc_date = time_entities.get("DocDate", {})
        
        if not doc_date:
            return None
            
        range_type = doc_date.get("range", "")
        
        # Select template based on range type
        if range_type == "exact":
            return f"/{entity_type}?$filter=DocDate eq '{{{{DocDate}}}}'"
        elif "month" in range_type:
            return f"/{entity_type}?$filter=DocDate ge '{{{{StartDate}}}}' and DocDate le '{{{{EndDate}}}}'"
        elif "quarter" in range_type:
            return f"/{entity_type}?$filter=DocDate ge '{{{{StartDate}}}}' and DocDate le '{{{{EndDate}}}}'"
        elif "year" in range_type:
            return f"/{entity_type}?$filter=DocDate ge '{{{{StartDate}}}}' and DocDate le '{{{{EndDate}}}}'"
        elif "day" in range_type:
            if "start" in doc_date and "end" in doc_date and doc_date["start"] == doc_date["end"]:
                return f"/{entity_type}?$filter=DocDate eq '{{{{DocDate}}}}'"
            else:
                return f"/{entity_type}?$filter=DocDate ge '{{{{StartDate}}}}' and DocDate le '{{{{EndDate}}}}'"
        else:
            # Custom range
            return f"/{entity_type}?$filter=DocDate ge '{{{{StartDate}}}}' and DocDate le '{{{{EndDate}}}}'"
    
    def validate_template_variables(self, pattern, entities):
        """
        Extract required variables from a template and validate that they're present in entities.
        Handles all formats of template variables including those with newlines and whitespace.
        
        Args:
            pattern: The template pattern string
            entities: Dictionary of available entity values
            
        Returns:
            bool: True if all required variables are present or can be inferred, False otherwise
            dict: Updated entities with inferred values if applicable
        """
        # Extract variable names from the pattern with enhanced regex that handles all formats
        import re
        
        # This regex will match variables in all formats:
        # {{var}}, {{ var }}, {{\n    "var"\n}}, etc.
        variable_pattern = r'\{\{([\s\n]*["\']?([^{}"\'\s\n]+)["\']?[\s\n]*)\}\}'
        matches = re.findall(variable_pattern, pattern)
        
        # Extract the actual variable names (second group in each match)
        required_vars = [match[1] for match in matches]
        
        # Make a copy of entities to avoid modifying the original
        updated_entities = entities.copy()
        
        # Log the required variables for debugging
        logger.info(f"Required variables for template: {required_vars}")
        logger.info(f"Available entities: {updated_entities.keys()}")
        
        # Infer missing variables if possible
        missing_vars = []
        for var in required_vars:
            if var not in updated_entities:
                # Handle common variable inference
                if var == 'CardName' and 'CardCode' in updated_entities:
                    # If we need CardName but have CardCode, use that as fallback
                    updated_entities['CardName'] = updated_entities['CardCode']
                    logger.info(f"Inferred CardName from CardCode: {updated_entities['CardCode']}")
                elif var == 'CardCode' and 'CardName' in updated_entities:
                    # If we need CardCode but have CardName, use that as fallback
                    updated_entities['CardCode'] = updated_entities['CardName']
                    logger.info(f"Inferred CardCode from CardName: {updated_entities['CardName']}")
                elif var == 'DocNum' and 'DocEntry' in updated_entities:
                    # If we need DocNum but have DocEntry, use that as fallback
                    updated_entities['DocNum'] = updated_entities['DocEntry']
                    logger.info(f"Inferred DocNum from DocEntry: {updated_entities['DocEntry']}")
                elif var == 'DocEntry' and 'DocNum' in updated_entities:
                    # If we need DocEntry but have DocNum, use that as fallback
                    updated_entities['DocEntry'] = updated_entities['DocNum']
                    logger.info(f"Inferred DocEntry from DocNum: {updated_entities['DocNum']}")
                elif var == 'ItemName' and 'ItemCode' in updated_entities:
                    updated_entities['ItemName'] = updated_entities['ItemCode']
                    logger.info(f"Inferred ItemName from ItemCode: {updated_entities['ItemCode']}")
                elif var == 'ItemCode' and 'ItemName' in updated_entities:
                    updated_entities['ItemCode'] = updated_entities['ItemName']
                    logger.info(f"Inferred ItemCode from ItemName: {updated_entities['ItemName']}")
                # Add more inference rules as needed
                elif var == 'top' and var not in updated_entities:
                    # Default value for top parameter
                    updated_entities['top'] = '50'
                    logger.info("Added default value for top: 50")
                # Add default values for common parameters
                elif var in ['skip', 'orderby', 'expand'] and var not in updated_entities:
                    updated_entities[var] = ''
                    logger.info(f"Added empty default value for {var}")
                else:
                    missing_vars.append(var)
        
        if missing_vars:
            logger.warning(f"Missing required variables for template: {missing_vars}")
            return False, updated_entities
        
        logger.info("All template variables validated successfully")
        return True, updated_entities
    
    def _construct_odata_url_from_template(self, pattern, entities, structured_query=None):
        """Apply entity values to a template pattern with improved variable handling."""
        url = pattern
        
        # Replace entity placeholders with properly formatted values
        for entity_name, entity_value in entities.items():
            # Enhanced regex pattern to match variables in all formats
            import re
            
            # Check if the entity is already in a quoted context in the pattern
            # Look for patterns like: eq '{{EntityName}}'
            is_in_quotes = re.search(r"eq\s*'[^']*\{\{" + re.escape(entity_name) + r"\}\}[^']*'", url)
            
            variable_patterns = [
                r'\{\{[\s\n]*' + re.escape(entity_name) + r'[\s\n]*\}\}',  # Simple format
                r'\{\{[\s\n]*["\']' + re.escape(entity_name) + r'["\'][\s\n]*\}\}'  # JSON quoted format
            ]
            
            for var_pattern in variable_patterns:
                # Check if this pattern exists in the URL
                if re.search(var_pattern, url):
                    # Format value based on type
                    if isinstance(entity_value, str):
                        if entity_value.isdigit() or entity_value.replace('.', '', 1).isdigit():
                            # Numeric string should be treated as a number without quotes
                            formatted_value = entity_value
                        else:
                            # Non-numeric string handling - escape single quotes first
                            escaped_value = entity_value.replace("'", "''")
                            if is_in_quotes:
                                formatted_value = escaped_value  # Don't add quotes if already in quotes
                            else:
                                formatted_value = f"'{escaped_value}'"
                    elif isinstance(entity_value, bool):
                        # Boolean values should be lowercase without quotes
                        formatted_value = str(entity_value).lower()
                    elif entity_value is None:
                        # Null values
                        formatted_value = "null"
                    else:
                        # Numbers and other values don't need quotes
                        formatted_value = str(entity_value)
                    
                    # Replace the placeholder with the formatted value
                    url = re.sub(var_pattern, formatted_value, url)
        
        # Add $select parameter if fields are specified
        if structured_query and structured_query.get("fields"):
            fields = structured_query.get("fields")
            if fields and isinstance(fields, list) and len(fields) > 0:
                field_param = ",".join(fields)
                # Add the $select parameter to the URL
                if "?" in url:
                    url += f"&$select={field_param}"
                else:
                    url += f"?$select={field_param}"
        
        return url
    
    def _construct_dynamic_url(self, entity_type, structured_query):
        """Construct URL directly from structured query parameters."""
        url = f"/{entity_type}"
        params = []
        
        # Add filter if present
        filter_conditions = structured_query.get('filter_conditions', [])
        filter_parts = []
        if filter_conditions:
            for condition in filter_conditions:
                if isinstance(condition, dict) and 'field' in condition and 'value' in condition:
                    field = condition['field']
                    operator = condition.get('operator', 'eq')
                    value = condition['value']
                    
                    # Format value correctly based on type
                    if isinstance(value, str) and not value.isdigit():
                        formatted_value = f"'{value}'"
                    else:
                        formatted_value = str(value)
                    
                    filter_parts.append(f"{field} {operator} {formatted_value}")
        
        # Handle count parameters
        count_only = structured_query.get('count_only', False)
        include_count = structured_query.get('include_count', False)

        if count_only:
            # Return count endpoint URL
            count_url = f"/{entity_type}/$count"
            if filter_parts:
                count_url += f"?$filter={' and '.join(filter_parts)}"
            return count_url
        else:
            # Add filter to params for normal queries
            if filter_parts:
                params.append(f"$filter={' and '.join(filter_parts)}")
            
            # Add select if present
            if "fields" in structured_query and structured_query["fields"]:
                fields = structured_query["fields"]
                if isinstance(fields, list) and len(fields) > 0:
                    params.append(f"$select={','.join(fields)}")
            
            # Add count with results if requested
            if include_count:
                params.append("$count=true")
            
            # Add top parameter (skip for count-only)
            params.append(f"$top={structured_query.get('top', 50)}")
            
            # Add skip if specified and non-zero
            skip = structured_query.get('skip', 0)
            if skip > 0:
                params.append(f"$skip={skip}")
            
            # Add orderby if present
            order_by = structured_query.get('order_by', '')
            if order_by:
                params.append(f"$orderby={order_by}")
            
            # Add expand if present
            expand = structured_query.get('expand', [])
            if expand:
                params.append(f"$expand={','.join(expand)}")
            
            # Add the parameters to the URL
            if params:
                url += "?" + "&".join(params)
        
        return url
    
    async def _dynamic_pattern_generation(self, entity_type: str, intent: str) -> Optional[str]:
        """
        Dynamically generate a query pattern for the given entity type and intent.
        
        Args:
            entity_type: The entity type to generate a pattern for
            intent: The intent to generate a pattern for
            
        Returns:
            Optional[str]: A generated query pattern or None if generation fails
        """
        if not self.entity_registry or not entity_type:
            return None
        
        try:
            # Get schema for this entity type
            schema = await self.entity_registry.get_entity_schema(entity_type)
            
            # Extract key fields and descriptive field
            key_fields = schema.get("key_fields", [])
            descriptive_field = schema.get("descriptive_field", "")
            
            # Define basic patterns based on entity type and schema
            if not key_fields:
                # If no key fields, just return a basic query
                return f"/{entity_type}"
                
            # Get the primary key field
            primary_key = key_fields[0] if key_fields else None
            
            # Generate pattern based on intent
            if "Find" in intent or "Get" in intent:
                # For find/get intents, filter by primary key
                if primary_key:
                    pattern = f"/{entity_type}?$filter={primary_key} eq {{{{{primary_key}}}}}"
                    
                    # If there's also a descriptive field, add an OR condition
                    if descriptive_field and descriptive_field != primary_key:
                        pattern = f"/{entity_type}?$filter={primary_key} eq {{{{{primary_key}}}}} or {descriptive_field} eq '{{{{{descriptive_field}}}}}'"
                    
                    return pattern
            elif "List" in intent:
                # For list intents, just return the entity type with a top parameter
                return f"/{entity_type}?$top={{{{top}}}}"
            
            # Fallback to a basic query
            return f"/{entity_type}"
            
        except Exception as e:
            logger.error(f"Error generating dynamic pattern: {str(e)}")
            return None
    
    
    async def async_invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Construct an OData query URL optimized for the SAP B1 API asynchronously."""
        try:
            # Get information from state
            structured_query = state.get("structured_query", {})
            intent = state.get("intent", "unknown")
            
            # Log the input for debugging
            logger.info(f"Query orchestrator invoked with intent: {intent}")
            logger.info(f"Structured query: {json.dumps(structured_query, indent=2)}")
            
            # Extract entity information
            entity_type = structured_query.get('entity_type', '')
            filter_conditions = structured_query.get('filter_conditions', [])
            
            # Create entities dictionary from filter conditions
            entities = {}
            for condition in filter_conditions:
                if isinstance(condition, dict) and 'field' in condition and 'value' in condition:
                    entities[condition['field']] = condition['value']
            
            logger.info(f"Extracted entities: {json.dumps(entities, indent=2)}")
            
            # First try to match with a known pattern
            if intent in self.query_patterns:
                pattern = self.query_patterns[intent]
                try:
                    # Clean the pattern first
                    pattern = self._clean_template_pattern(pattern)
                    # Validate variables before attempting to apply them
                    valid, updated_entities = self.validate_template_variables(pattern, entities)
                    if valid:
                        # Apply the updated entities to the pattern
                        odata_url = self._construct_odata_url_from_template(pattern, updated_entities, structured_query)
                        
                        # If we've successfully constructed the URL, use it
                        if odata_url and "{{" not in odata_url:
                            # Double-check that filter conditions are included if needed
                            if filter_conditions and "$filter=" not in odata_url:
                                # Fallback to direct URL construction to ensure filters are included
                                odata_url = self._construct_dynamic_url(entity_type, structured_query)
                                logger.info(f"Fallback to direct URL construction: {odata_url}")
                                
                            state["odata_url"] = odata_url
                            state["intent"] = intent
                            state["endpoint"] = intent.split('.')[0] if '.' in intent else entity_type
                            logger.info(f"Using predefined pattern for intent {intent}: {odata_url}")
                            return state
                    else:
                        logger.info(f"Skipping pattern for {intent} due to missing variables")
                except Exception as e:
                    logger.error(f"Error applying pattern: {str(e)}")
                    logger.error(traceback.format_exc())
            
            # If no predefined pattern, try to generate a dynamic pattern
            if self.entity_registry and entity_type:
                try:
                    dynamic_pattern = await self._dynamic_pattern_generation(entity_type, intent)
                    if dynamic_pattern:
                        # Clean the dynamic pattern
                        dynamic_pattern = self._clean_template_pattern(dynamic_pattern)
                        # Add default entities if needed
                        if "top" not in entities:
                            entities["top"] = structured_query.get('top', 50)
                        
                        # Validate and apply the dynamic pattern
                        valid, updated_entities = self.validate_template_variables(dynamic_pattern, entities)
                        if valid:
                            odata_url = self._construct_odata_url_from_template(dynamic_pattern, updated_entities)
                            
                            # Add this check to ensure filters are included
                            if "{{" not in odata_url:  # Template variables were all replaced
                                # Double-check that filter conditions are included
                                filter_conditions = structured_query.get('filter_conditions', [])
                                if filter_conditions and "$filter=" not in odata_url:
                                    # Fallback to direct URL construction to ensure filters are included
                                    odata_url = self._construct_dynamic_url(entity_type, structured_query)
                            
                            state["odata_url"] = odata_url
                            state["endpoint"] = entity_type
                            logger.info(f"Using dynamic pattern for {entity_type}: {odata_url}")
                            return state
                except Exception as e:
                    logger.error(f"Error with dynamic pattern generation: {str(e)}")
            
            # If no pattern match, use the LLM for more complex queries
            if entity_type:
                # Get entity schema if available
                entity_schema = ""
                if self.entity_registry:
                    entity_schema = await self._get_entity_schema_for_prompt(entity_type)
                
                # Get metadata and examples if available
                metadata = {}
                examples = []
                if "metadata_manager" in state and state["metadata_manager"]:
                    metadata_manager = state["metadata_manager"]
                    
                    # Get relevant metadata
                    metadata = metadata_manager.get_relevant_metadata(intent, entities)
                    
                    # Get similar successful queries
                    examples = metadata_manager.get_similar_successful_queries(intent, entities)
                
                # Construct prompts
                system_prompt = self._construct_system_prompt(entity_type, entity_schema, metadata, examples)
                user_prompt = self._construct_user_prompt(intent, structured_query)
                
                # Create the prompt template
                prompt = ChatPromptTemplate.from_messages([
                    ("system", system_prompt),
                    ("user", user_prompt)
                ])
                
                # Execute the chain
                chain = prompt | self.llm | StrOutputParser()
                response = chain.invoke({})
                
                try:
                    # Parse the JSON response
                    query_data = json.loads(response)
                    
                    # Get the URL
                    url = query_data.get("url")
                    if url:
                        # Double-check that filter conditions are included
                        if filter_conditions and "$filter=" not in url:
                            # Fallback to direct URL construction to ensure filters are included
                            url = self._construct_dynamic_url(entity_type, structured_query)
                            logger.info(f"Fallback to direct URL construction: {url}")
                            
                        # Update the state
                        state["odata_url"] = url
                        state["endpoint"] = intent.split('.')[0] if '.' in intent else entity_type
                        logger.info(f"Generated query URL using LLM: {url}")
                        return state
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse LLM response: {response}")
            
            # Fallback: Use direct URL construction instead of the basic approach
            if entity_type:
                # Use our new dynamic URL construction helper
                url = self._construct_dynamic_url(entity_type, structured_query)
                
                state["odata_url"] = url
                state["endpoint"] = entity_type
                logger.info(f"Generated fallback OData URL: {url}")
            else:
                raise ValueError("No entity_type specified in structured_query")
            
            return state
            
        except Exception as e:
            logger.error(f"Error in query orchestrator: {str(e)}")
            logger.error(traceback.format_exc())
            state["error"] = {
                "stage": "query_orchestration",
                "message": str(e),
                "can_retry": False
            }
            return state
    
    
    def invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Construct an OData query URL optimized for the SAP B1 API."""
        return asyncio.run(self.async_invoke(state))