# tools/odata_constructor.py

import re
from typing import Dict, Any, Optional
from urllib.parse import quote
from utils.enhanced_errors import (
    URLConstructionError, 
    InvalidFilterError, 
    EntityNotFoundError,
    URLValidationError,
    format_error_for_response,
    SAPAssistantError
)
from utils.url_validator import ODataURLValidator
from config import get_sap_credentials
import logging
from datetime import datetime

# Set up logger
logger = logging.getLogger(__name__)

class ODataConstructorTool:
    
    def __init__(self, base_url=None, entity_registry=None):
        # Get credentials to initialize the base URL
        try:
            credentials = get_sap_credentials()
            self.base_url = credentials.get("service_layer_url", "")
            
            # If the URL already ends with /b1s/v1, don't add it again
            if not self.base_url.endswith("/b1s/v1") and not self.base_url.endswith("/b1s/v1/"):
                # Make sure we don't have a trailing slash before adding path
                if self.base_url.endswith("/"):
                    self.base_url = f"{self.base_url}b1s/v1/"
                else:
                    self.base_url = f"{self.base_url}/b1s/v1/"
                    
        except Exception as e:
            print(f"Failed to get base URL from credentials: {str(e)}")
            # Fallback to a default or the provided value
            self.base_url = base_url or "https://172.16.0.217:50000/b1s/v1/"
            
        # Store entity registry reference
        self.entity_registry = entity_registry    
        
        # Ensure the base URL has a trailing slash
        if not self.base_url.endswith("/"):
            self.base_url += "/"
        
        # Add URL validator
        self.url_validator = ODataURLValidator()
            
        print(f"OData constructor initialized with base URL: {self.base_url}")
    
    def _ensure_sap_date_format(self, date_value: str) -> str:
        """Ensure date is in SAP B1 format with single quotes."""
        if not date_value:
            return "''"
        
        # Remove any existing quotes or datetime prefixes
        clean_value = str(date_value).replace("datetime'", "").replace("'", "").strip()
        
        # Validate it's a proper date format
        if re.match(r'^\d{4}-\d{2}-\d{2}$', clean_value):
            return f"'{clean_value}'"
        
        # If not in expected format, log warning but continue
        logger.warning(f"Date value '{date_value}' not in expected YYYY-MM-DD format")
        return f"'{clean_value}'"
    
    async def _build_filter(self, conditions, entity_type):
            """Enhanced filter building with SAP B1 type awareness."""
            if not conditions:
                return ""

            # Get field type information from entity registry
            field_types = {}
            if self.entity_registry and entity_type:
                try:
                    schema = await self.entity_registry.get_entity_schema(entity_type)
                    properties = schema.get('properties', [])
                    
                    if isinstance(properties, list):
                        for prop in properties:
                            if isinstance(prop, dict) and 'name' in prop and 'type' in prop:
                                field_types[prop['name']] = prop['type']
                except Exception as e:
                    print(f"Error getting schema for {entity_type}: {str(e)}")
            
            # SAP B1 specific field classifications
            date_fields = ['DocDate', 'CreateDate', 'UpdateDate', 'DueDate', 'TaxDate', 'PostingDate', 'ReferenceDate']
            numeric_fields = ['DocEntry', 'DocNum', 'LineNum', 'DocTotal', 'Price', 'Quantity', 'QuantityOnStock']
            bool_fields = ['Paid', 'Active', 'Valid', 'Cancelled']
            
            filter_parts = []
            for condition in conditions:
                if isinstance(condition, dict) and all(k in condition for k in ['field', 'operator', 'value']):
                    field = condition['field']
                    operator = condition['operator']
                    value = condition['value']
                    
                    # Handle different field types with SAP B1 specifics
                    if field in date_fields or 'Date' in field:
                        # Date fields need datetime prefix
                        if isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}', value):
                            if not value.startswith('datetime'):
                                formatted_value = f"datetime'{value}T00:00:00'"
                            else:
                                formatted_value = value
                        else:
                            formatted_value = f"'{value}'"
                            
                    elif field in numeric_fields or field.endswith('Num') or field.endswith('Entry'):
                        # Numeric fields - no quotes
                        formatted_value = str(value)
                        
                    elif field in bool_fields:
                        # Boolean fields using SAP B1 format
                        if str(value).lower() in ['true', 'yes']:
                            formatted_value = "'tYES'"
                        elif str(value).lower() in ['false', 'no']:
                            formatted_value = "'tNO'"
                        else:
                            formatted_value = f"'{value}'"
                            
                    elif field == 'DocumentStatus':
                        # Document status special handling
                        status_map = {'open': 'bost_Open', 'closed': 'bost_Close', 'cancelled': 'bost_Cancelled'}
                        mapped_status = status_map.get(str(value).lower(), value)
                        formatted_value = f"'{mapped_status}'"
                        
                    elif isinstance(value, str):
                        # String fields - add quotes
                        escaped_value = value.replace("'", "''")
                        formatted_value = f"'{escaped_value}'"
                        
                    elif value is None:
                        formatted_value = 'null'
                        
                    else:
                        formatted_value = str(value)
                    
                    filter_parts.append(f"{field} {operator} {formatted_value}")
            
            return "$filter=" + quote(" and ".join(filter_parts)) if filter_parts else ""
    
    def _build_select(self, fields):
        """Build OData $select parameter."""
        if not fields:
            return ""
        return "$select=" + quote(",".join(fields))
    
    def _build_expand(self, expands):
        """Build OData $expand parameter."""
        if not expands:
            return ""
        return "$expand=" + quote(",".join(expands))
    
    def _build_orderby(self, order_by):
        """Build OData $orderby parameter."""
        if not order_by:
            return ""
        return "$orderby=" + quote(order_by)
    
    def _build_count(self, count_only: bool, include_count: bool) -> str:
        """Build OData count parameter."""
        if count_only:
            return "/$count"  # Special endpoint for count only
        elif include_count:
            return "$count=true"  # Include count with results
        return ""
    
    def _inject_domain_knowledge(self, state: Dict[str, Any], url: str) -> str:
        """Enhanced domain knowledge injection with comprehensive SAP B1 fixes"""
        try:
            intent = state.get("intent", "unknown")
            entity_type = state.get("structured_query", {}).get("entity_type", "")
        
            # ENHANCED: Comprehensive SAP B1 knowledge rules
            enhanced_url = url
            
            # 1. Document Status corrections (most critical)
            status_fixes = [
                (r"DocumentStatus\s+eq\s+'?([Oo]|[Oo]pen)'?", "DocumentStatus eq 'bost_Open'"),
                (r"DocumentStatus\s+eq\s+'?([Cc]|[Cc]losed)'?", "DocumentStatus eq 'bost_Close'"),
                (r"DocumentStatus\s+eq\s+'?([Cc]ancelled)'?", "DocumentStatus eq 'bost_Cancelled'"),
                # Handle single letter codes
                (r"DocumentStatus\s+eq\s+'O'", "DocumentStatus eq 'bost_Open'"),
                (r"DocumentStatus\s+eq\s+'C'", "DocumentStatus eq 'bost_Close'"),
            ]
            
            for pattern, replacement in status_fixes:
                enhanced_url = re.sub(pattern, replacement, enhanced_url)
            
            # 2. Boolean field corrections  
            boolean_fixes = [
                (r"(Paid|Active|Valid)\s+eq\s+'?([Tt]rue)'?", r"\1 eq 'tYES'"),
                (r"(Paid|Active|Valid)\s+eq\s+'?([Ff]alse)'?", r"\1 eq 'tNO'"),
            ]
            
            for pattern, replacement in boolean_fixes:
                enhanced_url = re.sub(pattern, replacement, enhanced_url)
            
            # 3. Null value corrections
            null_fixes = [
                (r"\s+eq\s+None\b", " eq null"),
                (r"\s+ne\s+None\b", " ne null"),
                (r"\s+eq\s+'None'", " eq null"),
                (r"\s+ne\s+'None'", " ne null"),
            ]
            
            for pattern, replacement in null_fixes:
                enhanced_url = re.sub(pattern, replacement, enhanced_url)
            
            # 4. String field quoting (add quotes to unquoted strings)
            string_field_pattern = r"(CardName|ItemName|CardCode|ItemCode|Reference|Memo)\s+eq\s+([^'\s&][^\s&]*)"
            enhanced_url = re.sub(string_field_pattern, r"\1 eq '\2'", enhanced_url)
            
            # 5. Date format corrections
            date_fixes = [
                # Add datetime prefix for date fields
                (r"(CreateDate|DocDate|ReferenceDate|UpdateDate|DueDate|PostingDate|TaxDate)\s+([gl]e|eq)\s+'(\d{4}-\d{2}-\d{2})'", 
                 r"\1 \2 datetime'\3T00:00:00'"),
                 
                # Fix 'now' patterns
                (r"([gl]e|eq)\s+'now'", lambda m: f"{m.group(1)} datetime'{datetime.now().strftime('%Y-%m-%d')}T00:00:00'"),
            ]
            
            for pattern, replacement in date_fixes:
                if callable(replacement):
                    enhanced_url = re.sub(pattern, replacement, enhanced_url)
                else:
                    enhanced_url = re.sub(pattern, replacement, enhanced_url)
            
            # 6. Numeric field corrections (remove quotes from numbers)
            numeric_pattern = r"(DocEntry|DocNum|DocTotal|LineTotal|Price|Quantity|QuantityOnStock|Series)\s+([gl]e|eq)\s+'(\d+(?:\.\d+)?)'(?:\s|&|$)"
            enhanced_url = re.sub(numeric_pattern, r"\1 \2 \3", enhanced_url)
            
            # 7. String escaping for names with apostrophes
            def fix_apostrophes_in_strings(url):
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

            enhanced_url = fix_apostrophes_in_strings(enhanced_url)
            
            # 8. Entity-specific corrections
            if entity_type == "ProductionOrders":
                # Fix ProductionOrder status
                enhanced_url = re.sub(r"DocumentStatus\s+eq\s+'bost_Open'", "ProductionOrderStatus eq 'boposReleased'", enhanced_url)
                
            # 9. ServiceCall status corrections
            if entity_type == "ServiceCalls":
                enhanced_url = re.sub(r"Status\s+eq\s+'open'", "Status eq -1", enhanced_url)
                enhanced_url = re.sub(r"Status\s+eq\s+'closed'", "Status eq 1", enhanced_url)
            
            # Log changes if any were made
            if enhanced_url != url:
                print(f"🔧 Applied SAP B1 domain knowledge fixes")
                print(f"   Original: {url}")
                print(f"   Enhanced: {enhanced_url}")
            
            return enhanced_url
            
        except Exception as e:
            print(f"Error injecting domain knowledge: {str(e)}")
            return url

    def _ensure_filter(self, url: str, filter_condition: str) -> str:
        """Ensure a specific filter condition is included in the URL."""
        if "?" not in url:
            return f"{url}?$filter={filter_condition}"
        
        if "$filter=" not in url:
            return f"{url}&$filter={filter_condition}"
        
        # Check if this condition is already in the filter
        filter_start = url.index("$filter=") + 8
        filter_end = url.index("&", filter_start) if "&" in url[filter_start:] else len(url)
        current_filter = url[filter_start:filter_end]
        
        if filter_condition not in current_filter:
            # Add the condition
            url = url.replace(f"$filter={current_filter}", f"$filter={current_filter} and {filter_condition}")
        
        return url
    
    
    def _apply_preventive_fixes(self, url: str, risk_assessment: Dict, state: Dict) -> str:
        """Apply preventive fixes based on risk assessment"""
        fixed_url = url
        
        # Apply common preventive patterns based on risk factors
        for risk_factor in risk_assessment.get("risk_factors", []):
            if "DocStatus" in risk_factor:
                # Preemptively fix DocStatus to DocumentStatus
                fixed_url = re.sub(r'\bDocStatus\b', 'DocumentStatus', fixed_url)
                
            elif "single quote" in risk_factor:
                # Preemptively escape single quotes
                fixed_url = re.sub(r"([^'])('(?:[^']|'')*')([^'])", r"\1\2\3", fixed_url)
                
            elif "not a NUMBER" in risk_factor:
                # Remove quotes from numeric values
                fixed_url = re.sub(r"eq\s+'(\d+)'", r"eq \1", fixed_url)
        
        # Apply suggested modifications
        for modification in risk_assessment.get("suggested_modifications", []):
            if "DocumentStatus" in modification and "bost_" not in fixed_url:
                # Apply SAP status format
                fixed_url = re.sub(r"DocumentStatus\s+eq\s+'([^']+)'", 
                                lambda m: f"DocumentStatus eq 'bost_{m.group(1).title()}'", 
                                fixed_url)
        
        if fixed_url != url:
            logger.info(f"Preventive fixes applied: {url} -> {fixed_url}")
        
        return fixed_url

    def invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Construct an OData URL from the structured query with proactive error prevention."""
        try:
            if "structured_query" not in state:
                raise URLConstructionError(
                    message="No structured query found in state",
                    suggestions=["Check previous step output", "Ensure query understanding is complete"]
                )
                
            structured_query = state["structured_query"]
            
            # Check for required entity type
            if "entity_type" not in structured_query or not structured_query["entity_type"]:
                # Try to find similar entities if entity registry is available
                similar_entities = []
                if self.entity_registry:
                    try:
                        # Use an async method in a synchronous context
                        import asyncio
                        query = state.get("query", "")
                        suggested_entity = asyncio.run(self.entity_registry.suggest_entity_type(query))
                        if suggested_entity:
                            similar_entities = [suggested_entity]
                    except Exception:
                        pass
                
                raise EntityNotFoundError(
                    message="Entity type is required in structured query",
                    entity_type=None,
                    similar_entities=similar_entities,
                    suggestions=["Specify an entity type in your query", 
                                "Use an entity type like: BusinessPartners, Items, Orders, etc."]
                )
            
            if "fields" in state and state["fields"]:
                structured_query["fields"] = state["fields"]
            
            # Extract entity type
            entity_type = structured_query['entity_type']
            
            # Build query parameters asynchronously
            import asyncio
            
            # Create an async method to build all parts
            async def build_query_async():
                params = []
                filter_param = None
                
                # Add filter if present
                try:
                    filter_param = await self._build_filter(structured_query.get('filter_conditions', []), entity_type)
                    if filter_param:
                        params.append(filter_param)
                except Exception as e:
                    raise InvalidFilterError(
                        message=f"Error building filter parameter: {str(e)}",
                        details={"filter_conditions": structured_query.get('filter_conditions', [])},
                        original_exception=e
                    )
                
                # Add count parameter handling
                count_only = structured_query.get('count_only', False)
                include_count = structured_query.get('include_count', False)

                if count_only:
                    # For count-only queries, use /$count endpoint
                    url = f"{entity_type}/$count"
                    # Only include filter for count queries
                    count_params = []
                    if filter_param:
                        count_params.append(filter_param)
                    if count_params:
                        url += "?" + "&".join(count_params)
                    return url, []  # Return URL and empty params for count-only
                else:
                    # Normal query building continues...
                    # Add select if present
                    select_param = self._build_select(structured_query.get('fields', []))
                    if select_param:
                        params.append(select_param)
                    
                    # Add count with results if requested
                    if include_count:
                        params.append("$count=true")
                    
                    # Add expand if present
                    expand_param = self._build_expand(structured_query.get('expand', []))
                    if expand_param:
                        params.append(expand_param)
                    
                    # Add top with fallback to default
                    top = structured_query.get('top', 50)
                    params.append(f"$top={top}")
                    
                    # Add skip if non-zero
                    skip = structured_query.get('skip', 0)
                    if skip > 0:
                        params.append(f"$skip={skip}")
                    
                    # Add orderby if present
                    orderby_param = self._build_orderby(structured_query.get('order_by', ''))
                    if orderby_param:
                        params.append(orderby_param)
                    
                    return None, params  # Return None for URL (will be built below) and params
            
            # Run the async code
            count_url, params = asyncio.run(build_query_async())
            
            # Handle count-only queries
            if count_url:
                url = count_url
            else:
                # Combine URL and parameters for normal queries
                url = f"{entity_type}"  # Remove self.base_url here since we'll add it later if needed
                if params:
                    url += "?" + "&".join(params)
            
            # **NEW: PROACTIVE ERROR PREVENTION - ADD THIS SECTION HERE**
            if "metadata_manager" in state and state["metadata_manager"]:
                metadata_manager = state["metadata_manager"]
                
                # Assess risk before applying any fixes
                risk_assessment = metadata_manager.assess_query_risk(structured_query, url)
                
                if risk_assessment["risk_score"] > 0.6:  # High risk threshold
                    logger.warning(f"High risk query detected (score: {risk_assessment['risk_score']:.3f})")
                    logger.warning(f"Risk factors: {risk_assessment['risk_factors']}")
                    
                    # Apply preventive modifications
                    url = self._apply_preventive_fixes(url, risk_assessment, state)
                    
                    # Track the intervention
                    state["proactive_intervention"] = {
                        "risk_score": risk_assessment["risk_score"],
                        "risk_factors": risk_assessment["risk_factors"],
                        "modifications_applied": risk_assessment["suggested_modifications"]
                    }
                    
                    logger.info(f"Applied preventive fixes to high-risk query")
            
            # Apply domain knowledge to improve URL (existing code continues here)
            url = self._inject_domain_knowledge(state, url)
            
            # Use the new URL validator for enhanced validation
            is_valid, issues = self.url_validator.validate_url(url)
            
            # If issues are found, try to fix them automatically
            if not is_valid:
                # Log the issues found
                for issue in issues:
                    logger.warning(f"URL validation issue: {issue['message']}")
                
                # Try automatic fixes
                fixed_url, fixes_applied = self.url_validator.fix_common_issues(url)
                
                # Log applied fixes
                for fix in fixes_applied:
                    logger.info(f"Applied URL fix: {fix['message']}")
                
                # If any fixes were applied, use the fixed URL
                if fixes_applied:
                    logger.info(f"Original URL: {url}")
                    logger.info(f"Fixed URL: {fixed_url}")
                    url = fixed_url
                    
                    # Revalidate the fixed URL
                    is_valid, remaining_issues = self.url_validator.validate_url(url)
                    
                    # If still not valid, add warning to state but continue
                    if not is_valid:
                        state["warning"] = {
                            "message": "URL may have issues despite automatic fixes",
                            "issues": remaining_issues
                        }
                else:
                    # If no fixes were applied but issues exist, add warning
                    state["warning"] = {
                        "message": "URL validation issues detected",
                        "issues": issues
                    }
            
            # Ensure URL is properly formatted with base URL if needed
            if not url.startswith(self.base_url) and not url.startswith("http"):
                full_url = f"{self.base_url}{url}" if not url.startswith("/") else f"{self.base_url}{url[1:]}"
            else:
                full_url = url
                
            logger.info(f"Constructed OData URL: {full_url}")
            
            state["odata_url"] = full_url
            return state
            
        except SAPAssistantError as e:
            # Log the standardized error and update state
            error_dict = e.log()
            state["error"] = error_dict
            return state
        except Exception as e:
            # Convert generic exceptions to standard format
            error = URLConstructionError(
                message=f"Unexpected error in OData construction: {str(e)}",
                details={"error_type": type(e).__name__},
                original_exception=e
            )
            error_dict = error.log()
            state["error"] = error_dict
            return state