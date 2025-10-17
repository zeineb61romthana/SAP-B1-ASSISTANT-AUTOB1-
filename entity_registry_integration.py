# integration/entity_registry_integration.py

import logging
from typing import Dict, Any, List, Optional
import asyncio
import re
import sqlite3
import json
import os
import time
from datetime import datetime

logger = logging.getLogger("EntityRegistryIntegration")

class HybridEntityRegistry:
    def __init__(self, service_layer_client):
        self.client = service_layer_client
        
        # Pre-defined schemas for commonly used entities
        self.core_schemas = {
            "BusinessPartners": {
                "properties": ["CardCode", "CardName", "CardType", "GroupCode", "Phone1", "EmailAddress"],
                "key_fields": ["CardCode"],
                "common_filters": {
                    "customer": {"field": "CardType", "value": "C"},
                    "vendor": {"field": "CardType", "value": "S"},
                    "lead": {"field": "CardType", "value": "L"}
                },
                "descriptive_field": "CardName"
            },
            "Items": {
                "properties": ["ItemCode", "ItemName", "ItemType", "InventoryUOM", "ItemGroupCode"],
                "key_fields": ["ItemCode"],
                "common_filters": {
                    "inventory": {"field": "ItemType", "value": "I"},
                    "service": {"field": "ItemType", "value": "S"}
                },
                "descriptive_field": "ItemName"
            },
            # Add more pre-defined schemas for common entities
        }
        
        # Storage for dynamically discovered entities
        self.discovered_schemas = {}
        self.entity_set_mappings = {}  # Maps endpoint names to entity types
        
    async def initialize(self):
        """Initialize the registry with both pre-defined and discovered schemas"""
        # Fetch entity sets mapping (endpoint names to entity types)
        self._discover_entity_sets()
        
        # Initialize entity metadata for core schemas
        for entity_type in self.core_schemas:
            if entity_type not in self.discovered_schemas:
                try:
                    schema = await self._discover_entity_schema(entity_type)
                    # Merge with pre-defined schema
                    self.discovered_schemas[entity_type] = {
                        **schema,
                        "common_filters": self.core_schemas[entity_type].get("common_filters", {}),
                        "descriptive_field": self.core_schemas[entity_type].get("descriptive_field")
                    }
                except Exception as e:
                    logger.warning(f"Could not discover schema for core entity {entity_type}: {str(e)}")
        
    def _discover_entity_sets(self):
        """Discover all entity sets (endpoints) from the service document"""
        try:
            # For the enhanced client, we need to handle the execute_request method
            # instead of using get() directly
            response = self.client.execute_request("/")
            if isinstance(response, dict) and "error" in response:
                logger.error(f"Failed to fetch service document: {response.get('error')}")
                return
                
            # Extract entity sets from service document
            if "value" in response:
                # Handle format from the enhanced client
                for entity_set in response.get("value", []):
                    if isinstance(entity_set, str):
                        self.entity_set_mappings[entity_set] = entity_set
                    elif isinstance(entity_set, dict) and "name" in entity_set:
                        self.entity_set_mappings[entity_set["name"]] = entity_set["name"]
            
            logger.info(f"Discovered {len(self.entity_set_mappings)} entity sets")
            
        except Exception as e:
            logger.error(f"Error discovering entity sets: {str(e)}")
    
    async def _discover_entity_schema(self, entity_type):
        """Discover schema for a specific entity type on demand"""
        # If we already have this schema, return it
        if entity_type in self.discovered_schemas:
            return self.discovered_schemas[entity_type]
            
        try:
            # Approach: Infer schema from a sample entity
            endpoint = self.entity_set_mappings.get(entity_type, entity_type)
            response = self.client.execute_request(f"/{endpoint}?$top=1")
            
            if isinstance(response, dict) and "error" in response:
                raise Exception(f"Failed to fetch sample for {entity_type}: {response.get('error')}")
                
            # Extract schema from sample entity
            if "value" in response and len(response["value"]) > 0:
                sample = response["value"][0]
                
                properties = []
                for prop_name, prop_value in sample.items():
                    # Skip metadata properties
                    if prop_name.startswith("__"):
                        continue
                        
                    # Infer property type
                    prop_type = self._infer_type(prop_value)
                    properties.append({
                        "name": prop_name,
                        "type": prop_type,
                        "nullable": prop_value is None
                    })
                
                # We can't reliably determine keys from sample data alone
                # so we'll assume the ID field or first property is the key
                key_fields = []
                for key_candidate in ["Id", f"{entity_type}ID", "Code", f"{entity_type}Code"]:
                    if any(p["name"] == key_candidate for p in properties):
                        key_fields = [key_candidate]
                        break
                
                if not key_fields and properties:
                    key_fields = [properties[0]["name"]]
                
                schema = {
                    "properties": properties,
                    "key_fields": key_fields,
                    "inferred": True  # Flag that this was inferred, not from metadata
                }
                
                # Cache this schema
                self.discovered_schemas[entity_type] = schema
                return schema
            else:
                raise Exception(f"No sample data available for {entity_type}")
                
        except Exception as e:
            logger.error(f"Error discovering schema for {entity_type}: {str(e)}")
            # Return a minimal schema to avoid breaking functionality
            return {
                "properties": [],
                "key_fields": ["Code"],
                "inferred": True,
                "error": str(e)
            }
                    
    def _infer_type(self, value):
        """Infer property type from a value"""
        if value is None:
            return "Edm.String"  # Default assumption
        elif isinstance(value, bool):
            return "Edm.Boolean"
        elif isinstance(value, int):
            return "Edm.Int32"
        elif isinstance(value, float):
            return "Edm.Double"
        elif isinstance(value, str):
            # Check if it looks like a date
            if self._is_date_format(value):
                return "Edm.DateTime"
            return "Edm.String"
        elif isinstance(value, dict):
            return "Complex"
        elif isinstance(value, list):
            return "Collection"
        else:
            return "Edm.String"  # Default fallback
    
    def _is_date_format(self, value):
        """Check if a string looks like a date"""
        # Simple date format check
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # ISO format
            r'\d{4}/\d{2}/\d{2}',  # Slash format
            r'\d{2}/\d{2}/\d{4}'   # US format
        ]
        for pattern in date_patterns:
            if re.match(pattern, value):
                return True
        return False
        
    async def get_entity_schema(self, entity_type):
        """Get schema for an entity type, discovering it if needed"""
        # Check pre-defined schema first
        if entity_type in self.core_schemas:
            # If we also have discovered schema, merge them
            if entity_type in self.discovered_schemas:
                return {**self.discovered_schemas[entity_type], **self.core_schemas[entity_type]}
            return self.core_schemas[entity_type]
            
        # Check if we've already discovered this entity
        if entity_type in self.discovered_schemas:
            return self.discovered_schemas[entity_type]
            
        # Discover schema on demand
        schema = await self._discover_entity_schema(entity_type)
        return schema
        
    def get_all_entity_types(self):
        """Get all known entity types"""
        # Combine pre-defined and discovered entities
        return list(set(list(self.core_schemas.keys()) + 
                       list(self.discovered_schemas.keys()) + 
                       list(self.entity_set_mappings.keys())))


class EntityRegistryIntegration:
    """
    Integration class that connects HybridEntityRegistry with the SAP query understanding system
    to enable dynamic entity discovery and improved entity coverage.
    """
    
    def __init__(self, sap_client):
        """
        Initialize the integration with a SAP client.
        
        Args:
            sap_client: The client for interacting with the SAP B1 OData service
        """
        self.registry = HybridEntityRegistry(sap_client)
        self.initialized = False
        self.known_entity_types = set()
        self.entity_type_mappings = {}  # Maps common names to actual entity types
        
        # Cache configuration
        self.cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache")
        self.cache_file = os.path.join(self.cache_dir, "entity_registry_cache.json")
        self.cache_ttl = 86400 * 7  # Cache time-to-live in seconds (7 days)
        
        # Ensure cache directory exists
        if not os.path.exists(self.cache_dir):
            try:
                os.makedirs(self.cache_dir)
            except Exception as e:
                logger.error(f"Failed to create cache directory: {str(e)}")
        
        # ADD: SAP B1 specific field mappings
        self.sap_b1_field_mappings = {
            "BusinessPartners": {
                "docdate": "CreateDate",  # BusinessPartners don't have DocDate
                "docstatus": "Valid",     # BusinessPartners use Valid field
                "status": "Valid",
                "active": "Valid"
            },
            "Orders": {
                "docstatus": "DocumentStatus",  # Not DocStatus
                "status": "DocumentStatus"
            },
            "Invoices": {
                "docstatus": "DocumentStatus",
                "status": "DocumentStatus"
            },
            "JournalEntries": {
                "refdate": "ReferenceDate",  # Not RefDate
                "docdate": "ReferenceDate"
            },
            "ProductionOrders": {
                "docstatus": "ProductionOrderStatus",  # Not DocumentStatus
                "documentstatus": "ProductionOrderStatus"
            }
        }
        
        # ADD: SAP B1 enum value mappings
        self.sap_b1_enum_mappings = {
            "DocumentStatus": {
                "open": "bost_Open",
                "o": "bost_Open",
                "closed": "bost_Close",
                "c": "bost_Close",
                "cancelled": "bost_Cancelled"
            },
            "ProductionOrderStatus": {
                "open": "boposReleased",
                "o": "boposReleased"
            },
            "Paid": {
                "true": "tYES",
                "yes": "tYES",
                "false": "tNO",
                "no": "tNO"
            },
            "CardType": {
                "customer": "C",
                "c": "C",
                "vendor": "S",
                "supplier": "S",
                "s": "S"
            }
        }
    
    def _is_cache_valid(self):
        """Check if cache file exists and is not expired."""
        if not os.path.exists(self.cache_file):
            return False
            
        try:
            # Check file modification time
            modified_time = os.path.getmtime(self.cache_file)
            current_time = time.time()
            
            # Check if cache is expired
            if current_time - modified_time > self.cache_ttl:
                logger.info(f"Cache is expired ({(current_time - modified_time) / 86400:.1f} days old)")
                return False
                
            # Check if cache file is valid JSON
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
                
            # Check if cache has required keys
            required_keys = ["metadata", "entity_types", "entity_schemas", "entity_mappings"]
            if not all(key in cache_data for key in required_keys):
                logger.warning(f"Cache file is missing required keys")
                return False
                
            return True
        except Exception as e:
            logger.warning(f"Error validating cache: {str(e)}")
            return False
    
    def _load_from_cache(self):
        """Load entity registry data from cache file."""
        try:
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
                
            # Load entity types
            self.known_entity_types = set(cache_data["entity_types"])
            
            # Load entity mappings
            self.entity_type_mappings = cache_data["entity_mappings"]
            
            # Load entity schemas into registry
            self.registry.core_schemas = cache_data.get("core_schemas", self.registry.core_schemas)
            self.registry.discovered_schemas = cache_data["entity_schemas"]
            
            logger.info(f"Loaded {len(self.known_entity_types)} entity types from cache")
            return True
        except Exception as e:
            logger.error(f"Error loading from cache: {str(e)}")
            return False
    
    def _save_to_cache(self):
        """Save entity registry data to cache file."""
        try:
            cache_data = {
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "version": "1.0"
                },
                "entity_types": list(self.known_entity_types),
                "entity_mappings": self.entity_type_mappings,
                "entity_schemas": self.registry.discovered_schemas,
                "core_schemas": self.registry.core_schemas
            }
            
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
                
            logger.info(f"Saved {len(self.known_entity_types)} entity types to cache")
            return True
        except Exception as e:
            logger.error(f"Error saving to cache: {str(e)}")
            return False
    
    def _build_entity_mappings(self):
        """Create mappings for common entity names to actual entity types"""
        self.entity_type_mappings = {
            "customer": "BusinessPartners",
            "customers": "BusinessPartners", 
            "item": "Items",
            "items": "Items",
            "order": "Orders",
            "orders": "Orders",
            "invoice": "Invoices",
            "invoices": "Invoices"
        }
        # Add mappings from known entity types
        for entity_type in self.known_entity_types:
            self.entity_type_mappings[entity_type.lower()] = entity_type
    
    def refresh_cache(self, force=False):
        """Force a refresh of the entity registry cache."""
        if force or not self._is_cache_valid():
            self.initialized = False
            if os.path.exists(self.cache_file):
                try:
                    os.remove(self.cache_file)
                except:
                    pass
            asyncio.run(self.initialize())
            return True
        return False
    
    async def initialize(self):
        """Initialize the registry and cache known entity types"""
        if self.initialized:
            return
            
        try:
            logger.info("Initializing entity registry...")
            
            # Try to load from cache first
            if self._is_cache_valid() and self._load_from_cache():
                # Build entity mappings from cached data
                self._build_entity_mappings()
                self.initialized = True
                logger.info(f"Entity registry initialized from cache with {len(self.known_entity_types)} entity types")
                return
                
            # If cache is invalid or loading failed, initialize from API
            await self.registry.initialize()
            self.known_entity_types = set(await self.registry.get_all_entity_types())
            
            # Create mappings for common entity names to actual entity types
            self._build_entity_mappings()
            
            # Save to cache
            self._save_to_cache()
            
            self.initialized = True
            logger.info(f"Entity registry initialized with {len(self.known_entity_types)} entity types")
        except Exception as e:
            logger.error(f"Error initializing entity registry: {str(e)}")
            # Instead of raising, continue with minimal functionality
            self.initialized = True
            self.known_entity_types = set(self.registry.core_schemas.keys())
            logger.info(f"Fallback to pre-defined schemas: {len(self.known_entity_types)} entity types")

    # Update the get_entity_schema method to use the cache
    async def get_entity_schema(self, entity_type: str) -> Dict[str, Any]:
        """
        Get the schema for an entity type, enhanced with more properties for intent recognition.
        Uses cached data when available.
        
        Args:
            entity_type: The entity type to get the schema for
                
        Returns:
            The enhanced schema for the entity type
        """
        if not self.initialized:
            await self.initialize()
            
        # Map entity type if a common name was used
        mapped_entity_type = self.map_entity_type(entity_type)
        
        # Get the schema from the registry
        try:
            # Check if schema is in the cache
            if mapped_entity_type in self.registry.discovered_schemas:
                schema = self.registry.discovered_schemas[mapped_entity_type]
            else:
                # If not in cache, fetch from registry and add to cache
                schema = await self.registry.get_entity_schema(mapped_entity_type)
                # Save new schema to cache
                self.registry.discovered_schemas[mapped_entity_type] = schema
                self._save_to_cache()
            
            # Add query_patterns for known entity types
            if mapped_entity_type == "BusinessPartners":
                schema["query_patterns"] = {
                    "FindCustomer": "/BusinessPartners?$filter=CardType eq 'C' and CardName eq '{{CardName}}'",
                    "ListCustomers": "/BusinessPartners?$filter=CardType eq 'C'",
                    "FindSupplier": "/BusinessPartners?$filter=CardType eq 'S' and CardName eq '{{CardName}}'",
                    "ListSuppliers": "/BusinessPartners?$filter=CardType eq 'S'"
                }
                schema["common_phrases"] = {
                    "FindCustomer": ["find customer", "get customer", "show customer", "customer details"],
                    "ListCustomers": ["list customers", "show all customers", "get customers"],
                    "FindSupplier": ["find supplier", "get supplier", "show supplier", "supplier details"],
                    "ListSuppliers": ["list suppliers", "show all suppliers", "get suppliers"]
                }
            elif mapped_entity_type == "Items":
                schema["query_patterns"] = {
                    "FindItem": "/Items?$filter=ItemCode eq '{{ItemCode}}' or ItemName eq '{{ItemName}}'",
                    "ListItems": "/Items"
                }
                schema["common_phrases"] = {
                    "FindItem": ["find item", "get item", "show item", "item details", "product details"],
                    "ListItems": ["list items", "show all items", "get items", "list products"]
                }
            elif mapped_entity_type == "Orders":
                schema["query_patterns"] = {
                    "FindSpecificOrder": "/Orders?$filter=DocNum eq {{DocNum}}",
                    "FindOrdersByCustomer": "/Orders?$filter=CardCode eq '{{CardCode}}' or CardName eq '{{CardName}}'"
                }
                schema["common_phrases"] = {
                    "FindSpecificOrder": ["find order", "get order", "show order", "order details", "order number"],
                    "FindOrdersByCustomer": ["orders for customer", "customer orders", "find orders by customer"]
                }
            elif mapped_entity_type == "Invoices":
                schema["query_patterns"] = {
                    "FindInvoice": "/Invoices?$filter=DocNum eq {{DocNum}}",
                    "FindInvoicesByCustomer": "/Invoices?$filter=CardCode eq '{{CardCode}}' or CardName eq '{{CardName}}'"
                }
                schema["common_phrases"] = {
                    "FindInvoice": ["find invoice", "get invoice", "show invoice", "invoice details", "invoice number"],
                    "FindInvoicesByCustomer": ["invoices for customer", "customer invoices", "find invoices by customer"]
                }
            
            return schema
        except Exception as e:
            logger.error(f"Error getting schema for {mapped_entity_type}: {str(e)}")
            # Return a fallback schema
            return {
                "properties": [],
                "key_fields": [],
                "inferred": True,
                "error": str(e)
            }
            
    # Add this method to the EntityRegistryIntegration class
    async def get_query_template_for_intent(self, intent: str) -> Optional[str]:
        """
        Get a query template for a given intent.
        
        Args:
            intent: The intent to get a template for (e.g., "BusinessPartners.FindCustomer")
        
        Returns:
            A template string or None if no template is available
        """
        try:
            if "." not in intent:
                return None
                
            entity_type, action = intent.split(".")
            
            # Map entity type if needed
            mapped_entity_type = self.map_entity_type(entity_type)
            
            # Get schema for this entity type
            schema = await self.get_entity_schema(mapped_entity_type)
            
            # Look for query patterns in schema
            if "query_patterns" in schema and action in schema["query_patterns"]:
                return schema["query_patterns"][action]
            
            # If no specific pattern, generate a basic one
            if action.startswith("FindBy") and len(action) > 6:
                field = action[6:]  # Extract field name from "FindByX"
                return f"/{mapped_entity_type}?$filter={field} eq '{{{{{field}}}}}'"
            elif action == "Find":
                # Get key fields from schema
                key_fields = schema.get("key_fields", [])
                if key_fields:
                    primary_key = key_fields[0]
                    return f"/{mapped_entity_type}?$filter={primary_key} eq '{{{{{primary_key}}}}}'"
                else:
                    return f"/{mapped_entity_type}?$top=1"  # Fallback
            elif action == "List":
                return f"/{mapped_entity_type}"
            
            return None
        except Exception as e:
            logger.error(f"Error getting query template for intent {intent}: {str(e)}")
            return None        
    
    def map_entity_type(self, entity_type: str) -> str:
        """
        Map a user-provided entity type to an actual SAP B1 entity type.
        
        Args:
            entity_type: The entity type provided by the user or intent recognition
            
        Returns:
            The mapped entity type if found, otherwise the original entity type
        """
        if not entity_type:
            return entity_type
            
        # Check for direct match (case insensitive)
        entity_lower = entity_type.lower()
        if entity_lower in self.entity_type_mappings:
            return self.entity_type_mappings[entity_lower]
            
        # Try to match with partial name
        for known_type in self.known_entity_types:
            if known_type.lower().startswith(entity_lower) or entity_lower.startswith(known_type.lower()):
                return known_type
                
        # Return original if no mapping found
        return entity_type
    
    async def suggest_entity_type(self, query_text: str) -> Optional[str]:
        """
        Suggest an entity type based on the query text when no entity type is explicitly specified.

        Args:
            query_text: The raw query text from the user

        Returns:
            A suggested entity type or None
        """
        if not self.initialized:
            await self.initialize()

        query_lower = query_text.lower()

        # ✅ IMPLEMENT PRIORITY MAP (higher precedence than normal logic)
        priority_map = {
            "order": "Orders",
            "invoice": "Invoices",
            "customer": "BusinessPartners",
            "item": "Items"
        }
        for term, entity in priority_map.items():
            if term in query_lower and entity in self.known_entity_types:
                print(f"🎯 Priority mapping matched term '{term}' to entity '{entity}'")
                return entity

        # Existing direct mappings
        for common_name, entity_type in self.entity_type_mappings.items():
            if common_name in query_lower and entity_type in self.known_entity_types:
                return entity_type

        # Default fallbacks based on query context
        if any(word in query_lower for word in ["buy", "sell", "selling", "purchase", "order"]):
            if "Orders" in self.known_entity_types:
                return "Orders"

        if any(word in query_lower for word in ["invoice", "bill", "payment", "paid"]):
            if "Invoices" in self.known_entity_types:
                return "Invoices"

        if any(word in query_lower for word in ["stock", "inventory", "product", "item"]):
            if "Items" in self.known_entity_types:
                return "Items"

        if any(word in query_lower for word in ["customer", "client", "account"]):
            if "BusinessPartners" in self.known_entity_types:
                return "BusinessPartners"

        return None
    
    def enrich_structured_query(self, structured_query: Dict[str, Any], query_text: str) -> Dict[str, Any]:
        """
        Enrich a structured query with additional entity information from the registry.
        
        Args:
            structured_query: The structured query to enrich
            query_text: The original query text
            
        Returns:
            The enriched structured query
        """
        # Make a copy to avoid modifying the original
        enriched_query = structured_query.copy()
        
        # If no entity type is specified, try to suggest one
        if not enriched_query.get("entity_type"):
            entity_type = asyncio.run(self.suggest_entity_type(query_text))
            if entity_type:
                enriched_query["entity_type"] = entity_type
                logger.info(f"Suggested entity type: {entity_type} for query: {query_text}")
        else:
            # Map the entity type if it's a common name
            original_type = enriched_query["entity_type"]
            mapped_type = self.map_entity_type(original_type)
            
            if mapped_type != original_type:
                enriched_query["entity_type"] = mapped_type
                logger.info(f"Mapped entity type from {original_type} to {mapped_type}")
        
        # Ensure customers have CardType filter
        if enriched_query.get("entity_type") == "BusinessPartners" and "customer" in query_text.lower():
            # Check if we already have a CardType filter
            has_card_type = False
            for condition in enriched_query.get("filter_conditions", []):
                if isinstance(condition, dict) and condition.get("field") == "CardType":
                    has_card_type = True
                    break
            
            # Add CardType filter if needed
            if not has_card_type:
                if "filter_conditions" not in enriched_query:
                    enriched_query["filter_conditions"] = []
                
                enriched_query["filter_conditions"].append({
                    "field": "CardType",
                    "operator": "eq",
                    "value": "C"
                })
                logger.info(f"Added CardType='C' filter for customer query")
                
        return enriched_query
    
    async def validate_and_fix_structured_query(self, structured_query: Dict[str, Any], 
                                               original_query: str) -> Dict[str, Any]:
        """
        CRITICAL: Validate and fix structured query BEFORE URL construction
        This is the main fix for your field mapping issues
        """
        if not structured_query:
            return structured_query
            
        entity_type = structured_query.get("entity_type", "")
        if not entity_type:
            return structured_query
            
        logger.info(f"🔍 Pre-validating query for {entity_type}")
        
        # Create a copy to avoid modifying original
        fixed_query = structured_query.copy()
        fixes_applied = []
        
        # Fix field names in filter conditions
        filter_conditions = fixed_query.get("filter_conditions", [])
        fixed_conditions = []
        
        for condition in filter_conditions:
            if not isinstance(condition, dict):
                fixed_conditions.append(condition)
                continue
                
            fixed_condition = condition.copy()
            
            # Fix field name using SAP B1 mappings
            original_field = condition.get("field", "")
            original_field_lower = original_field.lower()
            
            entity_mappings = self.sap_b1_field_mappings.get(entity_type, {})
            if original_field_lower in entity_mappings:
                correct_field = entity_mappings[original_field_lower]
                fixed_condition["field"] = correct_field
                fixes_applied.append(f"Field: {original_field} -> {correct_field}")
                
            # Fix enum values
            field_name = fixed_condition.get("field", "")
            original_value = condition.get("value")
            
            if isinstance(original_value, str):
                for enum_field, mappings in self.sap_b1_enum_mappings.items():
                    if enum_field in field_name:
                        correct_value = mappings.get(original_value.lower(), original_value)
                        if correct_value != original_value:
                            fixed_condition["value"] = correct_value
                            fixes_applied.append(f"Value: {original_value} -> {correct_value}")
                        break
                        
            fixed_conditions.append(fixed_condition)
            
        fixed_query["filter_conditions"] = fixed_conditions
        
        # Add missing CardType for BusinessPartners customer queries
        if entity_type == "BusinessPartners" and "customer" in original_query.lower():
            has_cardtype = any(c.get("field") == "CardType" for c in fixed_conditions)
            if not has_cardtype:
                fixed_conditions.append({
                    "field": "CardType",
                    "operator": "eq",
                    "value": "C"
                })
                fixes_applied.append("Added CardType='C' filter")
                
        if fixes_applied:
            logger.info(f"✅ Applied {len(fixes_applied)} fixes: {fixes_applied}")
            
        return fixed_query
    
    async def get_all_entity_types(self) -> List[str]:
        """Get all known entity types from the registry"""
        if not self.initialized:
            await self.initialize()
            
        return list(self.known_entity_types)
    
    async def get_entity_field_mapping(self, entity_type: str) -> Dict[str, str]:
        """
        Get a mapping of common field names to actual field names for an entity type.
        
        Args:
            entity_type: The entity type to get the field mapping for
            
        Returns:
            A dictionary mapping common field names to actual field names
        """
        schema = await self.get_entity_schema(entity_type)
        field_mapping = {}
        
        # Extract properties from schema
        properties = schema.get("properties", [])
        
        # Handle different schema formats
        if isinstance(properties, list):
            if all(isinstance(prop, str) for prop in properties):
                # Simple list of property names
                for prop in properties:
                    field_mapping[prop.lower()] = prop
            elif all(isinstance(prop, dict) for prop in properties):
                # List of property objects
                for prop in properties:
                    if "name" in prop:
                        field_mapping[prop["name"].lower()] = prop["name"]
        elif isinstance(properties, dict):
            # Dictionary of properties
            for prop_name in properties:
                field_mapping[prop_name.lower()] = prop_name
        
        # Add common aliases for fields based on entity type
        if entity_type == "BusinessPartners":
            common_field_mappings = {
                "customer id": "CardCode",
                "customer code": "CardCode",
                "customer name": "CardName",
                "phone": "Phone1",
                "email": "EmailAddress",
                "type": "CardType",
                "group": "GroupCode",
                "balance": "CurrentAccountBalance"
            }
        elif entity_type == "Items":
            common_field_mappings = {
                "item code": "ItemCode",
                "item number": "ItemCode",
                "product code": "ItemCode",
                "product id": "ItemCode",
                "item name": "ItemName",
                "product name": "ItemName",
                "description": "ItemName",
                "price": "Price",
                "stock": "QuantityOnStock",
                "inventory": "QuantityOnStock",
                "unit": "InventoryUOM",
                "group": "ItemGroupCode"
            }
        elif entity_type == "Orders":
            common_field_mappings = {
                "order id": "DocNum",
                "order number": "DocNum",
                "order date": "DocDate",
                "customer": "CardCode",
                "customer name": "CardName",
                "total": "DocTotal",
                "status": "DocumentStatus",
                "due date": "DocDueDate"
            }
        elif entity_type == "Invoices":
            common_field_mappings = {
                "invoice id": "DocNum",
                "invoice number": "DocNum",
                "invoice date": "DocDate",
                "due date": "DocDueDate",
                "customer": "CardCode",
                "customer name": "CardName",
                "total": "DocTotal",
                "paid": "Paid"
            }
        else:
            common_field_mappings = {
                "id": "Code",
                "code": "Code",
                "name": "Name",
                "date": "Date",
                "description": "Description"
            }
        
        # Add only field mappings that exist in the schema
        for common_name, field_name in common_field_mappings.items():
            if field_name in field_mapping.values():
                field_mapping[common_name.lower()] = field_name
        
        return field_mapping
    
    async def map_field_name(self, entity_type: str, field_name: str) -> str:
        """
        Map a user-provided field name to an actual field name for an entity type.
        
        Args:
            entity_type: The entity type
            field_name: The field name provided by the user
            
        Returns:
            The mapped field name if found, otherwise the original field name
        """
        if not field_name:
            return field_name
            
        # Get field mapping for this entity type
        field_mapping = await self.get_entity_field_mapping(entity_type)
        
        # Try to map the field name
        field_lower = field_name.lower()
        if field_lower in field_mapping:
            return field_mapping[field_lower]
            
        # Return original if no mapping found
        return field_name
    
    async def suggest_correction_for_entity(self, entity_type: str) -> Optional[str]:
        """
        Suggest a correction for an invalid entity type based on available entities.
        
        Args:
            entity_type: The potentially invalid entity type
            
        Returns:
            A suggested correction or None if no suggestion is available
        """
        if not self.initialized:
            await self.initialize()
            
        if not entity_type:
            return None
            
        # If the entity already exists, no correction needed
        if entity_type in self.known_entity_types:
            return entity_type
            
        # Check if we can map it using common names
        mapped = self.map_entity_type(entity_type)
        if mapped != entity_type and mapped in self.known_entity_types:
            return mapped
            
        # Try to find a close match using string similarity
        try:
            import difflib
            matches = difflib.get_close_matches(entity_type, self.known_entity_types, n=1, cutoff=0.7)
            if matches:
                return matches[0]
        except Exception as e:
            logger.error(f"Error finding similar entity types: {str(e)}")
            
        return None
    
    def get_entity_relationships(self, entity_type: str) -> Dict[str, str]:
        """
        Get relationship information for an entity type.
        
        Args:
            entity_type: The entity type to get relationships for
            
        Returns:
            A dictionary mapping relationship names to related entity types
        """
        # Common relationships in SAP B1
        common_relationships = {
            "BusinessPartners": {
                "orders": "Orders",
                "invoices": "Invoices",
                "contacts": "Contacts",
                "addresses": "Addresses"
            },
            "Items": {
                "warehouses": "WarehouseItemInfo",
                "prices": "ItemPrices",
                "inventory": "InventoryGenEntries"
            },
            "Orders": {
                "customer": "BusinessPartners",
                "items": "DocumentLines",
                "delivery": "DeliveryNotes"
            },
            "Invoices": {
                "customer": "BusinessPartners",
                "items": "DocumentLines",
                "payments": "IncomingPayments"
            }
        }
        
        # Check if this entity type has predefined relationships
        if entity_type in common_relationships:
            return common_relationships[entity_type]
            
        # For now, return an empty dict for unknown entity types
        # In a more sophisticated implementation, this could discover
        # relationships by analyzing metadata
        return {}
        
    def is_initialized(self) -> bool:
        """Check if the entity registry is initialized"""
        return self.initialized
    
    def get_registry_status(self) -> Dict[str, Any]:
        """Get status information about the registry"""
        return {
            "initialized": self.initialized,
            "entity_count": len(self.known_entity_types),
            "entity_types": list(self.known_entity_types)[:10],  # First 10 for brevity
            "has_more_entities": len(self.known_entity_types) > 10
        }