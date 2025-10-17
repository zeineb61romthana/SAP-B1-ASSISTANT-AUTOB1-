# agents/query_understanding.py (entityextraction phase)

from typing import Dict, Any, List
from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain.schema import StrOutputParser
from langchain.output_parsers import PydanticOutputParser
from collections import Counter
from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import pickle
import os
from datetime import datetime
from utils.dynamic_time_resolver import DynamicTimeResolver
from pydantic import BaseModel, Field
import json

import asyncio

class StructuredSAPQuery(BaseModel):
    """Structured representation of an SAP B1 query."""
    entity_type: str = Field(..., description="The SAP business object entity type (e.g., Items, BusinessPartners)")
    filter_conditions: list = Field(default_factory=list, description="List of filter conditions to apply")
    fields: list = Field(default_factory=list, description="Fields to retrieve (empty for all fields)")
    top: int = Field(default=50, description="Maximum number of records to retrieve")
    skip: int = Field(default=0, description="Number of records to skip for pagination")
    order_by: str = Field(default="", description="Field to use for ordering results")
    expand: list = Field(default_factory=list, description="Related entities to expand in the response")
    count_only: bool = Field(default=False, description="Return only count of records")
    include_count: bool = Field(default=False, description="Include count with results")

class DynamicExampleStore:
    """Real AI: Learn from successful queries using semantic similarity"""
    
    def __init__(self):
        self.embedding_model = SentenceTransformer('all-mpnet-base-v2')
        self.examples = []
        self.embeddings = None
        self.store_file = "knowledge/dynamic_examples.pkl"
        self.max_examples = 100  # Keep best 100 examples
        self._load_examples()
    
    def add_successful_example(self, query: str, structured_result: dict, confidence: float):
        """Add a successful example for future learning"""
        if confidence < 0.7:  # Only learn from high-confidence examples
            return
            
        example = {
            "query": query,
            "entity_type": structured_result.get("entity_type"),
            "filter_conditions": structured_result.get("filter_conditions", []),
            "confidence": confidence,
            "timestamp": datetime.now().isoformat()
        }
        
        # Add example and update embeddings
        self.examples.append(example)
        self._update_embeddings()
        
        # Keep only best examples
        if len(self.examples) > self.max_examples:
            self._prune_examples()
        
        self._save_examples()
    
    def get_relevant_examples(self, query: str, top_k: int = 3) -> list:
        """Use semantic similarity to find most relevant examples"""
        if not self.examples or self.embeddings is None:
            return []
        
        # Get query embedding
        query_embedding = self.embedding_model.encode([query])
        
        # Calculate similarities
        similarities = cosine_similarity(query_embedding, self.embeddings)[0]
        
        # Get top-k most similar examples
        top_indices = np.argsort(similarities)[-top_k:][::-1]
        
        relevant_examples = []
        for idx in top_indices:
            example = self.examples[idx].copy()
            example["similarity"] = similarities[idx]
            relevant_examples.append(example)
        
        return relevant_examples
    
    def _update_embeddings(self):
        """Update embeddings for all examples"""
        if not self.examples:
            return
            
        queries = [ex["query"] for ex in self.examples]
        self.embeddings = self.embedding_model.encode(queries)
    
    def _prune_examples(self):
        """Keep only the best examples based on confidence and recency"""
        # Sort by confidence (descending) and keep top examples
        self.examples.sort(key=lambda x: (x["confidence"], x["timestamp"]), reverse=True)
        self.examples = self.examples[:self.max_examples]
        self._update_embeddings()
    
    def _save_examples(self):
        """Save examples to disk"""
        try:
            os.makedirs(os.path.dirname(self.store_file), exist_ok=True)
            with open(self.store_file, 'wb') as f:
                pickle.dump({
                    'examples': self.examples,
                    'embeddings': self.embeddings
                }, f)
        except Exception as e:
            print(f"Warning: Could not save examples: {e}")
    
    def _load_examples(self):
        """Load examples from disk"""
        try:
            if os.path.exists(self.store_file):
                with open(self.store_file, 'rb') as f:
                    data = pickle.load(f)
                    self.examples = data.get('examples', [])
                    self.embeddings = data.get('embeddings', None)
                print(f"Loaded {len(self.examples)} examples for few-shot learning")
        except Exception as e:
            print(f"Warning: Could not load examples: {e}")
            self.examples = []
            self.embeddings = None

class QueryUnderstandingAgent:

    def __init__(self, entity_registry_integration=None):
        # Store the entity registry integration
        self.entity_registry = entity_registry_integration
        self.time_resolver = DynamicTimeResolver()
        print("🎓 ACADEMIC: Time resolution with AI intelligence initialized")
        self.example_store = DynamicExampleStore()
        self.confidence_threshold = 0.75
        
        # Define the prompt for understanding user queries - fixed to avoid {field} references
        self.prompt = ChatPromptTemplate.from_template("""
        You are an expert in translating natural language queries into structured SAP B1 OData queries.
        
        Given the following user query about SAP B1 data, extract the relevant information to structure an OData request.
        
        User Query: {query}
        
        Available Entity Types: {available_entity_types}
        
        Return a JSON structure with the following information:
        - entity_type: The SAP B1 entity type to query
        - filter_conditions: List of filter conditions to apply
        - fields: List of specific fields to retrieve
        - top: Maximum number of records to retrieve (default: 50)
        - skip: Number of records to skip for pagination (default: 0)
        - order_by: Field to use for ordering results (default: empty string)
        - expand: Related entities to expand in the response (default: empty list)
        - count_only: Set to true if user only wants the count (default: false)
        - include_count: Set to true if user wants count with results (default: false)

        Count Detection Rules:
        - If query contains "count", "how many", "number of", set count_only=true
        - If query contains "count of" or "total number", set count_only=true
        - If query asks for both data and count, set include_count=true
        
        For filter conditions, use the following format:
        {{"field": "FieldName", "operator": "eq", "value": "Value"}}
        
        If there are multiple conditions, they will be combined with AND logic.
        
        IMPORTANT: For customer queries, use the following field mappings:
        - Customer ID/code: CardCode
        - Customer name: CardName
        - Customer type: CardType (use 'C' for customers)
        - Phone: Phone1
        - Email: EmailAddress
        
        EXAMPLE: For a query "Get customer XYZ", use filter_conditions:
        [{{"field": "CardCode", "operator": "eq", "value": "XYZ"}}, {{"field": "CardType", "operator": "eq", "value": "C"}}]
        
        OR: For a query "Get customer named XYZ", use filter_conditions:
        [{{"field": "CardName", "operator": "eq", "value": "XYZ"}}, {{"field": "CardType", "operator": "eq", "value": "C"}}]
        
        Field Information for {highlighted_entity_type}:
        {field_information}
        
        Response in valid JSON format:
        """)
        
        # Initialize the LLM with an appropriate model
        # Use GPT-3.5-turbo as a fallback if gpt-4-turbo is not available
        try:
            self.llm = ChatOpenAI(model="gpt-4-turbo", temperature=0)
        except Exception:
            try:
                self.llm = ChatOpenAI(model="gpt-4", temperature=0)
            except Exception:
                # Fallback to GPT-3.5-turbo
                self.llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)
        
        # Initialize the output parser
        self.output_parser = PydanticOutputParser(pydantic_object=StructuredSAPQuery)
    
    async def _get_entity_types(self) -> str:
        """Get all available entity types from the entity registry"""
        if not self.entity_registry:
            # Return a default list if no registry is available
            return "Items, BusinessPartners, Documents, Invoices, Orders, Quotations"
        
        try:
            entity_types = await self.entity_registry.get_all_entity_types()
            return ", ".join(entity_types)
        except Exception as e:
            print(f"Error getting entity types: {str(e)}")
            # Return a default list if there's an error
            return "Items, BusinessPartners, Documents, Invoices, Orders, Quotations"
    
    async def _get_field_information(self, entity_type: str) -> str:
        """Get field information for an entity type from the entity registry"""
        if not self.entity_registry or not entity_type:
            return ""
        
        try:
            schema = await self.entity_registry.get_entity_schema(entity_type)
            
            # Extract properties from schema
            properties = schema.get("properties", [])
            field_info = []
            
            # Handle different schema formats
            if isinstance(properties, list):
                if all(isinstance(prop, str) for prop in properties):
                    # Simple list of property names
                    field_info = properties
                elif all(isinstance(prop, dict) for prop in properties):
                    # List of property objects
                    for prop in properties:
                        if "name" in prop:
                            field_name = prop["name"]
                            prop_type = prop.get("type", "")
                            field_info.append(f"{field_name} ({prop_type})")
            elif isinstance(properties, dict):
                # Dictionary of properties
                for prop_name, prop_value in properties.items():
                    field_info.append(prop_name)
            
            # Format field information
            if field_info:
                return "Available fields: " + ", ".join(field_info)
            else:
                return ""
        except Exception as e:
            print(f"Error getting field information: {str(e)}")
            return ""
    
    async def _determine_entity_type(self, query: str) -> str:
        """Try to determine the entity type from the query"""
        if not self.entity_registry:
            return ""
        
        try:
            return await self.entity_registry.suggest_entity_type(query)
        except Exception as e:
            print(f"Error determining entity type: {str(e)}")
            return ""   
    
    def _build_dynamic_prompt(self, query: str, available_entity_types: str,  
                          highlighted_entity_type: str, field_information: str,
                          time_context: str, current_date: str, time_entities: dict = None) -> str:
        """Builds a dynamic prompt including resolved time context and natural language time description."""
        # Extract natural language time description if present
        time_desc = time_entities.get("_time_description", "") if time_entities else ""

        # Core prompt header
        prompt = f"""
    You are an SAP B1 query understanding expert. Current date: {current_date}

    RESOLVED TIME CONTEXT:
    {time_context}

    {time_desc}  # natural language time description

    USER QUERY: "{query}"

    SUGGESTED ENTITY TYPE: {highlighted_entity_type}

    Available Entity Types: {available_entity_types}

    Field Information for {highlighted_entity_type}:
    {field_information}

    """
        # Append learned examples section
        relevant_examples = self.example_store.get_relevant_examples(query, top_k=3)
        if relevant_examples:
            prompt += "\nLEARNED EXAMPLES (most similar to your query):\n"
            for i, example in enumerate(relevant_examples, 1):
                prompt += (
                    f"\nExample {i} (similarity: {example['similarity']:.3f}):\n"
                    f"Query: \"{example['query']}\"\n"
                    f"Entity: {example['entity_type']}\n"
                    f"Filters: {example['filter_conditions']}\n"
                )

        # Append instruction to return JSON
        prompt += (
            "\nBased on the context above, extract information for this query.\n"
            "Return a JSON object with the following keys:\n"
            "- entity_type: The SAP B1 entity type\n"
            "- filter_conditions: List of {\"field\": \"FieldName\", \"operator\": \"eq\", \"value\": \"Value\"}\n"
            "- fields: List of specific fields (empty for all fields)\n"
            "- top: Maximum records (default: 50)\n"
            "- skip: Records to skip (default: 0)\n"
            "- order_by: Field to order by (default: empty)\n"
            "- expand: Related entities (default: empty list)\n"
            "\nResponse in valid JSON format without additional explanation."
        )
        return prompt
    
    def _calculate_dynamic_confidence(self, query: str) -> float:
        """Calculate confidence based on similarity to learned examples"""
        base_confidence = 0.75  # Base LLM confidence
        
        # Get similarity to best examples
        relevant_examples = self.example_store.get_relevant_examples(query, top_k=1)
        
        if relevant_examples:
            best_similarity = relevant_examples[0]["similarity"]
            # Boost confidence if similar to successful examples
            similarity_boost = min(0.15, best_similarity * 0.15)
            final_confidence = min(0.95, base_confidence + similarity_boost)
        else:
            final_confidence = base_confidence
        
        return final_confidence
    
    async def _enhance_query_with_registry(self, structured_query: Dict[str, Any], query: str) -> Dict[str, Any]:
        """Enhance a structured query with information from the entity registry"""
        if not self.entity_registry:
            return structured_query
        
        try:
            # Map entity type if needed
            entity_type = structured_query.get("entity_type", "")
            if entity_type:
                mapped_entity_type = self.entity_registry.map_entity_type(entity_type)
                if mapped_entity_type != entity_type:
                    structured_query["entity_type"] = mapped_entity_type
            
            # Map field names in filter conditions
            if entity_type:
                for condition in structured_query.get("filter_conditions", []):
                    if isinstance(condition, dict) and "field" in condition:
                        original_field = condition["field"]
                        mapped_field = await self.entity_registry.map_field_name(entity_type, original_field)
                        if mapped_field != original_field:
                            condition["field"] = mapped_field
            
            return structured_query
        except Exception as e:
            print(f"Error enhancing query with registry: {str(e)}")
            return structured_query
    
    def _ensure_customer_filters(self, query_text: str, structured_query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure customer queries have proper filter conditions.
        This handles cases like "Show me details for customer XYZ".
        """
        query_lower = query_text.lower()
        
        # Check if this is a customer query
        if "customer" in query_lower and structured_query.get("entity_type") == "BusinessPartners":
            has_card_filter = False
            has_card_type = False
            
            # Check existing filter conditions
            for condition in structured_query.get("filter_conditions", []):
                if condition.get("field") in ["CardName", "CardCode"]:
                    has_card_filter = True
                if condition.get("field") == "CardType":
                    has_card_type = True
            
            # Extract customer identifier
            if not has_card_filter:
                # Simple extraction for "customer XYZ" pattern
                import re
                customer_pattern = re.compile(r"customer\s+([a-zA-Z0-9_]+)", re.IGNORECASE)
                match = customer_pattern.search(query_text)
                
                if match:
                    customer_id = match.group(1)
                    # Use CardName by default for simple queries
                    structured_query.setdefault("filter_conditions", []).append({
                        "field": "CardName", 
                        "operator": "eq", 
                        "value": customer_id
                    })
                    print(f"Added CardName filter for {customer_id}")
            
            # Always ensure CardType filter for customer queries
            if not has_card_type:
                structured_query.setdefault("filter_conditions", []).append({
                    "field": "CardType", 
                    "operator": "eq", 
                    "value": "C"
                })
                print("Added CardType filter for customer query")
        
        return structured_query


    def _is_valid_date_format(self, date_str: str) -> bool:
        """Check if date is in YYYY-MM-DD format."""
        import re
        return bool(re.match(r'^\d{4}-\d{2}-\d{2}$', str(date_str)))

    def _convert_to_sap_date(self, date_value: str) -> str:
        """Convert any date format to SAP B1 YYYY-MM-DD format."""
        from datetime import datetime, timedelta
        
        # Handle common time expressions
        if str(date_value).lower() == "today":
            return datetime.now().strftime("%Y-%m-%d")
        elif str(date_value).lower() == "yesterday":
            return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        elif str(date_value).lower() == "tomorrow":
            return (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        # If already in correct format, return as-is
        if self._is_valid_date_format(date_value):
            return date_value
        
        # Try to parse and reformat
        try:
            # Try various formats
            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d', '%B %d, %Y']:
                try:
                    dt = datetime.strptime(str(date_value), fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
        except Exception:
            pass
        
        # Fallback to today if parsing fails
        return datetime.now().strftime("%Y-%m-%d")
    
    def get_learning_stats(self) -> dict:
        """Get statistics about the learning process"""
        return {
            "total_examples": len(self.example_store.examples),
            "recent_examples": len([ex for ex in self.example_store.examples 
                                  if (datetime.now() - datetime.fromisoformat(ex["timestamp"])).days < 7]),
            "avg_confidence": np.mean([ex["confidence"] for ex in self.example_store.examples]) if self.example_store.examples else 0,
            "entity_distribution": self._get_entity_distribution()
        }
    
    def _get_entity_distribution(self) -> dict:
        """Get distribution of learned entities"""
        entities = [ex["entity_type"] for ex in self.example_store.examples if ex["entity_type"]]
        return dict(Counter(entities))
    
    def _detect_count_intent(self, query_text: str, structured_query: Dict[str, Any]) -> Dict[str, Any]:
        """Detect if user wants count information"""
        query_lower = query_text.lower()
        
        # Count-only patterns
        count_only_patterns = [
            r'\bhow many\b',
            r'\bcount\s+(?:of\s+)?(?:all\s+)?(?:the\s+)?\w+',
            r'\bnumber of\b',
            r'\btotal\s+(?:number\s+)?(?:of\s+)?\w+',
            r'\bcount$',
            r'^count\b'
        ]
        
        # Include count patterns  
        include_count_patterns = [
            r'\bwith count\b',
            r'\band count\b',
            r'\binclude count\b',
            r'\bshow count\b'
        ]
        
        import re
        
        # Check for count-only intent
        for pattern in count_only_patterns:
            if re.search(pattern, query_lower):
                structured_query["count_only"] = True
                structured_query["top"] = 0  # Don't need actual records
                break
        
        # Check for include count intent
        if not structured_query.get("count_only", False):
            for pattern in include_count_patterns:
                if re.search(pattern, query_lower):
                    structured_query["include_count"] = True
                    break
        
        return structured_query


    async def async_invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhanced entity extraction with AI-augmented prompt, time preprocessing,
        entity registry suggestion, and LLM fallback logic + SCHEMA VALIDATION
        """
        try:
            # ========== STEP 1: TIME RESOLUTION (MUST BE FIRST) ==========
            print(f"🕰️ Step 1: Extracting time expressions from query: '{state['query']}'")
            time_entities = self.time_resolver.extract_time_expressions(state['query'])
            # Filter out internal or underscored entries
            filtered_time = {k: v for k, v in time_entities.items() if not k.startswith("_")}
            time_context = json.dumps(filtered_time, indent=2)

            # ========== STEP 2: ENTITY REGISTRY ==========
            print("🔍 Step 2: Suggesting entity type from registry")
            entity_type = await self.entity_registry.suggest_entity_type(state['query'])

            # ========== STEP 3: BUILD PROMPT WITH RESOLVED CONTEXT ==========
            print("🛠️ Step 3: Building enhanced prompt")
            current_date = datetime.now().strftime("%Y-%m-%d")
            available_entity_types = await self._get_entity_types()
            field_information = await self._get_field_information(entity_type)
            enhanced_prompt = self._build_dynamic_prompt(
                query=state['query'],
                available_entity_types=available_entity_types,
                highlighted_entity_type=entity_type,
                field_information=field_information,
                time_context=time_context,
                current_date=current_date
            )

            # ========== STEP 4: LLM PROCESSING AND FALLBACK ==========
            print("🚀 Step 4: Invoking LLM with enhanced prompt")
            try:
                response = await self.llm.ainvoke([{"role": "user", "content": enhanced_prompt}])
                raw = response.content

                # Attempt to parse JSON directly
                try:
                    structured_query = json.loads(raw)
                except json.JSONDecodeError:
                    # Fallback to regex extraction of JSON block
                    import re
                    pattern = r'```json\s*([\s\S]*?)\s*```|^\s*(\{[\s\S]*\})\s*$'
                    match = re.search(pattern, raw)
                    if match:
                        snippet = match.group(1) or match.group(2)
                        structured_query = json.loads(snippet)
                    else:
                        raise ValueError("Could not extract JSON from LLM response")

                # Post-process structured query
                structured_query = self._ensure_customer_filters(state['query'], structured_query)
                structured_query = await self._enhance_query_with_registry(structured_query, state['query'])
                
                # ========== NEW: CRITICAL SCHEMA VALIDATION STEP ==========
                print("🔍 Step 5: Schema validation and field mapping correction")
                if hasattr(self.entity_registry, 'validate_and_fix_structured_query'):
                    original_query = state['query']
                    validated_query = await self.entity_registry.validate_and_fix_structured_query(
                        structured_query, original_query
                    )
                    structured_query = validated_query
                    print("✅ Schema validation completed")
                else:
                    print("⚠️ Schema validation not available")
                
                # ========== NEW: COUNT INTENT DETECTION ==========
                print("🔍 Step 6: Detecting count intent")
                structured_query = self._detect_count_intent(state['query'], structured_query)
                if structured_query.get('count_only'):
                    print("✅ Count-only query detected")
                elif structured_query.get('include_count'):
                    print("✅ Include-count query detected")
                confidence = self._calculate_dynamic_confidence(state['query'])

                # Update state with results
                state.update({
                    'structured_query': structured_query,
                    'confidence': confidence,
                    'method_used': 'dynamic_few_shot_llm_with_validation'
                })
                print(f"✅ LLM succeeded: entity_type={structured_query.get('entity_type')} (conf={confidence:.3f})")

                # Learn from high-confidence examples
                if confidence >= self.confidence_threshold:
                    self.example_store.add_successful_example(
                        state['query'], structured_query, confidence
                    )
                    print(f"📚 Example saved (confidence: {confidence:.3f})")

                return state

            except Exception as llm_err:
                print(f"❌ LLM processing failed: {llm_err}")

            # ========== FINAL FAILURE ==========
            error_msg = "Failed to understand query with all available methods"
            print(f"❌ {error_msg}")
            state['error'] = {
                'stage': 'query_understanding',
                'message': error_msg,
                'can_retry': False
            }
            state['output'] = f"Error understanding your query: {error_msg}"
            return state

        except Exception as e:
            # Unexpected exception handler
            import traceback
            print(f"❌ Unexpected error: {e}\n{traceback.format_exc()}")
            state['error'] = {
                'stage': 'query_understanding',
                'message': str(e),
                'can_retry': False
            }
            state['output'] = f"Error understanding your query: {e}"
            return state

    
    def invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Process the user query and extract structured information."""
        return asyncio.run(self.async_invoke(state))