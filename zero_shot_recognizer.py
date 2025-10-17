# agents/zero_shot_recognizer.py

import json
import logging
import traceback
from typing import Dict, Any, List, Optional
import asyncio
from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain.schema import StrOutputParser

logger = logging.getLogger("ZeroShotRecognizer")

class ZeroShotIntentRecognizer:
    
    def __init__(self, entity_registry_integration=None):
        self.entity_registry = entity_registry_integration
        
        # Initialize the LLM with an appropriate model
        try:
            self.llm = ChatOpenAI(model="gpt-4-turbo", temperature=0.1)
        except:
            try:
                self.llm = ChatOpenAI(model="gpt-4", temperature=0.1)
            except:
                # Fallback to GPT-3.5-turbo
                self.llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.1)
    
    async def generate_intent_descriptions(self):
        """Generate descriptions for possible intents based on entity registry"""
        intent_descriptions = {}
        
        # Default intents
        intent_descriptions.update({
            "BusinessPartners.FindCustomer": "Find a specific customer by name, ID, or other criteria",
            "BusinessPartners.ListCustomers": "List all customers, possibly with filters",
            "Items.FindItem": "Find a specific inventory item by code, name, or other criteria",
            "Items.ListItems": "List inventory items, possibly with filters",
            "Orders.FindSpecificOrder": "Find a specific order by document number",
            "Orders.FindOrdersByCustomer": "Find orders placed by a specific customer",
            "Invoices.FindInvoice": "Find a specific invoice by number or other criteria"
        })
        
        # Dynamically generate more intent descriptions based on entity registry
        if self.entity_registry:
            try:
                entity_types = await self.entity_registry.get_all_entity_types()
                
                for entity_type in entity_types:
                    # Skip already defined entities
                    if any(entity_type in intent for intent in intent_descriptions.keys()):
                        continue
                        
                    # Add standard intents for this entity type
                    intent_descriptions[f"{entity_type}.Find"] = f"Find a specific {entity_type} record"
                    intent_descriptions[f"{entity_type}.List"] = f"List {entity_type} records, possibly with filters"
                    
                    # Get schema for more specific intents
                    try:
                        schema = await self.entity_registry.get_entity_schema(entity_type)
                        
                        # If we have key fields, add FindBy intents
                        key_fields = schema.get("key_fields", [])
                        for field in key_fields:
                            intent_descriptions[f"{entity_type}.FindBy{field}"] = f"Find {entity_type} by {field}"
                    except Exception as e:
                        logger.error(f"Error getting schema for {entity_type}: {str(e)}")
            except Exception as e:
                logger.error(f"Error generating intent descriptions: {str(e)}")
        
        return intent_descriptions
    
    
    async def recognize_intent(self, query, force=False):
        """Use zero-shot learning to recognize intent"""
        try:
            # Generate intent descriptions
            intent_descriptions = await self.generate_intent_descriptions()
            
            # Create zero-shot prompt
            intent_options = "\n".join([f"- {intent}: {desc}" for intent, desc in intent_descriptions.items()])
            
            system_prompt = f"""
            You are an expert in understanding SAP B1 related queries. Analyze the query and determine which of the following intents best matches it:
            
            {intent_options}
            
            If none of these match well, determine the most appropriate new intent in the format: "EntityType.Action".
            
            Provide your answer as a JSON object with these fields:
            - intent: The most specific matching intent category
            - confidence: Your confidence level (0.0 to 1.0)
            - reasoning: Brief explanation of why this intent was chosen
            """
            
            user_prompt = f"Query: {query}\n\nDetermine the most appropriate intent category for this query."
            
            # Create the prompt template and execute
            prompt = ChatPromptTemplate.from_messages([
                ("system", system_prompt),
                ("user", user_prompt)
            ])
            
            # Execute the chain
            chain = prompt | self.llm | StrOutputParser()
            response = await chain.ainvoke({})
            
            try:
                # Parse the JSON response
                result = json.loads(response)
                
                # Adjust confidence if forced
                if force and result["confidence"] < 0.5:
                    result["confidence"] = 0.5
                    
                logger.info(f"Zero-shot recognition for '{query}': {result['intent']} (confidence: {result['confidence']})")
                return result
            except json.JSONDecodeError:
                logger.error(f"Failed to parse intent recognition response: {response}")
                return {
                    "intent": "unknown",
                    "confidence": 0.5 if force else 0.0,
                    "reasoning": "Failed to determine intent"
                }
                
        except Exception as e:
            logger.error(f"Error in zero-shot intent recognition: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                "intent": "unknown",
                "confidence": 0.0,
                "reasoning": "Error in recognition process"
            }