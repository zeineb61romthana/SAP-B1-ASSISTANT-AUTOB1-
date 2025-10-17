# agents/intent_recognition_manager.py (SIMPLIFIED)

import logging
import time
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime

from .distilbert_intent_recognizer import DistilBERTIntentRecognizer
from .zero_shot_recognizer import ZeroShotIntentRecognizer

logger = logging.getLogger("IntentRecognitionManager")

class IntentRecognitionManager:
    """
    Simplified Intent Recognition: DistilBERT for speed, LLM for reliability.
    No more over-engineering.
    """
    
    def __init__(self, entity_registry=None, sap_client=None, llm=None, 
                 distilbert_model_path="sap_intent_model_trained"):
        self.entity_registry = entity_registry
        
        # Initialize DistilBERT (optional - works without it)
        try:
            self.distilbert = DistilBERTIntentRecognizer(distilbert_model_path)
            if not self.distilbert.is_available():
                logger.warning("DistilBERT not available, using LLM only")
                self.distilbert = None
        except Exception as e:
            logger.warning(f"DistilBERT initialization failed: {e}")
            self.distilbert = None
        
        # Initialize LLM fallback (required)
        self.llm_recognizer = ZeroShotIntentRecognizer(entity_registry)
        
        # Simple stats tracking
        self.stats = {
            "total_queries": 0,
            "distilbert_used": 0,
            "llm_used": 0,
            "start_time": datetime.now()
        }
    
    async def recognize_intent(self, query: str, **kwargs) -> Dict[str, Any]:
        """
        Simple 2-step process:
        1. Try DistilBERT if available and confident (>= 0.8)
        2. Otherwise use LLM (always works)
        """
        start_time = time.time()
        self.stats["total_queries"] += 1
        
        # Step 1: Try DistilBERT for speed
        if self.distilbert:
            try:
                result = self.distilbert.predict_intent(query)
                confidence = result.get("confidence", 0)
                
                # Use DistilBERT if highly confident
                if confidence >= 0.8:
                    result["method_used"] = "distilbert"
                    result["total_response_time_ms"] = (time.time() - start_time) * 1000
                    self.stats["distilbert_used"] += 1
                    
                    logger.info(f"DistilBERT: {result['intent']} (confidence: {confidence:.3f})")
                    return result
                else:
                    logger.info(f"DistilBERT low confidence ({confidence:.3f}), using LLM fallback")
                    
            except Exception as e:
                logger.warning(f"DistilBERT failed: {e}")
        
        # Step 2: LLM fallback (always use if DistilBERT unavailable/low confidence)
        try:
            result = await self.llm_recognizer.recognize_intent(query)
            result["method_used"] = "llm_fallback"
            result["total_response_time_ms"] = (time.time() - start_time) * 1000
            self.stats["llm_used"] += 1
            
            logger.info(f"LLM: {result['intent']} (confidence: {result.get('confidence', 0):.3f})")
            return result
            
        except Exception as e:
            logger.error(f"Both DistilBERT and LLM failed: {e}")
            return {
                "intent": "unknown",
                "confidence": 0.0,
                "method_used": "failed",
                "error": str(e),
                "total_response_time_ms": (time.time() - start_time) * 1000
            }
    
    def get_usage_statistics(self) -> Dict[str, Any]:
        """Simple stats for monitoring."""
        total = self.stats["total_queries"]
        if total == 0:
            return {"message": "No queries processed yet"}
        
        return {
            "total_queries": total,
            "distilbert_usage": f"{self.stats['distilbert_used']}/{total} ({self.stats['distilbert_used']/total*100:.1f}%)",
            "llm_usage": f"{self.stats['llm_used']}/{total} ({self.stats['llm_used']/total*100:.1f}%)",
            "session_duration": str(datetime.now() - self.stats["start_time"]),
            "distilbert_available": self.distilbert is not None
        }