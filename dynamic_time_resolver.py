# utils/dynamic_time_resolver.py - ENHANCED WITH AI INTELLIGENCE

import re
import json
import time
from datetime import datetime, timedelta, date
import calendar
from dateutil.relativedelta import relativedelta
from typing import Dict, Any, Optional
import logging

# ADD these new imports for AI functionality
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class DynamicTimeResolver:
    """
    ENHANCED: AI-Augmented Dynamic Time Resolver
    Academic Project: Single point of entry for all time resolution
    """
    
    def __init__(self):
        self.now = datetime.now()
        self.current_year = self.now.year
        self.current_month = self.now.month
        self.current_day = self.now.day
        
        # ENHANCED: Add AI capabilities
        try:
            self.llm = ChatOpenAI(model="gpt-4", temperature=0.1)
        except Exception:
            try:
                self.llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0.1)
            except Exception as e:
                logger.warning(f"AI not available: {e}")
                self.llm = None
        # ENHANCED: Intelligent caching system
        self.exact_cache = {}
        self.ai_enhanced_patterns = {}
        
        # Mapping of month names to numbers
        self.month_names = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12,
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6,
            'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
        }
        
        # ENHANCED: Core patterns with additional complex patterns
        self.date_expressions = {
            # Original patterns
            r'yesterday': self._get_yesterday,
            r'today': self._get_today,
            r'tomorrow': self._get_tomorrow,
            r'last\s+month': self._get_last_month,
            r'this\s+month': self._get_this_month,
            r'next\s+month': self._get_next_month,
            r'last\s+week': self._get_last_week,
            r'this\s+week': self._get_this_week,
            r'next\s+week': self._get_next_week,
            r'last\s+year': self._get_last_year,
            r'this\s+year': self._get_this_year,
            r'next\s+year': self._get_next_year,
            r'last\s+(\d+)\s+days?': self._get_last_n_days,
            r'next\s+(\d+)\s+days?': self._get_next_n_days,
            r'(\d{4})-(\d{1,2})-(\d{1,2})': self._get_exact_date,
            r'(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})': self._get_formatted_date,
            r'(january|february|march|april|may|june|july|august|september|october|november|december)': self._get_month,
            r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)': self._get_month_abbrev,
            r'q([1-4])': self._get_quarter,
            
            # ENHANCED: Additional patterns for complex queries
            r'(?:last|past|previous)\s+(\d+)\s+weeks?': self._get_last_n_weeks,
            r'(?:next|coming|following)\s+(\d+)\s+weeks?': self._get_next_n_weeks,
            r'(?:last|previous)\s+(?:business|work)\s+week': self._get_last_business_week,
            r'(?:this|current)\s+(?:business|work)\s+week': self._get_this_business_week,
        }
        
        # ENHANCED: AI prompt for complex queries
        self.ai_prompt = ChatPromptTemplate.from_template("""
        You are a time expression expert for SAP B1 business system. Current date: {current_date}
        
        Task: Parse this time expression into precise date range for business queries.
        
        Query: "{query}"
        
        Context: SAP B1 business system where users query:
        - Document dates (invoices, orders, purchase orders)
        - Business periods (quarters, fiscal years)
        - Relative dates (last week, past 30 days)
        
        Rules:
        1. Return dates in YYYY-MM-DD format
        2. For single dates, start_date = end_date
        3. For ranges, provide both start_date and end_date
        4. Business context: Monday = start of week
        5. Be precise with date boundaries
        
        Response format (JSON only):
        {{
            "success": true/false,
            "range_type": "exact" or "range",
            "start_date": "YYYY-MM-DD",
            "end_date": "YYYY-MM-DD", 
            "confidence": 0.0-1.0,
            "reasoning": "brief explanation"
        }}
        
        Examples:
        - "last 2 weeks" → range from 14 days ago to today
        - "between July 1st and July 10th 2025" → exact range
        - "Q1 2025" → Jan 1 to Mar 31, 2025
        """)
    
    def extract_time_expressions(self, query: str) -> Dict[str, Any]:
        """
        ENHANCED: Main entry point with intelligent AI routing.
        Maintains backward compatibility while adding AI intelligence.
        """
        start_time = time.time()
        print(f"🔍 Query: '{query}'")

        # Initialize result variable
        result: Dict[str, Any] = {}

        # Stage 1: Exact Cache Lookup (fastest)
        if query in self.exact_cache:
            result = self.exact_cache[query]
            execution_time = (time.time() - start_time) * 1000

        else:
            # Stage 2: Enhanced rule-based pattern matching
            rule_entities = self._try_enhanced_rule_matching(query)
            if rule_entities:
                result = rule_entities
                self.exact_cache[query] = result
                execution_time = (time.time() - start_time) * 1000
            else:
                # Stage 3: AI-Enhanced Resolution for complex queries
                if self._should_use_ai_enhancement(query):
                    ai_result = self._try_ai_enhanced(query)
                    if ai_result:
                        result = ai_result
                        self._learn_from_ai_success(query, result)
                        self.exact_cache[query] = result
                        execution_time = (time.time() - start_time) * 1000
                        print(f"📊 AI Success! Strategy: ai_enhanced, Time: {execution_time:.2f}ms, Confidence: 0.850, Cost: $0.01")

        # Stage 4: Fallback (empty result for backward compatibility)
        if not result:
            execution_time = (time.time() - start_time) * 1000
            print(f"❌ Resolution Failed! Time: {execution_time:.2f}ms")

        # ✅ CHANGE 4: Add LLM-friendly natural language description if DocDate present
        if "DocDate" in result:
            date_info = result["DocDate"]
            if date_info.get("range") == "exact":
                result["_time_description"] = f"on {date_info['start']}"
            else:
                result["_time_description"] = f"between {date_info['start']} and {date_info['end']}"

        # FILTER INTERNAL KEYS BEFORE RETURNING
        filtered = {k: v for k, v in result.items() if not k.startswith('_')}
        return filtered

    
    def _try_enhanced_rule_matching(self, query):
        """Enhanced rule matching with date range support"""
        # NEW: If query contains specific date patterns, force AI
        if re.search(r'\b\w+\s+\d{1,2}(?:st|nd|rd|th)\s+\d{4}\b.*and.*\b\w+\s+\d{1,2}(?:st|nd|rd|th)\s+\d{4}\b', query, re.IGNORECASE):
            return {}  # Force AI usage
        
        # ENHANCED: First check for date range patterns (between X and Y)
        range_patterns = [
            r'between\s+(.+?)\s+and\s+(.+?)(?:\s*$)',
            r'from\s+(.+?)\s+to\s+(.+?)(?:\s*$)',
        ]

        
        for pattern in range_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                start_text = match.group(1).strip()
                end_text = match.group(2).strip()
                
                print(f"🔍 DEBUG: Found date range: '{start_text}' to '{end_text}'")
                
                # Parse each date
                start_date = self._parse_flexible_date(start_text)
                end_date = self._parse_flexible_date(end_text)
                
                if start_date and end_date:
                    time_entities = {
                        "DocDate": {
                            "range": "custom_range",
                            "start": self._format_date(start_date),
                            "end": self._format_date(end_date)
                        }
                    }
                    print(f"🔍 DEBUG: Successfully parsed range: {time_entities['DocDate']}")
                    return time_entities
                else:
                    print(f"🔍 DEBUG: Failed to parse one or both dates")
        
        # ENHANCED: Then check existing single date expressions
        for pattern, handler in self.date_expressions.items():
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                try:
                    time_entities = {"DocDate": handler(match)}
                    logger.info(f"Found time expression: '{match.group(0)}' => {time_entities['DocDate']}")
                    return time_entities
                except Exception as e:
                    logger.warning(f"Handler failed for pattern {pattern}: {e}")
                    continue
        
        return {}
    
    def _should_use_ai_enhancement(self, query: str) -> bool:
        """Intelligent decision: when does AI add value?"""
        if not self.llm:
            return False  # No AI available
            
        complexity_indicators = [
            r'\bbetween\s+.+\s+and\s+',  # Date ranges
            r'\blast\s+\d+\s+',  # "last N days/weeks"
            r'\bbusiness\s+',  # Business calendar
            r'\bfiscal\s+',  # Fiscal periods
            r'\bquarter\s+',  # Quarter references
            r'\b\w+\s+\d{1,2}(?:st|nd|rd|th)\s+\d{4}',  # "July 1st 2025"
            r'\bprevious\s+',  # Previous periods
            r'\bpast\s+',  # Past periods
            r'\bmonday|tuesday|wednesday|thursday|friday|saturday|sunday\b',  # Weekdays
        ]
        
        # NEW: Always use AI for complex patterns
        return any(re.search(indicator, query, re.IGNORECASE) for indicator in complexity_indicators)
    
    def _try_ai_enhanced(self, query: str) -> Optional[Dict[str, Any]]:
        """Use AI to enhance or replace rule-based result"""
        if not self.llm:
            return None
            
        try:
            current_date = datetime.now().strftime("%Y-%m-%d")
            
            response = self.llm.invoke(
                self.ai_prompt.format(
                    current_date=current_date,
                    query=query
                )
            )
            
            result = json.loads(response.content)
            
            if result.get("success", False):
                # Convert AI response to our format
                time_entity = {
                    "range": result["range_type"],
                    "start": result["start_date"],
                    "end": result["end_date"]
                }
                
                return {"DocDate": time_entity}
            
        except Exception as e:
            logger.error(f"AI enhancement failed: {e}")
        
        return None
    
    def _learn_from_ai_success(self, query: str, ai_result: Dict[str, Any]):
        """Learn patterns from successful AI resolutions"""
        pattern_candidates = [
            r'\blast\s+\d+\s+weeks?\b',
            r'\bbetween\s+\w+.*and\s+\w+',
            r'\bbusiness\s+week\b',
            r'\bprevious\s+\w+',
        ]
        
        for candidate in pattern_candidates:
            if re.search(candidate, query, re.IGNORECASE):
                if candidate not in self.ai_enhanced_patterns:
                    self.ai_enhanced_patterns[candidate] = []
                
                self.ai_enhanced_patterns[candidate].append({
                    "query": query,
                    "result": ai_result,
                    "confidence": 0.8
                })
                break
    
    def _parse_flexible_date(self, date_text):
        """Parse various date formats including month names"""
        date_text = date_text.strip()
        
        # Handle "July 1st 2025" format
        month_date_pattern = r'(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?\s+(\d{4})'
        match = re.match(month_date_pattern, date_text, re.IGNORECASE)
        if match:
            month_name, day, year = match.groups()
            month_num = self.month_names.get(month_name.lower())
            if month_num:
                try:
                    return datetime(int(year), month_num, int(day))
                except ValueError:
                    logger.warning(f"Invalid date: {month_name} {day}, {year}")
        
        # Handle ISO dates (2025-07-01)
        if re.match(r'\d{4}-\d{2}-\d{2}', date_text):
            try:
                return datetime.strptime(date_text, '%Y-%m-%d')
            except ValueError:
                pass
        
        # Handle MM/DD/YYYY
        if re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_text):
            try:
                return datetime.strptime(date_text, '%m/%d/%Y')
            except ValueError:
                pass
        
        return None
    
    # ENHANCED: Add missing pattern handlers
    def _get_last_n_weeks(self, match):
        """Get date range for last N weeks"""
        weeks = int(match.group(1))
        end_date = self.now
        start_date = end_date - timedelta(weeks=weeks)
        return {
            "range": f"last_{weeks}_weeks",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_next_n_weeks(self, match):
        """Get date range for next N weeks"""
        weeks = int(match.group(1))
        start_date = self.now
        end_date = start_date + timedelta(weeks=weeks)
        return {
            "range": f"next_{weeks}_weeks",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_last_business_week(self, match):
        """Get last business week (Monday to Friday)"""
        today = self.now
        # Get last Monday
        days_since_monday = today.weekday() + 7  # Add 7 to go to previous week
        last_monday = today - timedelta(days=days_since_monday)
        last_friday = last_monday + timedelta(days=4)
        
        return {
            "range": "last_business_week",
            "start": self._format_date(last_monday),
            "end": self._format_date(last_friday)
        }
    
    def _get_this_business_week(self, match):
        """Get this business week (Monday to Friday)"""
        today = self.now
        # Get this Monday
        this_monday = today - timedelta(days=today.weekday())
        this_friday = this_monday + timedelta(days=4)
        
        return {
            "range": "this_business_week",
            "start": self._format_date(this_monday),
            "end": self._format_date(this_friday)
        }
    
    # Keep all existing methods unchanged for backward compatibility
    def _format_date(self, dt):
        """Format date in ISO 8601 format (YYYY-MM-DD)"""
        return dt.strftime("%Y-%m-%d")
    
    def _get_today(self, *args):
        """Get today's date"""
        date_str = self._format_date(self.now)
        return {"range": "exact", "start": date_str, "end": date_str}
    
    def _get_yesterday(self, *args):
        """Get yesterday's date"""
        yesterday = self.now - timedelta(days=1)
        date_str = self._format_date(yesterday)
        return {"range": "exact", "start": date_str, "end": date_str}
    
    def _get_tomorrow(self, *args):
        """Get tomorrow's date"""
        tomorrow = self.now + timedelta(days=1)
        date_str = self._format_date(tomorrow)
        return {"range": "exact", "start": date_str, "end": date_str}
    
    def _get_last_month(self, *args):
        """Get date range for last month"""
        last_month = self.now - relativedelta(months=1)
        start_date = date(last_month.year, last_month.month, 1)
        end_date = date(last_month.year, last_month.month, 
                        calendar.monthrange(last_month.year, last_month.month)[1])
        return {
            "range": "last_month",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_this_month(self, *args):
        """Get date range for current month"""
        start_date = date(self.current_year, self.current_month, 1)
        end_date = date(self.current_year, self.current_month, 
                        calendar.monthrange(self.current_year, self.current_month)[1])
        return {
            "range": "this_month",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_next_month(self, *args):
        """Get date range for next month"""
        next_month = self.now + relativedelta(months=1)
        start_date = date(next_month.year, next_month.month, 1)
        end_date = date(next_month.year, next_month.month, 
                        calendar.monthrange(next_month.year, next_month.month)[1])
        return {
            "range": "next_month",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_last_week(self, *args):
        """Get date range for last week"""
        start_date = self.now - timedelta(days=self.now.weekday() + 7)
        end_date = start_date + timedelta(days=6)
        return {
            "range": "last_week",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_this_week(self, *args):
        """Get date range for current week"""
        start_date = self.now - timedelta(days=self.now.weekday())
        end_date = start_date + timedelta(days=6)
        return {
            "range": "this_week",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_next_week(self, *args):
        """Get date range for next week"""
        start_date = self.now - timedelta(days=self.now.weekday()) + timedelta(days=7)
        end_date = start_date + timedelta(days=6)
        return {
            "range": "next_week",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_last_year(self, *args):
        """Get date range for last year"""
        start_date = date(self.current_year - 1, 1, 1)
        end_date = date(self.current_year - 1, 12, 31)
        return {
            "range": "last_year",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_this_year(self, *args):
        """Get date range for current year"""
        start_date = date(self.current_year, 1, 1)
        end_date = date(self.current_year, 12, 31)
        return {
            "range": "this_year",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_next_year(self, *args):
        """Get date range for next year"""
        start_date = date(self.current_year + 1, 1, 1)
        end_date = date(self.current_year + 1, 12, 31)
        return {
            "range": "next_year",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_last_n_days(self, match):
        """Get date range for last N days"""
        days = int(match.group(1))
        end_date = self.now
        start_date = end_date - timedelta(days=days)
        return {
            "range": f"last_{days}_days",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_next_n_days(self, match):
        """Get date range for next N days"""
        days = int(match.group(1))
        start_date = self.now
        end_date = start_date + timedelta(days=days)
        return {
            "range": f"next_{days}_days",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_exact_date(self, match):
        """Parse an exact date in YYYY-MM-DD format"""
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        
        try:
            dt = date(year, month, day)
            date_str = self._format_date(dt)
            return {"range": "exact", "start": date_str, "end": date_str}
        except ValueError:
            # Invalid date, return today as fallback
            logger.warning(f"Invalid date: {year}-{month}-{day}, using today as fallback")
            return self._get_today()
    
    def _get_formatted_date(self, match):
        """Parse a date in MM/DD/YYYY or similar formats"""
        # Assuming MM/DD/YYYY format - adjust as needed for your locale
        month = int(match.group(1))
        day = int(match.group(2))
        year = int(match.group(3))
        
        try:
            dt = date(year, month, day)
            date_str = self._format_date(dt)
            return {"range": "exact", "start": date_str, "end": date_str}
        except ValueError:
            # Invalid date, return today as fallback
            logger.warning(f"Invalid date: {month}/{day}/{year}, using today as fallback")
            return self._get_today()
    
    def _get_month(self, match):
        """Get date range for a specific month in current year"""
        month_name = match.group(1).lower()
        month_num = self.month_names.get(month_name)
        
        if not month_num:
            logger.warning(f"Unknown month name: {month_name}, using current month as fallback")
            return self._get_this_month()
            
        start_date = date(self.current_year, month_num, 1)
        end_date = date(self.current_year, month_num, 
                      calendar.monthrange(self.current_year, month_num)[1])
        
        return {
            "range": f"{month_name}_{self.current_year}",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
        
    def _get_month_abbrev(self, match):
        """Get date range for a specific month (abbreviated) in current year"""
        month_abbrev = match.group(1).lower()
        month_num = self.month_names.get(month_abbrev)
        
        if not month_num:
            logger.warning(f"Unknown month abbreviation: {month_abbrev}, using current month as fallback")
            return self._get_this_month()
            
        start_date = date(self.current_year, month_num, 1)
        end_date = date(self.current_year, month_num, 
                      calendar.monthrange(self.current_year, month_num)[1])
        
        return {
            "range": f"{month_abbrev}_{self.current_year}",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }
    
    def _get_quarter(self, match):
        """Get date range for a specific quarter in current year"""
        quarter = int(match.group(1))
        
        if quarter < 1 or quarter > 4:
            logger.warning(f"Invalid quarter: {quarter}, using current month as fallback")
            return self._get_this_month()
            
        start_month = (quarter - 1) * 3 + 1
        end_month = quarter * 3
        
        start_date = date(self.current_year, start_month, 1)
        end_date = date(self.current_year, end_month, 
                      calendar.monthrange(self.current_year, end_month)[1])
        
        return {
            "range": f"Q{quarter}_{self.current_year}",
            "start": self._format_date(start_date),
            "end": self._format_date(end_date)
        }


# ENHANCED: Keep backward compatibility
def extract_time_expressions(query):
    """
    ENHANCED: Backward compatible function with AI intelligence
    This is the main entry point used by your existing code
    """
    resolver = DynamicTimeResolver()
    return resolver.extract_time_expressions(query)  