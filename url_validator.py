# utils/url_validator.py

import re
from typing import Dict, Any, List, Tuple, Optional, Union
from utils.enhanced_errors import URLValidationError, InvalidFilterError

class ODataURLValidator:
    """Validates SAP B1 OData URLs for common issues."""
    
    def __init__(self):
        # Common patterns for SAP B1 OData URLs
        self.entity_pattern = r'^/([A-Za-z0-9_]+)'
        self.filter_pattern = r'\$filter=([^&]+)'
        self.select_pattern = r'\$select=([^&]+)'
        self.expand_pattern = r'\$expand=([^&]+)'
        self.orderby_pattern = r'\$orderby=([^&]+)'
        
        # SAP B1 specific constants
        self.sap_constants = {
            "document_status": ["bost_Open", "bost_Close", "bost_Cancelled", "bost_Paid"],
            "boolean_values": ["tYES", "tNO"],
            "common_entities": ["Orders", "BusinessPartners", "Items", "Invoices", "PurchaseOrders"]
        }
    
    def validate_url(self, url: str) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Validate an OData URL for common issues.
        
        Args:
            url: The OData URL to validate
            
        Returns:
            Tuple of (is_valid, issues) where issues is a list of validation issues
        """
        issues = []
        
        # Basic URL structure validation
        if not url.startswith('/'):
            issues.append({
                "type": "structure",
                "message": "URL should start with a forward slash",
                "severity": "error"
            })
        
        # Extract entity type
        entity_match = re.match(self.entity_pattern, url)
        if not entity_match:
            issues.append({
                "type": "entity",
                "message": "Could not identify entity type in URL",
                "severity": "error"
            })
            # Without entity we can't do much more validation
            return (len(issues) == 0, issues)
            
        entity_type = entity_match.group(1)
        
        # Check if entity type is known
        if entity_type not in self.sap_constants["common_entities"]:
            issues.append({
                "type": "entity",
                "message": f"Entity type '{entity_type}' is not in common entity list",
                "severity": "warning"
            })
        
        # Validate filter conditions
        filter_match = re.search(self.filter_pattern, url)
        if filter_match:
            filter_issues = self._validate_filter(filter_match.group(1), entity_type)
            issues.extend(filter_issues)
        
        # Validate string values have quotes
        string_value_issues = self._validate_string_values(url)
        issues.extend(string_value_issues)
        
        # Validate date formats
        date_issues = self._validate_date_formats(url)
        issues.extend(date_issues)
        
        # Check for missing parameters in URL templates
        if '{{' in url or '}}' in url:
            issues.append({
                "type": "template",
                "message": "URL contains unreplaced template parameters",
                "severity": "error"
            })
        
        return (len(issues) == 0, issues)
    
    def _validate_filter(self, filter_expr: str, entity_type: str) -> List[Dict[str, Any]]:
        """Validate filter expressions for common issues."""
        issues = []
        
        # Check for quoted numeric values (a common error)
        numeric_pattern = r"(\w+)\s+(?:eq|gt|lt|ge|le)\s+'(\d+(?:\.\d+)?)'(?:$|\s|&)"
        numeric_matches = re.finditer(numeric_pattern, filter_expr)
        
        for match in numeric_matches:
            field, value = match.groups()
            issues.append({
                "type": "filter",
                "message": f"Numeric value '{value}' for field '{field}' should not be quoted",
                "severity": "error",
                "location": match.span()
            })
        
        # Check for unquoted string values
        string_pattern = r"(\w+)\s+(?:eq|startswith|endswith|contains)\s+([^'\d][^\s&]+)"
        string_matches = re.finditer(string_pattern, filter_expr)
        
        for match in string_matches:
            field, value = match.groups()
            if not (value.lower() == 'null' or value.lower() == 'true' or value.lower() == 'false'):
                issues.append({
                    "type": "filter",
                    "message": f"String value '{value}' for field '{field}' should be quoted",
                    "severity": "error",
                    "location": match.span()
                })
        
        # Check document status format (should be bost_X)
        if "DocumentStatus" in filter_expr:
            status_pattern = r"DocumentStatus\s+eq\s+'([^']+)'"
            status_match = re.search(status_pattern, filter_expr)
            if status_match:
                status = status_match.group(1)
                if not status.startswith("bost_"):
                    issues.append({
                        "type": "filter",
                        "message": f"DocumentStatus value '{status}' should use bost_ prefix",
                        "severity": "error"
                    })
        
        # Check boolean values (should be tYES/tNO for some fields)
        boolean_fields = ["Paid", "Active"]
        for field in boolean_fields:
            if field in filter_expr:
                bool_pattern = f"{field}\\s+eq\\s+'?([^'\\s&]+)"
                bool_match = re.search(bool_pattern, filter_expr)
                if bool_match:
                    value = bool_match.group(1)
                    if value not in self.sap_constants["boolean_values"] and value.lower() not in ['true', 'false']:
                        issues.append({
                            "type": "filter",
                            "message": f"Field '{field}' should use SAP B1 values 'tYES'/'tNO'",
                            "severity": "error"
                        })
        
        return issues
    
    def _validate_string_values(self, url: str) -> List[Dict[str, Any]]:
        """Validate string values have proper quotes."""
        issues = []
        
        # Fields that commonly contain string values in SAP B1
        string_fields = ["CardName", "CardCode", "ItemName", "ItemCode", "DocumentStatus"]
        
        for field in string_fields:
            pattern = f"{field}\\s+eq\\s+([^'\\s&][^\\s&]*)"
            matches = re.finditer(pattern, url)
            
            for match in matches:
                value = match.group(1)
                if not (value.lower() == 'null' or value.isdigit()):
                    issues.append({
                        "type": "string_value",
                        "message": f"String value for field '{field}' should be quoted",
                        "severity": "error",
                        "location": match.span()
                    })
        
        return issues
    
    def _validate_date_formats(self, url: str) -> List[Dict[str, Any]]:
        """Validate date formats in the URL."""
        issues = []
        
        # Date fields in SAP B1
        date_fields = ["DocDate", "CreateDate", "UpdateDate", "DueDate", "TaxDate", "PostingDate"]
        
        for field in date_fields:
            # Look for date patterns
            pattern = f"{field}\\s+(?:eq|gt|lt|ge|le)\\s+'?([^'\\s&]+)"
            matches = re.finditer(pattern, url)
            
            for match in matches:
                date_value = match.group(1)
                
                # Validate ISO format YYYY-MM-DD
                if not re.match(r'\d{4}-\d{2}-\d{2}', date_value):
                    issues.append({
                        "type": "date_format",
                        "message": f"Date value '{date_value}' should be in YYYY-MM-DD format",
                        "severity": "error",
                        "location": match.span()
                    })
                
                # Check if date is quoted
                if not date_value.startswith("'"):
                    date_pattern = f"{field}\\s+(?:eq|gt|lt|ge|le)\\s+{date_value}"
                    if re.search(date_pattern, url):
                        issues.append({
                            "type": "date_format",
                            "message": f"Date value for field '{field}' should be quoted",
                            "severity": "error"
                        })
        
        return issues
    
    def fix_common_issues(self, url: str) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Attempt to fix common issues in an OData URL.
        
        Args:
            url: The OData URL to fix
            
        Returns:
            Tuple of (fixed_url, fixes_applied)
        """
        fixes_applied = []
        fixed_url = url
        
        # Fix 1: Add quotes to string values
        for field in ["CardName", "CardCode", "ItemName", "ItemCode"]:
            pattern = f"({field}\\s+eq\\s+)([^'\\s&][^\\s&]*)"
            
            def add_quotes(match, field_name=field):
                fixes_applied.append({
                    "type": "string_value",
                    "message": f"Added quotes to string value for field '{field_name}'",
                    "original": match.group(0),
                    "fixed": f"{match.group(1)}'{match.group(2)}'"
                })
                return f"{match.group(1)}'{match.group(2)}'"
            
            fixed_url = re.sub(pattern, add_quotes, fixed_url)
        
        # Fix 2: Add bost_ prefix to DocumentStatus values
        pattern = r"(DocumentStatus\s+eq\s+'?)([A-Za-z]+)('?)"
        def fix_doc_status(match):
            prefix, value, suffix = match.groups()
            if not value.startswith("bost_"):
                capitalized = value[0].upper() + value[1:].lower()
                fixed_value = f"bost_{capitalized}"
                fixes_applied.append({
                    "type": "document_status",
                    "message": f"Added bost_ prefix to DocumentStatus value",
                    "original": match.group(0),
                    "fixed": f"{prefix}{fixed_value}{suffix}"
                })
                return f"{prefix}{fixed_value}{suffix}"
            return match.group(0)
        
        fixed_url = re.sub(pattern, fix_doc_status, fixed_url)
        
        # Fix 3: Replace tYES/tNO for boolean fields
        for field in ["Paid", "Active"]:
            # True -> tYES
            true_pattern = f"({field}\\s+eq\\s+)(?:'?)(?:true|True|TRUE)(?:'?)"
            fixed_url = re.sub(true_pattern, lambda m: self._apply_fix(m, f"{m.group(1)}'tYES'", fixes_applied,
                               f"Replaced True value with 'tYES' for field '{field}'"), fixed_url)
            
            # False -> tNO
            false_pattern = f"({field}\\s+eq\\s+)(?:'?)(?:false|False|FALSE)(?:'?)"
            fixed_url = re.sub(false_pattern, lambda m: self._apply_fix(m, f"{m.group(1)}'tNO'", fixes_applied,
                                f"Replaced False value with 'tNO' for field '{field}'"), fixed_url)
        
        # Fix 4: Format dates properly
        date_fields = ["DocDate", "CreateDate", "UpdateDate", "DueDate", "TaxDate", "PostingDate"]
        for field in date_fields:
            # Add quotes to dates if missing
            date_pattern = f"({field}\\s+(?:eq|gt|lt|ge|le)\\s+)(\\d{{4}}-\\d{{2}}-\\d{{2}})(\\s|&|$)"
            fixed_url = re.sub(date_pattern, lambda m: self._apply_fix(m, f"{m.group(1)}'{m.group(2)}'{m.group(3)}", fixes_applied,
                                f"Added quotes to date value for field '{field}'"), fixed_url)
            
            # Try to fix date formats MM/DD/YYYY -> YYYY-MM-DD
            date_slash_pattern = f"({field}\\s+(?:eq|gt|lt|ge|le)\\s+')?(\\d{{1,2}}/\\d{{1,2}}/\\d{{4}})((?:')?(?:\\s|&|$))"
            def fix_date_format(m):
                quoted_prefix = m.group(1) or ""
                date_parts = m.group(2).split('/')
                iso_date = f"{date_parts[2]}-{date_parts[0].zfill(2)}-{date_parts[1].zfill(2)}"
                
                fixes_applied.append({
                    "type": "date_format",
                    "message": f"Converted date from MM/DD/YYYY to ISO format",
                    "original": m.group(0),
                    "fixed": f"{quoted_prefix}{iso_date}{m.group(3)}"
                })
                
                return f"{quoted_prefix}{iso_date}{m.group(3)}"
            
            fixed_url = re.sub(date_slash_pattern, fix_date_format, fixed_url)
        
        # Fix 5: Remove quotes from numeric values
        numeric_pattern = r"(\w+\s+(?:eq|gt|lt|ge|le)\s+)'(\d+(?:\.\d+)?)'((?:\s|&|$))"
        
        def remove_quotes_from_numbers(match):
            field, value, suffix = match.groups()
            
            # Skip fields that might require quotes
            if any(field.startswith(f) for f in ["CardCode", "ItemCode"]):
                return match.group(0)
                
            fixes_applied.append({
                "type": "numeric_value",
                "message": f"Removed quotes from numeric value for field '{field}'",
                "original": match.group(0),
                "fixed": f"{field} {value}{suffix}"
            })
            return f"{field} {value}{suffix}"
        
        fixed_url = re.sub(numeric_pattern, remove_quotes_from_numbers, fixed_url)
        
        return fixed_url, fixes_applied
    
    def _apply_fix(self, match, replacement, fixes_applied, message):
        """Helper method to apply a fix and track it."""
        fixes_applied.append({
            "message": message,
            "original": match.group(0),
            "fixed": replacement
        })
        return replacement