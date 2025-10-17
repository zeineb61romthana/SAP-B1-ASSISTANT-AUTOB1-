# agents/result_formatting.py

from typing import Dict, Any
import json
import pandas as pd
from tabulate import tabulate

# Import the enhanced error utilities
from utils.enhanced_errors import (
    SAPAssistantError,
    FormattingError,
    format_error_for_response
)

class ResultFormattingAgent:
    
    def __init__(self):
        # The constructor is intentionally left empty as no initialization is required for this agent.
        pass
    
    def _get_key_columns(self, df, max_columns=6):
        """
        Intelligently select the most important columns to display in table format.
        This prevents tables from being too wide and unreadable.
        """
        if len(df.columns) <= max_columns:
            return df.columns.tolist()
        
        # Priority columns that are commonly important in SAP B1
        priority_columns = [
            'ItemCode', 'ItemName', 'CardCode', 'CardName', 'DocNum', 'DocDate',
            'DocEntry', 'U_NAME', 'Name', 'Code', 'Description', 'QuantityOnStock',
            'Price', 'Total', 'LineTotal', 'DocTotal', 'Balance', 'DebitCredit',
            'Account', 'AccountCode', 'AccountName', 'WhsCode', 'WhsName',
            'BPCode', 'BPName', 'VendorCode', 'VendorName', 'CustomerCode', 'CustomerName'
        ]
        
        # Find columns that exist in our data and are in priority list
        available_priority = [col for col in priority_columns if col in df.columns]
        
        # If we have enough priority columns, use them
        if len(available_priority) >= max_columns:
            return available_priority[:max_columns]
        
        # Otherwise, add the first few columns that aren't in priority list
        remaining_columns = [col for col in df.columns if col not in available_priority]
        additional_needed = max_columns - len(available_priority)
        
        return available_priority + remaining_columns[:additional_needed]
    
    def _format_as_table(self, data):
        """Format data as a pretty, readable table with smart column selection."""
        if not data:
            return "No data found."
            
        # If data is a dictionary with 'value' key (standard OData format)
        if isinstance(data, dict) and "value" in data and isinstance(data["value"], list):
            items = data["value"]
        elif isinstance(data, list):
            items = data
        else:
            items = [data]
        
        if not items:
            return "No data found."
            
        # Convert to DataFrame for easier handling
        df = pd.DataFrame(items)
        
        # Smart column selection - only show the most relevant columns
        key_columns = self._get_key_columns(df)
        df_display = df[key_columns].copy()
        
        # Handle nested objects by converting them to string representations
        for col in df_display.columns:
            if df_display[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df_display[col] = df_display[col].apply(
                    lambda x: json.dumps(x, indent=2) if isinstance(x, (dict, list)) else x
                )
        
        # Truncate long text fields to make table more readable
        for col in df_display.columns:
            if df_display[col].dtype == 'object':  # String columns
                df_display[col] = df_display[col].astype(str).apply(
                    lambda x: x[:50] + "..." if len(str(x)) > 50 else x
                )
        
        # Generate table with a cleaner format
        table_output = tabulate(df_display, headers="keys", tablefmt="simple_grid", showindex=False)
        
        # Add summary information
        total_columns = len(df.columns)
        shown_columns = len(key_columns)
        
        summary = f"\nShowing {shown_columns} of {total_columns} columns | {len(items)} records"
        if total_columns > shown_columns:
            hidden_columns = [col for col in df.columns if col not in key_columns]
            summary += f"\nHidden columns: {', '.join(hidden_columns[:10])}"
            if len(hidden_columns) > 10:
                summary += f" and {len(hidden_columns) - 10} more..."
            summary += f"\nUse '--format json' to see all data"
        
        return table_output + summary
    
    def _format_as_json(self, data):
        """Format data as pretty, readable JSON."""
        return json.dumps(data, indent=2, ensure_ascii=False)
    
    def _format_as_csv(self, data):
        """Format data as CSV."""
        if not data:
            return "No data found."
            
        # If data is a dictionary with 'value' key (standard OData format)
        if isinstance(data, dict) and "value" in data and isinstance(data["value"], list):
            items = data["value"]
        elif isinstance(data, list):
            items = data
        else:
            items = [data]
        
        if not items:
            return "No data found."
            
        # Convert to DataFrame and then to CSV
        df = pd.DataFrame(items)
        return df.to_csv(index=False)
    
    def _format_count_result(self, data, count_only=False):
        """Format count results specifically."""
        if count_only:
            # For /$count endpoint, response is just a number
            if isinstance(data, (int, str)):
                return f"Count: {data}"
            else:
                return f"Count: {str(data)}"
        else:
            # For $count=true, SAP returns @odata.count property
            if isinstance(data, dict):
                count = data.get('@odata.count', data.get('odata.count'))
                if count is not None:
                    result_data = data.get('value', [])
                    record_count = len(result_data) if isinstance(result_data, list) else 0
                    
                    formatted_data = self._format_as_json(data) if hasattr(self, '_format_as_json') else str(data)
                    return f"Total Count: {count}\nRecords Retrieved: {record_count}\n\n{formatted_data}"
            
            # Fallback to normal formatting
            return self._format_as_json(data) if hasattr(self, '_format_as_json') else str(data)
    
    def invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Format the response data according to the specified output format with enhanced error handling."""
        try:
            # Check if response is present
            if "response" not in state:
                if "error" in state:
                    # Use enhanced error formatting for user display
                    error_dict = state["error"]
                    
                    # Create a nicely formatted error message
                    error_title = f"ERROR: {error_dict.get('code', 'UNKNOWN_ERROR')}"
                    error_message = error_dict.get("user_message", error_dict.get("message", "Unknown error"))
                    
                    # Format suggestions if available
                    suggestions = error_dict.get("suggestions", [])
                    suggestion_text = ""
                    if suggestions:
                        suggestion_text = "\n\nSuggestions:\n" + "\n".join(f"- {s}" for s in suggestions)
                    
                    # Format the entire error output
                    formatted_error = f"{error_title}\n{'-' * len(error_title)}\n{error_message}{suggestion_text}"
                    
                    state["output"] = formatted_error
                else:
                    state["output"] = "No response data available."
                return state
            
            # Get the output format - DEFAULT TO JSON instead of table
            output_format = state.get("output_format", "json").lower()
            
            # Check if this is a count query
            structured_query = state.get("structured_query", {})
            count_only = structured_query.get("count_only", False)
            include_count = structured_query.get("include_count", False)
            
            # Format the data
            if count_only or include_count:
                state["output"] = self._format_count_result(state["response"], count_only)
            elif output_format == "table":
                state["output"] = self._format_as_table(state["response"])
            elif output_format == "csv":
                state["output"] = self._format_as_csv(state["response"])
            else:  # Default to JSON
                state["output"] = self._format_as_json(state["response"])
            
            # Check for warnings and add them to the output if present
            if "warning" in state:
                warning_msg = state["warning"].get("message", "Warning: The result may have issues.")
                state["output"] = f"{warning_msg}\n\n{state['output']}"
            
            return state
            
        except SAPAssistantError as e:
            # Already in standard format, log and update state
            error_dict = e.log()
            state["error"] = error_dict
            
            # Create user-friendly output
            error_message = error_dict.get("user_message", error_dict.get("message", "Unknown error"))
            state["output"] = f"Error formatting results: {error_message}"
            
            # Include raw response as fallback if available
            if "response" in state:
                state["output"] += "\n\nRaw response data:\n" + str(state["response"])
                
            return state
        except Exception as e:
            # Convert to standard error format
            error = FormattingError(
                message=f"Error formatting results: {str(e)}",
                details={"error_type": type(e).__name__},
                original_exception=e,
                suggestions=["Try a different output format", "Check if response contains valid data"]
            )
            error_dict = error.log()
            state["error"] = error_dict
            
            # Create user-friendly output
            state["output"] = f"Error formatting results: {str(e)}"
            
            # Include raw response as fallback if available
            if "response" in state:
                state["output"] += "\n\nRaw response data:\n" + str(state["response"])
                
            return state