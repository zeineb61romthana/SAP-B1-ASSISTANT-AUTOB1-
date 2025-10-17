# tools/support_tools.py

import os
import json
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime
from utils.enhanced_errors import SAPAssistantError, format_error_for_response
import logging

logger = logging.getLogger(__name__)

class CrystalReportsGenerator:
    """Crystal Reports integration for invoice and order reports"""
    
    def __init__(self, reports_directory: str = "reports"):
        self.reports_directory = reports_directory
        os.makedirs(reports_directory, exist_ok=True)
    
    async def get_crystal_report(self, report_type: str, record_id: str, record_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Generate Crystal Report for invoice or order"""
        try:
            logger.info(f"Generating Crystal Report: {report_type} for {record_id}")
            
            # Validate report type
            valid_types = ["invoice", "order", "customer_statement", "business_partner"]
            if report_type.lower() not in valid_types:
                return {
                    "status": "error",
                    "message": f"Invalid report type. Must be one of: {valid_types}"
                }
            
            # Generate report filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_filename = f"{report_type}_{record_id}_{timestamp}.pdf"
            report_path = os.path.join(self.reports_directory, report_filename)
            
            # Simulate Crystal Reports processing
            logger.info("Generating Crystal Report...")
            await asyncio.sleep(2)  # Simulate report generation delay
            
            # In a real implementation, you would:
            # 1. Connect to Crystal Reports Server
            # 2. Pass the record_data to the report template
            # 3. Generate the PDF report
            # 4. Save it to the specified path
            
            # For now, we'll create a mock PDF file
            await self._create_mock_report(report_path, report_type, record_id, record_data)
            
            result = {
                "status": "success",
                "report_type": report_type,
                "record_id": record_id,
                "report_path": report_path,
                "report_filename": report_filename,
                "generated_at": datetime.now().isoformat(),
                "file_size": self._get_file_size(report_path)
            }
            
            logger.info(f"Crystal Report generated: {report_path}")
            return result
            
        except Exception as e:
            logger.error(f"Crystal Report generation failed: {e}")
            return {
                "status": "error",
                "message": f"Error generating Crystal Report: {str(e)}"
            }
    
    async def _create_mock_report(self, report_path: str, report_type: str, record_id: str, record_data: Dict[str, Any]):
        """Create a mock report file (replace with actual Crystal Reports integration)"""
        # This is a placeholder - in reality you'd integrate with Crystal Reports
        mock_content = f"""CRYSTAL REPORT - {report_type.upper()}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Record ID: {record_id}

--- REPORT DATA ---
{json.dumps(record_data, indent=2) if record_data else 'No data provided'}

--- END REPORT ---
"""
        
        # Write mock content to file
        with open(report_path, 'w') as f:
            f.write(mock_content)
    
    def _get_file_size(self, file_path: str) -> str:
        """Get human-readable file size"""
        try:
            size_bytes = os.path.getsize(file_path)
            if size_bytes < 1024:
                return f"{size_bytes} B"
            elif size_bytes < 1024 * 1024:
                return f"{size_bytes / 1024:.1f} KB"
            else:
                return f"{size_bytes / (1024 * 1024):.1f} MB"
        except:
            return "Unknown size"

class SAVTicketSystem:
    """Service After Sale (SAV) ticket system integration"""
    
    def __init__(self, ticket_system_api: str = None):
        self.ticket_system_api = ticket_system_api
        self.tickets_directory = "tickets"
        os.makedirs(self.tickets_directory, exist_ok=True)
    
    async def create_sav_ticket(self, issue_title: str, issue_description: str, 
                               customer_email: str, priority: str = "normal", 
                               additional_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create SAV (Service After Sale) ticket when automated processing fails"""
        try:
            logger.info("Creating SAV ticket for human support")
            
            # Generate ticket ID
            ticket_id = f"SAV-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            
            # Prepare ticket data
            ticket_data = {
                "ticket_id": ticket_id,
                "title": issue_title,
                "description": issue_description,
                "customer_email": customer_email,
                "priority": priority,
                "status": "open",
                "created_at": datetime.now().isoformat(),
                "assigned_to": "support_team",
                "additional_data": additional_data or {}
            }
            
            # In a real implementation, you would:
            # 1. Connect to your ticketing system (ServiceNow, Jira, etc.)
            # 2. Create the ticket via API
            # 3. Send notifications to support team
            # 4. Return ticket details
            
            # For now, we'll save ticket data locally
            ticket_file = os.path.join(self.tickets_directory, f"{ticket_id}.json")
            with open(ticket_file, 'w') as f:
                json.dump(ticket_data, f, indent=2)
            
            # Log ticket creation
            logger.info(f"Support ticket created: {ticket_id}")
            logger.info(f"Customer: {customer_email}")
            logger.info(f"Title: {issue_title}")
            logger.info(f"Priority: {priority}")
            
            result = {
                "status": "success",
                "ticket_id": ticket_id,
                "message": f"SAV ticket {ticket_id} created successfully. Our support team will contact you within 24 hours at {customer_email}.",
                "priority": priority,
                "estimated_response_time": self._get_response_time(priority)
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error creating SAV ticket: {e}")
            return {
                "status": "error",
                "message": f"Failed to create SAV ticket: {str(e)}"
            }
    
    def _get_response_time(self, priority: str) -> str:
        """Get estimated response time based on priority"""
        response_times = {
            "low": "72 hours",
            "normal": "24 hours", 
            "high": "4 hours",
            "critical": "1 hour"
        }
        return response_times.get(priority.lower(), "24 hours")

class SupportToolsIntegration:
    """Integration class for Crystal Reports and SAV tickets with SAP Assistant"""
    
    def __init__(self):
        self.crystal_reports = CrystalReportsGenerator()
        self.sav_system = SAVTicketSystem()
    
    async def generate_invoice_report(self, invoice_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate Crystal Report for invoice"""
        invoice_id = invoice_data.get("invoice_id", invoice_data.get("doc_entry", "unknown"))
        return await self.crystal_reports.get_crystal_report("invoice", invoice_id, invoice_data)
    
    async def generate_order_report(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate Crystal Report for order"""
        order_id = order_data.get("order_id", order_data.get("doc_entry", "unknown"))
        return await self.crystal_reports.get_crystal_report("order", order_id, order_data)
    
    async def create_support_ticket(self, title: str, description: str, customer_email: str, 
                                   priority: str = "normal", context_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create support ticket with context"""
        return await self.sav_system.create_sav_ticket(title, description, customer_email, priority, context_data)
    
    def invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Tool invoke method for workflow integration"""
        action = state.get("support_action")
        
        try:
            if action == "generate_crystal_report":
                report_type = state.get("report_type", "invoice")
                record_id = state.get("record_id", "")
                record_data = state.get("record_data", {})
                
                # Run async method in sync context
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                result = loop.run_until_complete(
                    self.crystal_reports.get_crystal_report(report_type, record_id, record_data)
                )
                state["crystal_report"] = result
                
            elif action == "create_sav_ticket":
                title = state.get("ticket_title", "Customer Support Request")
                description = state.get("ticket_description", "")
                customer_email = state.get("customer_email", "")
                priority = state.get("priority", "normal")
                context_data = state.get("context_data", {})
                
                # Run async method in sync context
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                result = loop.run_until_complete(
                    self.sav_system.create_sav_ticket(title, description, customer_email, priority, context_data)
                )
                state["sav_ticket"] = result
                
            return state
            
        except Exception as e:
            error_dict = format_error_for_response(e)
            state["error"] = error_dict
            return state