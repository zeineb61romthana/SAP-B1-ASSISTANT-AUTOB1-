# tools/sap_business_tools.py

import json
import re
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from integration.enhanced_sap_client import SAPB1EnhancedClient
from utils.enhanced_errors import SAPAssistantError, format_error_for_response
import logging

logger = logging.getLogger(__name__)

class SAPBusinessTools:
    """SAP B1 business tools for email agent integration"""
    
    def __init__(self, sap_client: SAPB1EnhancedClient = None, entity_registry=None):
        self.sap_client = sap_client
        self.entity_registry = entity_registry
        
        if not self.sap_client:
            # Initialize SAP client if not provided
            self.sap_client = SAPB1EnhancedClient()
    
    def get_business_partner_from_mail(self, email_address: str) -> Dict[str, Any]:
        """Get business partner details from email address using real SAP B1 data"""
        try:
            logger.info(f"Looking up business partner by email: {email_address}")
            
            # Query SAP B1 for business partner with this email
            odata_url = f"/BusinessPartners?$filter=EmailAddress eq '{email_address}'"
            
            response = self.sap_client.execute_request(odata_url)
            
            if isinstance(response, dict) and "value" in response:
                partners = response["value"]
                
                if partners:
                    partner = partners[0]  # Take first match
                    
                    result = {
                        "status": "found",
                        "partner_id": partner.get("CardCode", ""),
                        "name": partner.get("CardName", ""),
                        "email": partner.get("EmailAddress", ""),
                        "customer_type": "Customer" if partner.get("CardType") == "C" else "Vendor",
                        "phone": partner.get("Phone1", ""),
                        "address": partner.get("Address", ""),
                        "card_code": partner.get("CardCode", ""),
                        "card_name": partner.get("CardName", "")
                    }
                    
                    logger.info(f"Business partner found: {partner.get('CardName')}")
                    return result
            
            # If not found by email, try alternative approaches
            logger.info(f"No direct email match, trying alternative searches...")
            
            # Search by partial email or domain
            domain = email_address.split('@')[1] if '@' in email_address else ""
            if domain:
                odata_url = f"/BusinessPartners?$filter=contains(EmailAddress, '{domain}') or contains(CardName, '{domain.split('.')[0]}')"
                response = self.sap_client.execute_request(odata_url)
                
                if isinstance(response, dict) and "value" in response and response["value"]:
                    partner = response["value"][0]
                    result = {
                        "status": "found_partial",
                        "partner_id": partner.get("CardCode", ""),
                        "name": partner.get("CardName", ""),
                        "email": partner.get("EmailAddress", ""),
                        "customer_type": "Customer" if partner.get("CardType") == "C" else "Vendor",
                        "phone": partner.get("Phone1", ""),
                        "address": partner.get("Address", ""),
                        "card_code": partner.get("CardCode", ""),
                        "card_name": partner.get("CardName", ""),
                        "match_type": "partial_domain"
                    }
                    logger.info(f"Partial match found: {partner.get('CardName')}")
                    return result
            
            logger.info(f"Business partner not found for email: {email_address}")
            return {"status": "not_found", "email": email_address}
            
        except Exception as e:
            logger.error(f"Error looking up business partner: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def get_latest_order_for_business_partner(self, partner_email: str) -> Dict[str, Any]:
        """Get the latest order for a business partner using real SAP B1 data"""
        try:
            logger.info(f"Getting latest order for: {partner_email}")
            
            # First get the business partner
            partner_info = self.get_business_partner_from_mail(partner_email)
            
            if partner_info["status"] not in ["found", "found_partial"]:
                return {"status": "partner_not_found", "email": partner_email}
            
            card_code = partner_info.get("card_code")
            if not card_code:
                return {"status": "no_card_code", "email": partner_email}
            
            # Query for latest order for this business partner
            odata_url = f"/Orders?$filter=CardCode eq '{card_code}'&$orderby=DocDate desc&$top=1"
            
            response = self.sap_client.execute_request(odata_url)
            
            if isinstance(response, dict) and "value" in response:
                orders = response["value"]
                
                if orders:
                    order = orders[0]
                    
                    result = {
                        "status": "found",
                        "order_id": str(order.get("DocNum", order.get("DocEntry", ""))),
                        "doc_entry": order.get("DocEntry", ""),
                        "customer_name": order.get("CardName", ""),
                        "customer_email": partner_email,
                        "card_code": order.get("CardCode", ""),
                        "order_date": order.get("DocDate", ""),
                        "order_status": self._format_document_status(order.get("DocumentStatus", "")),
                        "amount": order.get("DocTotal", 0),
                        "currency": order.get("DocCurrency", ""),
                        "invoice_available": True  # We'll check this in invoice lookup
                    }
                    
                    logger.info(f"Latest order found: {result['order_id']} - {result['amount']}")
                    return result
            
            logger.info(f"No orders found for partner: {card_code}")
            return {"status": "no_orders", "partner_id": card_code}
            
        except Exception as e:
            logger.error(f"Error getting latest order: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def get_invoices_related_to_order(self, order_id: str) -> Dict[str, Any]:
        """Get all invoices related to a specific order using real SAP B1 data"""
        try:
            logger.info(f"Getting invoices for order: {order_id}")
            
            # First try to find the order
            order_response = self.sap_client.execute_request(f"/Orders?$filter=DocNum eq {order_id}")
            
            if not (isinstance(order_response, dict) and "value" in order_response and order_response["value"]):
                return {"status": "order_not_found", "order_id": order_id}
            
            order = order_response["value"][0]
            card_code = order.get("CardCode")
            
            # Look for invoices with the same CardCode and similar date range
            order_date = order.get("DocDate", "")
            
            # Query for invoices for this customer around the order date
            odata_url = f"/Invoices?$filter=CardCode eq '{card_code}'"
            
            if order_date:
                # Add date range filter (30 days after order date)
                odata_url += f" and DocDate ge '{order_date}'"
            
            response = self.sap_client.execute_request(odata_url)
            
            if isinstance(response, dict) and "value" in response:
                invoices = response["value"]
                
                if invoices:
                    result = {
                        "status": "found",
                        "order_id": order_id,
                        "invoice_count": len(invoices),
                        "invoices": []
                    }
                    
                    for invoice in invoices:
                        result["invoices"].append({
                            "invoice_id": str(invoice.get("DocNum", invoice.get("DocEntry", ""))),
                            "doc_entry": invoice.get("DocEntry", ""),
                            "invoice_date": invoice.get("DocDate", ""),
                            "amount": invoice.get("DocTotal", 0),
                            "currency": invoice.get("DocCurrency", ""),
                            "status": self._format_document_status(invoice.get("DocumentStatus", "")),
                            "card_code": invoice.get("CardCode", ""),
                            "customer_name": invoice.get("CardName", "")
                        })
                    
                    logger.info(f"Found {len(invoices)} invoices for order {order_id}")
                    return result
            
            logger.info(f"No invoices found for order {order_id}")
            return {"status": "not_found", "order_id": order_id}
            
        except Exception as e:
            logger.error(f"Error getting invoices for order: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def get_invoice_by_id(self, invoice_id: str) -> Dict[str, Any]:
        """Get specific invoice by invoice ID using real SAP B1 data"""
        try:
            logger.info(f"Getting invoice: {invoice_id}")
            
            odata_url = f"/Invoices?$filter=DocNum eq {invoice_id}"
            response = self.sap_client.execute_request(odata_url)
            
            if isinstance(response, dict) and "value" in response:
                invoices = response["value"]
                
                if invoices:
                    invoice = invoices[0]
                    
                    result = {
                        "status": "found",
                        "invoice_id": str(invoice.get("DocNum", "")),
                        "doc_entry": invoice.get("DocEntry", ""),
                        "order_id": "",  # SAP B1 doesn't directly link invoices to orders
                        "customer_email": "",  # We'll need to look this up
                        "card_code": invoice.get("CardCode", ""),
                        "customer_name": invoice.get("CardName", ""),
                        "invoice_date": invoice.get("DocDate", ""),
                        "amount": invoice.get("DocTotal", 0),
                        "currency": invoice.get("DocCurrency", ""),
                        "status": self._format_document_status(invoice.get("DocumentStatus", ""))
                    }
                    
                    # Try to get customer email
                    if result["card_code"]:
                        bp_response = self.sap_client.execute_request(f"/BusinessPartners('{result['card_code']}')")
                        if isinstance(bp_response, dict) and "EmailAddress" in bp_response:
                            result["customer_email"] = bp_response["EmailAddress"]
                    
                    logger.info(f"Invoice found: {invoice_id} - {result['amount']}")
                    return result
            
            logger.info(f"Invoice not found: {invoice_id}")
            return {"status": "not_found", "invoice_id": invoice_id}
            
        except Exception as e:
            logger.error(f"Error getting invoice: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def extract_order_number_from_email(self, email_text: str) -> str:
        """Extract order number from customer email text"""
        logger.info("Analyzing email text for order number...")
        
        patterns = [
            r'order\s+(?:number|#|no\.?|id)\s*:?\s*([0-9]+)',
            r'order\s+([0-9]{6,})',
            r'#([0-9]{6,})',
            r'([0-9]{7})',
        ]
        
        email_lower = email_text.lower()
        for pattern in patterns:
            match = re.search(pattern, email_lower)
            if match:
                order_number = match.group(1)
                logger.info(f"Order number found: {order_number}")
                return order_number
        
        logger.info("No order number found in email")
        return ""
    
    def lookup_order_by_id(self, order_id: str) -> Dict[str, Any]:
        """Look up order details by order ID using real SAP B1 data"""
        try:
            logger.info(f"Looking up order: {order_id}")
            
            if not order_id:
                return {"status": "error", "message": "No order ID provided"}
            
            odata_url = f"/Orders?$filter=DocNum eq {order_id}"
            response = self.sap_client.execute_request(odata_url)
            
            if isinstance(response, dict) and "value" in response:
                orders = response["value"]
                
                if orders:
                    order = orders[0]
                    
                    result = {
                        "status": "found",
                        "order_id": str(order.get("DocNum", "")),
                        "doc_entry": order.get("DocEntry", ""),
                        "customer_name": order.get("CardName", ""),
                        "card_code": order.get("CardCode", ""),
                        "customer_email": "",  # Will look up separately
                        "order_date": order.get("DocDate", ""),
                        "order_status": self._format_document_status(order.get("DocumentStatus", "")),
                        "amount": order.get("DocTotal", 0),
                        "currency": order.get("DocCurrency", ""),
                        "invoice_available": True
                    }
                    
                    # Try to get customer email
                    if result["card_code"]:
                        bp_response = self.sap_client.execute_request(f"/BusinessPartners('{result['card_code']}')")
                        if isinstance(bp_response, dict) and "EmailAddress" in bp_response:
                            result["customer_email"] = bp_response["EmailAddress"]
                    
                    logger.info(f"Order found: {order.get('CardName')} - {order.get('DocTotal')}")
                    return result
            
            logger.info(f"Order not found: {order_id}")
            return {"status": "not_found", "order_id": order_id}
            
        except Exception as e:
            logger.error(f"Error looking up order: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def _format_document_status(self, status: str) -> str:
        """Format SAP B1 document status for display"""
        status_mapping = {
            "bost_Open": "Open",
            "bost_Close": "Closed",
            "bost_Cancelled": "Cancelled",
            "O": "Open",
            "C": "Closed"
        }
        return status_mapping.get(status, status)

    def invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Tool invoke method for workflow integration"""
        action = state.get("sap_action")
        
        try:
            if action == "get_business_partner_from_mail":
                email = state.get("email_address", "")
                result = self.get_business_partner_from_mail(email)
                state["business_partner"] = result
                
            elif action == "get_latest_order":
                email = state.get("email_address", "")
                result = self.get_latest_order_for_business_partner(email)
                state["latest_order"] = result
                
            elif action == "get_invoices_for_order":
                order_id = state.get("order_id", "")
                result = self.get_invoices_related_to_order(order_id)
                state["order_invoices"] = result
                
            elif action == "get_invoice_by_id":
                invoice_id = state.get("invoice_id", "")
                result = self.get_invoice_by_id(invoice_id)
                state["invoice_details"] = result
                
            elif action == "lookup_order":
                order_id = state.get("order_id", "")
                result = self.lookup_order_by_id(order_id)
                state["order_details"] = result
                
            elif action == "extract_order_number":
                email_text = state.get("email_text", "")
                order_number = self.extract_order_number_from_email(email_text)
                state["extracted_order_number"] = order_number
                
            return state
            
        except Exception as e:
            error_dict = format_error_for_response(e)
            state["error"] = error_dict
            return state