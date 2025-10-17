# agents/gmail_invoice_agent.py

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import create_openai_functions_agent, AgentExecutor
from langchain.tools import tool

from tools.gmail_integration import GmailIntegrationTool, GmailMessage
from tools.sap_business_tools import SAPBusinessTools
from tools.support_tools import SupportToolsIntegration
from integration.enhanced_sap_client import SAPB1EnhancedClient
from utils.enhanced_errors import SAPAssistantError, format_error_for_response

logger = logging.getLogger(__name__)

class GmailInvoiceProcessingAgent:
    """Gmail-integrated AI Agent for processing invoice requests using SAP B1 data"""
    
    def __init__(self, sap_client: SAPB1EnhancedClient = None, entity_registry=None, openai_api_key: str = None):
        self.sap_client = sap_client or SAPB1EnhancedClient()
        self.entity_registry = entity_registry
        
        # Validate API key requirement for LLM-only approach
        if not openai_api_key:
            raise ValueError("OpenAI API key is required for LLM-based email classification")
        
        # Initialize tools with API key for LLM classification
        self.gmail_tool = GmailIntegrationTool(openai_api_key=openai_api_key)
        self.sap_tools = SAPBusinessTools(self.sap_client, entity_registry)
        self.support_tools = SupportToolsIntegration()
        
        # Initialize LangChain components
        self.llm = ChatOpenAI(
            model="gpt-4",
            api_key=openai_api_key,
            temperature=0.1
        )
        
        # Create LangChain tools from our methods
        self.tools = self._create_langchain_tools()
        
        # Create agent prompt
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", """You are an AI customer service agent that processes invoice requests from Gmail using the company's SAP B1 system.

AVAILABLE TOOLS:
- get_business_partner_from_mail: Get business partner info from email address (uses real SAP B1 data)
- get_latest_order_for_business_partner: Get the most recent order for a customer (uses real SAP B1 data)
- get_invoices_related_to_order: Get all invoices for a specific order (uses real SAP B1 data)
- get_invoice_by_id: Get specific invoice by ID (uses real SAP B1 data)
- extract_order_number_from_email: Extract order number from email text
- lookup_order_by_id: Look up order by specific ID (uses real SAP B1 data)
- generate_crystal_report: Generate PDF Crystal Report for invoice/order
- send_invoice_via_gmail: Send invoice via Gmail with attachment
- create_sav_ticket: Create SAV ticket for human assistance

PROCESS FLOW:
1. First, identify the business partner using their email address
2. If they mention a specific order number, look it up directly
3. If they mention "latest order" or similar, get their most recent order
4. Find the invoices related to the order
5. Generate Crystal Report for the invoice if needed
6. Send the invoice via Gmail
7. If any step fails, create a SAV ticket for human intervention

Always be helpful and professional. If you cannot complete the request automatically, create a support ticket and inform the customer that our team will assist them."""),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad")
        ])
        
        # Create agent
        self.agent = create_openai_functions_agent(self.llm, self.tools, self.prompt)
        self.agent_executor = AgentExecutor(agent=self.agent, tools=self.tools, verbose=True)
        
        logger.info("Gmail Invoice Agent initialized with LLM-only classification")
    
    def _create_langchain_tools(self):
        """Create LangChain tools from our SAP and support methods"""
        
        @tool
        def get_business_partner_from_mail(email_address: str) -> str:
            """Get business partner details from email address using SAP B1."""
            result = self.sap_tools.get_business_partner_from_mail(email_address)
            return json.dumps(result)
        
        @tool
        def get_latest_order_for_business_partner(partner_email: str) -> str:
            """Get the latest order for a business partner by email using SAP B1."""
            result = self.sap_tools.get_latest_order_for_business_partner(partner_email)
            return json.dumps(result)
        
        @tool
        def get_invoices_related_to_order(order_id: str) -> str:
            """Get all invoices related to a specific order using SAP B1."""
            result = self.sap_tools.get_invoices_related_to_order(order_id)
            return json.dumps(result)
        
        @tool
        def get_invoice_by_id(invoice_id: str) -> str:
            """Get specific invoice by invoice ID using SAP B1."""
            result = self.sap_tools.get_invoice_by_id(invoice_id)
            return json.dumps(result)
        
        @tool
        def extract_order_number_from_email(email_text: str) -> str:
            """Extract order number from customer email text."""
            return self.sap_tools.extract_order_number_from_email(email_text)
        
        @tool
        def lookup_order_by_id(order_id: str) -> str:
            """Look up order details by order ID using SAP B1."""
            result = self.sap_tools.lookup_order_by_id(order_id)
            return json.dumps(result)
        
        @tool
        async def generate_crystal_report(report_type: str, record_id: str, record_data: str = "{}") -> str:
            """Generate Crystal Report for invoice or order."""
            try:
                data = json.loads(record_data) if record_data != "{}" else {}
                result = await self.support_tools.crystal_reports.get_crystal_report(report_type, record_id, data)
                return json.dumps(result)
            except Exception as e:
                return json.dumps({"status": "error", "message": str(e)})
        
        @tool
        def send_invoice_via_gmail(order_id: str, customer_email: str, report_path: str = "") -> str:
            """Send invoice for order to customer via Gmail."""
            try:
                # Get order details
                order_result = self.sap_tools.lookup_order_by_id(order_id)
                
                if order_result["status"] != "found":
                    return json.dumps({"status": "error", "message": f"Order {order_id} not found"})
                
                order = order_result
                
                # Verify email matches
                if order.get("customer_email", "").lower() != customer_email.lower():
                    # Get business partner details to check email
                    bp_result = self.sap_tools.get_business_partner_from_mail(customer_email)
                    if bp_result["status"] not in ["found", "found_partial"]:
                        return json.dumps({
                            "status": "error",
                            "message": "Security Error: Cannot send invoice - email address verification failed"
                        })
                
                # Prepare email content
                subject = f"Your Invoice for Order #{order_id}"
                body = f"""Dear {order.get('customer_name', 'Customer')},

Thank you for your order! Please find attached your invoice for order #{order_id}.

Order Details:
- Order ID: {order_id}
- Date: {order.get('order_date', 'N/A')}
- Amount: {order.get('currency', '')} {order.get('amount', 0)}
- Status: {order.get('order_status', 'N/A')}

If you have any questions, please don't hesitate to contact us.

Best regards,
Customer Service Team
"""
                
                # Send email
                success = self.gmail_tool.send_email(
                    to_email=customer_email,
                    subject=subject,
                    body=body,
                    attachment_path=report_path if report_path else None
                )
                
                if success:
                    return json.dumps({
                        "status": "success",
                        "message": f"Invoice for order {order_id} sent successfully to {customer_email}"
                    })
                else:
                    return json.dumps({
                        "status": "error",
                        "message": f"Failed to send invoice email for order {order_id}"
                    })
                    
            except Exception as e:
                return json.dumps({"status": "error", "message": str(e)})
        
        @tool
        async def create_sav_ticket(issue_title: str, issue_description: str, customer_email: str, priority: str = "normal") -> str:
            """Create SAV (Service After Sale) ticket when automated processing fails."""
            try:
                # Create the support ticket
                result = await self.support_tools.create_support_ticket(
                    title=issue_title,
                    description=issue_description,
                    customer_email=customer_email,
                    priority=priority
                )
                
                # If ticket creation was successful, send email notification to customer
                if result.get("status") == "success":
                    ticket_id = result.get("ticket_id", "")
                    estimated_response_time = result.get("estimated_response_time", "24 hours")
                    
                    # Compose professional email to customer
                    email_subject = f"Support Ticket Created - {ticket_id}"
                    email_body = f"""Dear Valued Customer,

        Thank you for contacting us regarding your invoice request.

        We apologize for the inconvenience you experienced. We have created a support ticket to resolve your issue promptly.

        Ticket Details:
        - Ticket ID: {ticket_id}
        - Priority: {priority.title()}
        - Estimated Response Time: {estimated_response_time}

        Our customer service team will review your request and contact you at {customer_email} within the estimated timeframe.

        If you have any urgent questions, please reference ticket ID {ticket_id} in your communication.

        Thank you for your patience and understanding.

        Best regards,
        Customer Service Team"""

                    # Send email notification using Gmail tool
                    try:
                        email_sent = self.gmail_tool.send_email(
                            to_email=customer_email,
                            subject=email_subject,
                            body=email_body
                        )
                        
                        if email_sent:
                            # Update result to include email notification status
                            result["email_sent"] = True
                            result["message"] = f"SAV ticket {ticket_id} created successfully and email notification sent to {customer_email}."
                        else:
                            result["email_sent"] = False
                            result["message"] = f"SAV ticket {ticket_id} created successfully, but email notification failed to send to {customer_email}."
                            
                    except Exception as email_error:
                        # Email failed but ticket was created
                        result["email_sent"] = False
                        result["email_error"] = str(email_error)
                        result["message"] = f"SAV ticket {ticket_id} created successfully, but email notification failed: {str(email_error)}"
                        
                return json.dumps(result)
                
            except Exception as e:
                # Return error if ticket creation fails
                error_result = {
                    "status": "error",
                    "message": f"Failed to create SAV ticket: {str(e)}",
                    "email_sent": False
                }
                return json.dumps(error_result)

        return [
            get_business_partner_from_mail,
            get_latest_order_for_business_partner,
            get_invoices_related_to_order,
            get_invoice_by_id,
            extract_order_number_from_email,
            lookup_order_by_id,
            generate_crystal_report,
            send_invoice_via_gmail,
            create_sav_ticket
        ]
    
    async def process_gmail_message(self, message: GmailMessage) -> Dict[str, Any]:
        """Process a Gmail message for invoice requests"""
        try:
            logger.info(f"Processing message from: {message.sender}")
            logger.info(f"Subject: {message.subject}")
            
            # Check if this looks like an invoice request using LLM
            if not self.gmail_tool.is_invoice_request(message):
                logger.info("Not an invoice request, skipping")
                return {"status": "skipped", "reason": "Not an invoice request"}

            # Prepare input for the agent
            user_input = f"""CUSTOMER EMAIL REQUEST:
From: {message.sender}
Subject: {message.subject}
Body: {message.body}

Please help this customer get their invoice using our SAP B1 system. Follow the process flow to:
1. Look up the business partner
2. Find their order (specific number if mentioned, or latest order)
3. Get the related invoices
4. Generate Crystal Report if needed
5. Send the invoice via email
6. If you encounter any errors, create a SAV ticket for human assistance"""

            logger.info("Processing request with LangChain agent...")
            
            # Execute the agent
            response = await self.agent_executor.ainvoke({
                "input": user_input
            })
            
            # Mark email as read
            self.gmail_tool.mark_as_read(message.message_id)
            
            logger.info("Message processed successfully")
            
            return {
                "status": "success",
                "response": response.get("output", "No response generated"),
                "message_id": message.message_id,
                "customer_email": message.sender
            }
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            
            # Create SAV ticket for error
            try:
                await self.support_tools.create_support_ticket(
                    title=f"AI Processing Error - Gmail Message from {message.sender}",
                    description=f"Error: {str(e)}\n\nOriginal Message:\nFrom: {message.sender}\nSubject: {message.subject}\nBody: {message.body}",
                    customer_email=message.sender,
                    priority="high"
                )
            except:
                pass  # Don't fail if ticket creation also fails
            
            return {"status": "error", "error": str(e)}
    
    async def monitor_gmail_continuously(self, check_interval: int = 60):
        """Continuously monitor Gmail for new invoice requests"""
        logger.info(f"Starting Gmail monitoring (checking every {check_interval} seconds)")
        
        while True:
            try:
                # Simple query for unread messages - let LLM do the classification
                messages = self.gmail_tool.get_messages(query="is:unread")
                
                # Process each message
                for message in messages:
                    result = await self.process_gmail_message(message)
                    logger.info(f"Processing result: {result['status']}")
                    await asyncio.sleep(1)  # Small delay between messages
                
                if not messages:
                    logger.info("No new messages found")
                
                logger.info(f"Waiting {check_interval} seconds before next check...")
                await asyncio.sleep(check_interval)
                
            except KeyboardInterrupt:
                logger.info("Stopping Gmail monitoring...")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(30)  # Wait before retrying
    
    def process_single_message(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single message (for CLI integration)"""
        message = GmailMessage(
            message_id=message_data.get("message_id", ""),
            sender=message_data.get("sender", ""),
            subject=message_data.get("subject", ""),
            body=message_data.get("body", ""),
            received_at=datetime.now(),
            thread_id=message_data.get("thread_id", "")
        )
        
        # Run async method in sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(self.process_gmail_message(message))
            return result
        finally:
            loop.close()

    def invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke method for workflow integration"""
        try:
            action = state.get("gmail_action", "process_message")
            
            if action == "process_message":
                message_data = state.get("gmail_message", {})
                result = self.process_single_message(message_data)
                state["gmail_processing_result"] = result
                
            elif action == "get_messages":
                query = state.get("gmail_query", "is:unread")
                messages = self.gmail_tool.get_messages(query)
                state["gmail_messages"] = [
                    {
                        "message_id": msg.message_id,
                        "sender": msg.sender,
                        "subject": msg.subject,
                        "body": msg.body,
                        "received_at": msg.received_at.isoformat(),
                        "thread_id": msg.thread_id
                    }
                    for msg in messages
                ]
            
            return state
            
        except Exception as e:
            error_dict = format_error_for_response(e)
            state["error"] = error_dict
            return state