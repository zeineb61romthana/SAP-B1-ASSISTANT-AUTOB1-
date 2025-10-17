# tools/gmail_integration.py

import os
import json
import base64
import pickle
import asyncio
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import logging

# LLM imports
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import HumanMessage

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from utils.enhanced_errors import SAPAssistantError, format_error_for_response
from integration.enhanced_sap_client import SAPB1EnhancedClient

logger = logging.getLogger(__name__)

# Gmail API scopes
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.modify'
]

@dataclass
class GmailMessage:
    message_id: str
    sender: str
    subject: str
    body: str
    received_at: datetime
    thread_id: str

@dataclass
class EmailClassificationResult:
    is_invoice_request: bool
    confidence: float
    reasoning: str
    classification_method: str

class GmailIntegrationTool:
    """Gmail integration tool with LLM-only email classification"""
    
    def __init__(self, credentials_file: str = "credentials.json", token_file: str = "gmail_token.pickle", 
                 openai_api_key: str = None):
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.service = None
        
        # Initialize LLM for email classification
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        
        if not self.openai_api_key:
            raise ValueError("OpenAI API key is required for LLM-based email classification. Set OPENAI_API_KEY environment variable or pass openai_api_key parameter.")
        
        try:
            self.llm = ChatOpenAI(
                model="gpt-4o-mini",  # More cost-effective than gpt-4
                api_key=self.openai_api_key,
                temperature=0.0,  # Zero temperature for consistent classification
                max_tokens=200,
                timeout=30
            )
            self._setup_classification_prompt()
            logger.info("LLM email classification initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize LLM: {e}")
            raise ValueError(f"Failed to initialize LLM: {e}")
        
        self._authenticate()
    
    def _setup_classification_prompt(self):
        """Setup enhanced classification prompt for zero-shot learning"""
        self.classification_prompt = ChatPromptTemplate.from_messages([
            ("system", """You are an expert email classifier for a business accounting system. Your job is to identify emails where customers are specifically requesting invoices, receipts, or billing documents.

INVOICE REQUEST emails (classify as YES):
- "I need the invoice for order #12345"
- "Can you resend my receipt from last week?"
- "Where is my bill for the recent purchase?"
- "Please send me the invoice copy"
- "I didn't receive the billing document for my order"
- "Missing invoice for transaction ABC123"
- "Could you email me the payment receipt?"
- "I need a copy of my invoice for tax purposes"

NOT INVOICE REQUEST emails (classify as NO):
- Product announcements: "Meet Claude 4", "Welcome to Augment Code"
- Marketing/newsletters: "Try Mermaid Chart pro today for free!"
- Technical updates: "Now included in Pro: Claude Code, Integrations"
- General support: "How to use our platform"
- Order placement: "I want to buy product X" (placing new order, not requesting invoice)
- Account issues: "Can't login to my account"
- Product questions: "What features does Pro include?"
- System notifications: "Your deployment was successful"
- Social/casual: Any non-business related content

EDGE CASES:
- If someone mentions "order" but asks for invoice/receipt → YES
- If someone mentions "payment" but it's about making a new payment → NO
- If someone mentions "billing" but it's about billing issues/setup → NO
- Marketing emails mentioning "invoice" in templates → NO

You must respond in this EXACT format:
CLASSIFICATION: [YES/NO]
CONFIDENCE: [0.0-1.0]
REASONING: [one sentence explanation]

Be very conservative - when in doubt, classify as NO."""),
            ("human", """Please classify this email:

SUBJECT: {subject}

FROM: {sender}

BODY: {body}

Classify this email according to the rules above.""")
        ])
    
    def _authenticate(self):
        """Authenticate with Gmail API"""
        creds = None
        
        if os.path.exists(self.token_file):
            with open(self.token_file, 'rb') as token:
                creds = pickle.load(token)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(self.credentials_file):
                    raise FileNotFoundError(f"""
Gmail credentials file not found: {self.credentials_file}

To set up Gmail integration:
1. Go to Google Cloud Console: https://console.cloud.google.com/
2. Create a project and enable Gmail API
3. Create OAuth 2.0 credentials
4. Download credentials.json and save as {self.credentials_file}
""")
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, SCOPES)
                creds = flow.run_local_server(port=0)
            
            with open(self.token_file, 'wb') as token:
                pickle.dump(creds, token)
        
        self.service = build('gmail', 'v1', credentials=creds)
        logger.info("Gmail authentication successful")
    
    def get_messages(self, query: str = "is:unread", max_results: int = 50) -> List[GmailMessage]:
        """Get messages from Gmail based on query"""
        try:
            results = self.service.users().messages().list(
                userId='me', 
                q=query,
                maxResults=max_results
            ).execute()
            
            messages = results.get('messages', [])
            gmail_messages = []
            
            for msg in messages:
                message = self.service.users().messages().get(
                    userId='me', 
                    id=msg['id'],
                    format='full'
                ).execute()
                
                gmail_msg = self._parse_message(message)
                if gmail_msg:
                    gmail_messages.append(gmail_msg)
            
            return gmail_messages
            
        except Exception as e:
            raise SAPAssistantError(
                message=f"Error reading Gmail: {str(e)}",
                code="GMAIL_READ_ERROR",
                can_retry=True
            )
    
    def _parse_message(self, message: dict) -> Optional[GmailMessage]:
        """Parse Gmail message into our format"""
        try:
            headers = message['payload'].get('headers', [])
            
            sender = ""
            subject = ""
            
            for header in headers:
                name = header.get('name', '').lower()
                value = header.get('value', '')
                
                if name == 'from':
                    sender = value
                elif name == 'subject':
                    subject = value
            
            # Extract email address from sender
            if '<' in sender and '>' in sender:
                sender_email = sender.split('<')[1].split('>')[0]
            else:
                sender_email = sender
            
            # Extract body
            body = self._extract_body(message['payload'])
            
            return GmailMessage(
                message_id=message['id'],
                sender=sender_email,
                subject=subject,
                body=body,
                received_at=datetime.now(),
                thread_id=message['threadId']
            )
            
        except Exception as e:
            logger.error(f"Error parsing message: {e}")
            return None
    
    def _extract_body(self, payload: dict) -> str:
        """Extract text body from Gmail message payload"""
        body = ""
        
        if 'parts' in payload:
            for part in payload['parts']:
                if part['mimeType'] == 'text/plain':
                    data = part['body'].get('data', '')
                    if data:
                        body = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
                        break
        elif payload['mimeType'] == 'text/plain':
            data = payload['body'].get('data', '')
            if data:
                body = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
        
        return body
    
    def _classify_with_llm(self, message: GmailMessage) -> EmailClassificationResult:
        """Classify email using LLM"""
        try:
            # Limit body length for cost and context efficiency
            body_preview = message.body[:2000] if message.body else ""
            
            # Format the prompt
            prompt = self.classification_prompt.format(
                subject=message.subject or "No Subject",
                sender=message.sender,
                body=body_preview or "No body content"
            )
            
            # Get LLM response with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = self.llm.invoke([HumanMessage(content=prompt)])
                    response_text = response.content.strip()
                    
                    # Parse the response
                    return self._parse_llm_response(response_text)
                    
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    logger.warning(f"LLM request failed, retry {attempt + 1}/{max_retries}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"LLM classification failed: {e}")
            # Return conservative default - classify as NOT an invoice request
            return EmailClassificationResult(
                is_invoice_request=False,
                confidence=0.0,
                reasoning=f"LLM classification failed: {str(e)}",
                classification_method="llm_error"
            )
    
    def _parse_llm_response(self, response_text: str) -> EmailClassificationResult:
        """Parse the structured LLM response"""
        try:
            lines = response_text.strip().split('\n')
            
            # Extract classification
            classification_line = next((line for line in lines if line.startswith('CLASSIFICATION:')), '')
            is_invoice = 'YES' in classification_line.upper()
            
            # Extract confidence
            confidence_line = next((line for line in lines if line.startswith('CONFIDENCE:')), '')
            confidence_str = confidence_line.split(':', 1)[1].strip() if ':' in confidence_line else '0.5'
            try:
                confidence = float(confidence_str)
                confidence = max(0.0, min(1.0, confidence))  # Clamp between 0 and 1
            except ValueError:
                confidence = 0.5
            
            # Extract reasoning
            reasoning_line = next((line for line in lines if line.startswith('REASONING:')), '')
            reasoning = reasoning_line.split(':', 1)[1].strip() if ':' in reasoning_line else "No reasoning provided"
            
            return EmailClassificationResult(
                is_invoice_request=is_invoice,
                confidence=confidence,
                reasoning=reasoning,
                classification_method="llm"
            )
            
        except Exception as e:
            logger.error(f"Failed to parse LLM response: {e}\nResponse was: {response_text}")
            # Default to conservative classification
            return EmailClassificationResult(
                is_invoice_request=False,
                confidence=0.0,
                reasoning="Failed to parse LLM response",
                classification_method="llm_parse_error"
            )
    
    def classify_email(self, message: GmailMessage) -> EmailClassificationResult:
        """Main classification method - LLM only"""
        logger.debug(f"Classifying email from {message.sender} using LLM")
        result = self._classify_with_llm(message)
        
        logger.info(f"Email classification - From: {message.sender}, "
                   f"Subject: {message.subject[:50]}..., "
                   f"Result: {result.is_invoice_request}, "
                   f"Confidence: {result.confidence:.2f}, "
                   f"Method: {result.classification_method}, "
                   f"Reasoning: {result.reasoning}")
        
        return result
    
    def is_invoice_request(self, message: GmailMessage) -> bool:
        """Enhanced invoice request detection using LLM only"""
        try:
            classification = self.classify_email(message)
            return classification.is_invoice_request
            
        except Exception as e:
            logger.error(f"Email classification failed completely: {e}")
            # Conservative fallback - assume NOT an invoice request
            return False
    
    def debug_classification(self, message: GmailMessage) -> Dict[str, Any]:
        """Debug method to troubleshoot classification issues"""
        debug_info = {
            "llm_available": self.llm is not None,
            "api_key_set": self.openai_api_key is not None,
            "prompt_ready": self.classification_prompt is not None,
            "message_preview": {
                "sender": message.sender,
                "subject": message.subject,
                "body_length": len(message.body),
                "body_preview": message.body[:200] + "..." if len(message.body) > 200 else message.body
            }
        }
        
        # Test LLM availability
        try:
            test_response = self.llm.invoke([HumanMessage(content="Test message")])
            debug_info["llm_test"] = {
                "working": True,
                "response_preview": test_response.content[:100]
            }
        except Exception as e:
            debug_info["llm_test"] = {
                "working": False,
                "error": str(e)
            }
        
        # Test actual classification
        try:
            classification = self.classify_email(message)
            debug_info["classification"] = {
                "is_invoice": classification.is_invoice_request,
                "confidence": classification.confidence,
                "method": classification.classification_method,
                "reasoning": classification.reasoning
            }
        except Exception as e:
            debug_info["classification"] = {
                "error": str(e)
            }
        
        return debug_info
    
    def send_email(self, to_email: str, subject: str, body: str, attachment_path: str = None) -> bool:
        """Send email with optional attachment"""
        try:
            message = MIMEMultipart()
            message['to'] = to_email
            message['subject'] = subject
            
            message.attach(MIMEText(body, 'plain'))
            
            if attachment_path and os.path.exists(attachment_path):
                with open(attachment_path, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= {os.path.basename(attachment_path)}'
                )
                message.attach(part)
            
            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
            send_message = {'raw': raw_message}
            
            self.service.users().messages().send(
                userId='me', 
                body=send_message
            ).execute()
            
            return True
            
        except Exception as e:
            raise SAPAssistantError(
                message=f"Error sending email: {str(e)}",
                code="GMAIL_SEND_ERROR",
                can_retry=True
            )
    
    def mark_as_read(self, message_id: str):
        """Mark message as read"""
        try:
            self.service.users().messages().modify(
                userId='me',
                id=message_id,
                body={'removeLabelIds': ['UNREAD']}
            ).execute()
        except Exception as e:
            logger.error(f"Error marking message as read: {e}")

    def invoke(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Gmail tool invoke method for workflow integration"""
        action = state.get("gmail_action", "get_messages")
        
        try:
            if action == "get_messages":
                query = state.get("gmail_query", "is:unread")
                messages = self.get_messages(query)
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
                
            elif action == "send_email":
                success = self.send_email(
                    to_email=state.get("to_email"),
                    subject=state.get("email_subject"),
                    body=state.get("email_body"),
                    attachment_path=state.get("attachment_path")
                )
                state["email_sent"] = success
                
            elif action == "mark_read":
                self.mark_as_read(state.get("message_id"))
                state["marked_read"] = True
                
            return state
            
        except Exception as e:
            error_dict = format_error_for_response(e)
            state["error"] = error_dict
            return state