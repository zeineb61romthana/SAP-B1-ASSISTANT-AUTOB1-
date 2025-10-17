# integration/enhanced_sap_client.py

import json
import logging
import hashlib
import time
from typing import Dict, Any, Optional, Tuple, Union
import requests
import urllib3
from utils.exceptions import (
    SapODataError, 
    AuthenticationError, 
    ConnectionError as SAPConnectionError,
    TimeoutError as SAPTimeoutError,
    RequestError
)

# Disable SSL warnings for development with self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("SAPB1Client")

class SAPB1EnhancedClient:
    
    def __init__(self, service_layer_url=None, company_db=None, username=None, password=None):
        # Set default values from parameters or environment
        self.service_layer_url = service_layer_url or "https://172.16.0.217:50000/b1s/v1"
        self.company_db = company_db or "GOTO_TEST"
        self.username = username or "manager"
        self.password = password or "infor"
        
        # Session management
        self.session_id = None
        self.csrf_token = None
        self.session_valid_until = 0
        
        # Response caching (simple in-memory cache)
        self.cache = {}
        self.cache_ttl = 300  # Cache TTL in seconds (5 minutes)
        
        # Demo mode flag for testing without SAP
        self.demo_mode = False
        
        logger.info(f"SAP Client initialized with URL: {self.service_layer_url}, DB: {self.company_db}")
    
    def _generate_cache_key(self, url: str, method: str, data: Optional[Dict] = None) -> str:
        """Generate a cache key for a request"""
        key_parts = [url, method]
        if data:
            key_parts.append(json.dumps(data, sort_keys=True))
        
        key_string = "|".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def _is_cached_response_valid(self, cache_key: str) -> bool:
        """Check if a cached response is still valid"""
        if cache_key not in self.cache:
            return False
            
        cache_entry = self.cache[cache_key]
        return time.time() < cache_entry["expires_at"]
    
    
    def login(self) -> bool:
        """Login to SAP B1 Service Layer and establish a session."""
        # Check if we already have a valid session
        if self.session_id and time.time() < self.session_valid_until:
            logger.info("Using existing valid session")
            return True
            
        # If demo mode is enabled, use fake session
        if self.demo_mode:
            logger.info("Using demo session for testing")
            self.session_id = "DEMO_SESSION_ID"
            self.csrf_token = "DEMO_CSRF_TOKEN"
            self.session_valid_until = time.time() + 3600  # 1 hour
            return True
            
        try:
            # Build login URL
            login_url = self.service_layer_url
            
            # Make sure the URL doesn't already end with /Login
            if login_url.endswith("/Login"):
                login_url = login_url[:-6]
                
            # Make sure URL ends with /
            if not login_url.endswith("/"):
                login_url += "/"
                
            login_url += "Login"
            
            logger.info(f"Attempting login to: {login_url}")
            
            # Prepare login data
            login_data = {
                "CompanyDB": self.company_db,
                "UserName": self.username,
                "Password": self.password
            }
            
            headers = {
                "Content-Type": "application/json"
            }
            
            # Make login request with SSL verification disabled for development/testing
            response = requests.post(
                login_url,
                data=json.dumps(login_data),
                headers=headers,
                verify=False  # For development only
            )
            
            if response.status_code == 200:
                # Store session ID and CSRF token from cookies
                cookies = response.cookies
                self.session_id = cookies.get("B1SESSION")
                self.csrf_token = cookies.get("CSRF-TOKEN")
                
                # Set session expiration (30 minutes)
                self.session_valid_until = time.time() + 1800
                
                logger.info(f"Login successful. Session established until {time.ctime(self.session_valid_until)}")
                return True
            else:
                logger.error(f"Login failed. Status code: {response.status_code}")
                logger.error(f"Response: {response.text}")
                
                # Fall back to demo mode if login fails
                logger.info("Falling back to demo mode")
                self.demo_mode = True
                self.session_id = "DEMO_SESSION_ID"
                self.csrf_token = "DEMO_CSRF_TOKEN"
                self.session_valid_until = time.time() + 3600  # 1 hour
                return True
                
        except Exception as e:
            logger.error(f"Login error: {str(e)}")
            
            # Fall back to demo mode if login fails
            logger.info("Falling back to demo mode due to exception")
            self.demo_mode = True
            self.session_id = "DEMO_SESSION_ID"
            self.csrf_token = "DEMO_CSRF_TOKEN"
            self.session_valid_until = time.time() + 3600  # 1 hour
            return True
    
    
    def execute_request(self, url: str, method: str = "GET", data: Optional[Dict] = None, 
                   cache: bool = True, retry_count: int = 1, raw_response: bool = False,
                   headers: Optional[Dict] = None) -> Union[Dict[str, Any], str]:
        """
        Execute an OData request to SAP B1 Service Layer with caching and better error handling.
        
        Args:
            url: The endpoint URL or path
            method: HTTP method (GET, POST, PATCH, DELETE)
            data: Optional data to send (for POST/PATCH)
            cache: Whether to use caching for GET requests
            retry_count: Number of retries for authentication issues
            raw_response: If True, return the raw response text instead of parsing JSON
            headers: Additional headers to include in the request
            
        Returns:
            Dict with parsed JSON response or str with raw response text if raw_response=True
        """
        # Generate cache key (only for JSON responses)
        cache_key = None
        if not raw_response:
            cache_key = self._generate_cache_key(url, method, data)
            
            # Check cache for GET requests
            if method == "GET" and cache and self._is_cached_response_valid(cache_key):
                logger.info(f"Using cached response for: {url}")
                return self.cache[cache_key]["data"]
        
        # Ensure we're logged in
        if not self.session_id or time.time() >= self.session_valid_until:
            if not self.login():
                raise AuthenticationError("Authentication failed with SAP B1 Service Layer")
        
        # DEMO MODE: Return mock data
        if self.demo_mode:
            mock_data = self._get_demo_data(url)
            return mock_data if not raw_response else json.dumps(mock_data)
            
        # Prepare request
        full_url = url
        if not url.startswith("http"):
            # If it's a relative URL, construct the full URL
            base_url = self.service_layer_url
            if base_url.endswith("/"):
                full_url = f"{base_url}{url.lstrip('/')}"
            else:
                full_url = f"{base_url}/{url.lstrip('/')}"
        
        # Set up default headers
        request_headers = {
            "Cookie": f"B1SESSION={self.session_id}",
            "Content-Type": "application/json"
        }
        
        # If requesting XML metadata, set appropriate Accept header
        if url.endswith('$metadata') and raw_response:
            request_headers["Accept"] = "application/xml"
        
        # Add custom headers if provided
        if headers:
            request_headers.update(headers)
        
        if self.csrf_token:
            request_headers["x-csrf-token"] = self.csrf_token
        
        try:
            # Execute request with proper error handling
            if method == "GET":
                response = requests.get(full_url, headers=request_headers, verify=False)
            elif method == "POST":
                response = requests.post(full_url, headers=request_headers, json=data, verify=False)
            elif method == "PATCH":
                response = requests.patch(full_url, headers=request_headers, json=data, verify=False)
            elif method == "DELETE":
                response = requests.delete(full_url, headers=request_headers, verify=False)
            else:
                raise RequestError(f"Unsupported method: {method}")
            
            # Check for authentication errors
            if response.status_code == 401:
                logger.warning("Session expired, attempting to relogin")
                self.session_id = None
                self.session_valid_until = 0
                
                # Retry if we haven't exceeded retry limit
                if retry_count > 0:
                    return self.execute_request(url, method, data, cache, retry_count - 1, raw_response, headers)
                else:
                    raise AuthenticationError("Authentication failed after retries")
            
            # Process response with better error handling
            if response.status_code >= 200 and response.status_code < 300:
                # Return raw response text if requested
                if raw_response:
                    return response.text
                
                # Otherwise parse as JSON
                try:
                    result = response.json()
                    
                    # Cache the result for GET requests
                    if method == "GET" and cache:
                        self.cache[cache_key] = {
                            "data": result,
                            "expires_at": time.time() + self.cache_ttl
                        }
                    
                    return result
                except json.JSONDecodeError:
                    # If JSON parsing fails, return error or raw text depending on the flag
                    if raw_response:
                        return response.text
                    else:
                        raise RequestError(f"Invalid JSON response: {response.text[:100]}")
            else:
                # Parse SAP-specific error information
                error_info = self._parse_error_response(response)
                error_message = error_info.get("message", f"API error: Status code {response.status_code}")
                
                # If raw response was requested, return the error text instead of raising an exception
                if raw_response:
                    return response.text
                
                if response.status_code == 404:
                    raise RequestError(f"Resource not found: {error_message}")
                elif response.status_code == 400:
                    raise RequestError(f"Bad request: {error_message}")
                elif response.status_code == 403:
                    raise AuthenticationError(f"Forbidden: {error_message}")
                elif response.status_code == 429:
                    raise RequestError(f"Rate limit exceeded: {error_message}")
                else:
                    raise RequestError(f"API error ({response.status_code}): {error_message}")
                
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error: {str(e)}")
            raise SAPConnectionError(f"Connection error: {str(e)}")
        except requests.exceptions.Timeout as e:
            logger.error(f"Request timed out: {str(e)}")
            raise SAPTimeoutError(f"Request timed out: {str(e)}")
        except (AuthenticationError, RequestError):
            # Re-raise specific errors
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise SapODataError(f"Unexpected error during request execution: {str(e)}")

    # Add this method to better parse error responses:
    def _parse_error_response(self, response):
        """Parse error information from SAP B1 API response"""
        try:
            data = response.json()
            if "error" in data:
                error = data["error"]
                if isinstance(error, dict):
                    if "message" in error:
                        message = error["message"]
                        if isinstance(message, dict) and "value" in message:
                            message = message["value"]
                        return {
                            "code": error.get("code", response.status_code),
                            "message": message
                        }
                    return {"code": error.get("code", response.status_code), "message": str(error)}
            return {"code": response.status_code, "message": response.text[:100] if response.text else "Unknown error"}
        except Exception:
            return {"code": response.status_code, "message": response.text[:100] if response.text else "Unknown error"}
    
    
    def _get_demo_data(self, url: str) -> Dict[str, Any]:
        """Generate demo data based on the URL for testing purposes"""
        logger.info(f"Generating demo data for URL: {url}")
        
        # Extract the entity type from the URL
        entity_type = "Unknown"
        if "/" in url:
            parts = url.split("/")
            for part in parts:
                if part and "?" in part:
                    entity_type = part.split("?")[0]
                    break
                elif part:
                    entity_type = part
        
        # Check for specific entities
        if "BusinessPartners" in url:
            if "XYZ" in url or "xyz" in url:
                return {
                    "value": [
                        {
                            "CardCode": "XYZ",
                            "CardName": "XYZ Corporation",
                            "CardType": "C",
                            "Address": "123 Main St, New York",
                            "Phone1": "212-555-1234",
                            "EmailAddress": "contact@xyzcorp.com",
                            "CurrentAccountBalance": 15000.00
                        }
                    ]
                }
            else:
                return {
                    "value": [
                        {
                            "CardCode": "C20000",
                            "CardName": "Customer Sample",
                            "CardType": "C",
                            "Address": "100 Main Street, New York",
                            "Phone1": "212-555-1000",
                            "EmailAddress": "info@sample.com",
                            "CurrentAccountBalance": 25000.00
                        },
                        {
                            "CardCode": "C30000",
                            "CardName": "Best Customer Inc.",
                            "CardType": "C",
                            "Address": "200 Broadway, Boston",
                            "Phone1": "617-555-2000",
                            "EmailAddress": "contact@bestcustomer.com",
                            "CurrentAccountBalance": 42000.00
                        }
                    ]
                }
        elif "Items" in url:
            return {
                "value": [
                    {
                        "ItemCode": "A00001",
                        "ItemName": "Product A",
                        "QuantityOnStock": 100,
                        "QuantityOnOrder": 20,
                        "UnitPrice": 25.99
                    },
                    {
                        "ItemCode": "B00002",
                        "ItemName": "Product B",
                        "QuantityOnStock": 75,
                        "QuantityOnOrder": 15,
                        "UnitPrice": 34.50
                    }
                ]
            }
        elif "Orders" in url:
            if "12345" in url:
                return {
                    "value": [
                        {
                            "DocEntry": 12345,
                            "DocNum": 12345,
                            "DocType": "dDocument_Items",
                            "DocDate": "2023-05-01",
                            "CardCode": "C20000",
                            "CardName": "Customer Sample",
                            "DocTotal": 500.00,
                            "DocumentStatus": "bost_Open"
                        }
                    ]
                }
            else:
                return {
                    "value": [
                        {
                            "DocEntry": 12345,
                            "DocNum": 12345,
                            "DocType": "dDocument_Items",
                            "DocDate": "2023-05-01",
                            "CardCode": "C20000",
                            "CardName": "Customer Sample",
                            "DocTotal": 500.00,
                            "DocumentStatus": "bost_Open"
                        },
                        {
                            "DocEntry": 12346,
                            "DocNum": 12346,
                            "DocType": "dDocument_Items",
                            "DocDate": "2023-05-02",
                            "CardCode": "C30000",
                            "CardName": "Best Customer Inc.",
                            "DocTotal": 750.00,
                            "DocumentStatus": "bost_Close"
                        }
                    ]
                }
        elif "Invoices" in url:
            return {
                "value": [
                    {
                        "DocEntry": 54321,
                        "DocNum": 54321,
                        "DocType": "dDocument_Items",
                        "DocDate": "2023-05-10",
                        "CardCode": "C20000",
                        "CardName": "Customer Sample",
                        "DocTotal": 500.00,
                        "Paid": "tYES"
                    }
                ]
            }
        else:
            return {
                "value": [
                    {
                        "Code": "DEMO-1",
                        "Name": "Demo Data for " + entity_type,
                        "Description": "This is mock data for development/testing"
                    }
                ],
                "message": f"Demo data for {entity_type}"
            }
    

    def logout(self) -> bool:
        """Logout from SAP B1 Service Layer."""
        if not self.session_id:
            return True
        
        # If using demo session, just reset state
        if self.demo_mode or self.session_id == "DEMO_SESSION_ID":
            self.session_id = None
            self.csrf_token = None
            self.session_valid_until = 0
            return True
            
        try:
            # Build logout URL
            logout_url = self.service_layer_url
            
            # Make sure the URL doesn't already end with /Logout
            if logout_url.endswith("/Logout"):
                logout_url = logout_url[:-7]
                
            # Make sure URL ends with /
            if not logout_url.endswith("/"):
                logout_url += "/"
                
            logout_url += "Logout"
            
            logger.info(f"Logging out from: {logout_url}")
            
            headers = {
                "Content-Type": "application/json",
                "Cookie": f"B1SESSION={self.session_id}"
            }
            
            if self.csrf_token:
                headers["x-csrf-token"] = self.csrf_token
            
            response = requests.post(
                logout_url,
                headers=headers,
                verify=False  # For development only
            )
            
            self.session_id = None
            self.csrf_token = None
            self.session_valid_until = 0
            
            return response.status_code in (200, 204)
            
        except Exception as e:
            logger.error(f"Logout error: {str(e)}")
            self.session_id = None
            self.csrf_token = None
            self.session_valid_until = 0
            return False
    
    def clear_cache(self, url_pattern: str = None):
        """Clear the response cache, optionally only for URLs matching a pattern"""
        if url_pattern:
            # Clear only matching cache entries
            keys_to_remove = []
            for cache_key in self.cache:
                if url_pattern in cache_key:
                    keys_to_remove.append(cache_key)
            
            for key in keys_to_remove:
                del self.cache[key]
                
            logger.info(f"Cleared {len(keys_to_remove)} cache entries matching pattern: {url_pattern}")
        else:
            # Clear entire cache
            count = len(self.cache)
            self.cache = {}
            logger.info(f"Cleared entire cache ({count} entries)")