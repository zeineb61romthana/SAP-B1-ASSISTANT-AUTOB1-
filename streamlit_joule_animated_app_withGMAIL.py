# app.py - Professional SAP B1 Business Intelligence Platform with Enhanced Invoice Management
# Enterprise-grade interface following SAP Fiori design principles
# Built for SAP functional managers and business executives

import streamlit as st
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List
import re
import base64
from io import StringIO, BytesIO
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Import your existing workflow
# from graph.enhanced_workflow import EnhancedSAPDataWorkflow

# Mock workflow for demo purposes - replace with your actual import
class EnhancedSAPDataWorkflow:
    def __init__(self):
        # Initialize Gmail integration capabilities
        self.gmail_enabled = True
        
    def invoke(self, params):
        # Mock data for demonstration
        import random
        query = params.get('query', '').lower()
        action = params.get('gmail_action', '')
        
        # Handle Gmail actions
        if action == 'get_messages':
            return {
                'status': 'success',
                'message_count': random.randint(5, 15),
                'messages': [
                    {
                        'message_id': f'msg_{i}',
                        'sender': f'customer{i}@company.com',
                        'subject': f'Invoice Request - Order #{1000+i}',
                        'body': f'Hello, I need the invoice for order #{1000+i}. Please send it to this email.',
                        'received_at': (datetime.now() - timedelta(hours=random.randint(1, 48))).isoformat(),
                        'is_invoice_request': True
                    }
                    for i in range(random.randint(3, 8))
                ]
            }
        elif action == 'process_message':
            return {
                'status': 'success',
                'response': f"Invoice request processed successfully. Invoice sent to {params.get('message_data', {}).get('sender', 'customer')}.",
                'ticket_created': False
            }
        elif action == 'lookup_partner':
            email = params.get('email_address', '')
            return {
                'status': 'found',
                'partner_id': 'C20000',
                'name': 'ABC Corporation',
                'email': email,
                'customer_type': 'Customer',
                'phone': '+1-555-0123',
                'address': '123 Business St, City'
            }
        elif action == 'get_latest_order':
            return {
                'status': 'found',
                'order_id': '12345',
                'customer_name': 'ABC Corporation',
                'order_date': '2024-01-15',
                'amount': 15750.00,
                'currency': 'USD',
                'order_status': 'Delivered'
            }
        elif action == 'generate_report':
            return {
                'status': 'success',
                'report_path': f'/reports/invoice_{params.get("invoice_id", "12345")}.pdf',
                'report_filename': f'invoice_{params.get("invoice_id", "12345")}.pdf',
                'file_size': '245 KB'
            }
        
        # Regular query handling
        if 'revenue' in query or 'invoice' in query:
            return {
                'response': {
                    'value': [
                        {
                            'DocNum': f'INV-{1000+i}',
                            'CustomerName': f'Customer {chr(65+i%26)}',
                            'DocTotal': random.randint(1000, 50000),
                            'DocDate': f'2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}',
                            'Status': random.choice(['Open', 'Paid', 'Overdue']),
                            'DaysOverdue': random.randint(0, 90) if random.choice([True, False]) else 0,
                            'PaymentMethod': random.choice(['Credit Card', 'Bank Transfer', 'Check', 'Cash'])
                        } 
                        for i in range(random.randint(15, 35))
                    ]
                },
                'odata_url': 'https://sap-b1-server:50000/b1s/v1/Invoices',
                'intent': 'invoice_analysis',
                'method_used': 'odata_query'
            }
        elif 'stock' in query or 'inventory' in query:
            return {
                'response': {
                    'value': [
                        {
                            'ItemCode': f'ITM-{1000+i:04d}',
                            'ItemName': f'Product {chr(65+i%26)}-{i+1}',
                            'QuantityOnStock': random.randint(0, 500),
                            'ReorderLevel': random.randint(10, 50),
                            'UnitPrice': round(random.uniform(10, 1000), 2),
                            'Warehouse': random.choice(['WH-01', 'WH-02', 'WH-03'])
                        } 
                        for i in range(random.randint(20, 40))
                    ]
                },
                'odata_url': 'https://sap-b1-server:50000/b1s/v1/Items',
                'intent': 'inventory_analysis'
            }
        elif 'order' in query:
            return {
                'response': {
                    'value': [
                        {
                            'DocNum': f'SO-{2000+i}',
                            'CustomerName': f'Customer {chr(65+i%26)}',
                            'Status': random.choice(['Open', 'Delivered', 'Cancelled']),
                            'DocTotal': random.randint(500, 25000),
                            'DocDate': f'2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}',
                            'DeliveryDate': f'2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}'
                        } 
                        for i in range(random.randint(10, 25))
                    ]
                },
                'odata_url': 'https://sap-b1-server:50000/b1s/v1/Orders'
            }
        else:
            return {
                'response': {
                    'value': [
                        {
                            'ID': i+1,
                            'Description': f'Business Item {i+1}',
                            'Value': random.randint(100, 5000),
                            'Category': random.choice(['Sales', 'Purchasing', 'Inventory', 'Finance'])
                        } 
                        for i in range(random.randint(8, 20))
                    ]
                }
            }

# ============================================================================
# PROFESSIONAL ENTERPRISE CSS - SAP Fiori Inspired
# ============================================================================

def apply_enterprise_css():
    st.markdown("""
    <style>
    /* Import SAP-standard fonts */
    @import url('https://fonts.googleapis.com/css2?family=72:wght@400;600;700&family=Source+Sans+Pro:wght@400;600;700&display=swap');
    
    /* SAP Enterprise Color Palette */
    :root {
        --sap-blue: #0F69A0;
        --sap-blue-light: #427CAC;
        --sap-blue-dark: #0A5C96;
        --sap-gray-1: #FAFAFA;
        --sap-gray-2: #F7F7F7;
        --sap-gray-3: #EDEDED;
        --sap-gray-4: #D5D5D5;
        --sap-gray-5: #CCCCCC;
        --sap-gray-6: #999999;
        --sap-gray-7: #666666;
        --sap-gray-8: #333333;
        --sap-white: #FFFFFF;
        --sap-success: #30914C;
        --sap-error: #E52929;
        --sap-warning: #E26300;
        --sap-info: #0F69A0;
        --sap-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        --sap-border: 1px solid var(--sap-gray-4);
        --enterprise-transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
    }
    
    /* Global Reset and Base Styles */
    * {
        box-sizing: border-box;
    }
    
    .main .block-container {
        padding-top: 1rem;
        max-width: 1600px;
        font-family: '72', 'Source Sans Pro', 'Helvetica Neue', Arial, sans-serif;
        color: var(--sap-gray-8);
        background-color: var(--sap-gray-1);
    }
    
    /* Enterprise Header */
    .enterprise-header {
        background: linear-gradient(135deg, var(--sap-blue) 0%, var(--sap-blue-dark) 100%);
        color: var(--sap-white);
        padding: 1.5rem 2rem;
        margin: -1rem -1rem 2rem -1rem;
        border-bottom: 3px solid var(--sap-blue-dark);
    }
    
    .enterprise-header h1 {
        font-size: 1.75rem;
        font-weight: 600;
        margin: 0;
        font-family: '72', sans-serif;
    }
    
    .enterprise-header .subtitle {
        font-size: 0.875rem;
        opacity: 0.9;
        margin-top: 0.25rem;
        font-weight: 400;
    }
    
    .enterprise-header .user-info {
        float: right;
        font-size: 0.75rem;
        opacity: 0.8;
    }
    
    /* Professional KPI Cards */
    .kpi-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
        gap: 1rem;
        margin-bottom: 2rem;
    }
    
    .kpi-card {
        background: var(--sap-white);
        border: var(--sap-border);
        border-radius: 4px;
        padding: 1.5rem;
        box-shadow: var(--sap-shadow);
        transition: var(--enterprise-transition);
        cursor: pointer;
        position: relative;
    }
    
    .kpi-card:hover {
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.12);
        border-color: var(--sap-blue-light);
    }
    
    .kpi-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        width: 4px;
        height: 100%;
        background: var(--sap-blue);
        border-radius: 4px 0 0 4px;
    }
    
    .kpi-title {
        font-size: 0.75rem;
        font-weight: 600;
        color: var(--sap-gray-7);
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.5rem;
    }
    
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        color: var(--sap-gray-8);
        margin-bottom: 0.25rem;
        line-height: 1;
    }
    
    .kpi-trend {
        font-size: 0.75rem;
        font-weight: 600;
    }
    
    .trend-positive { color: var(--sap-success); }
    .trend-negative { color: var(--sap-error); }
    .trend-neutral { color: var(--sap-gray-6); }
    
    /* Professional Section Headers */
    .section-header {
        background: var(--sap-white);
        border: var(--sap-border);
        border-radius: 4px;
        padding: 1rem 1.5rem;
        margin-bottom: 1rem;
        box-shadow: var(--sap-shadow);
    }
    
    .section-title {
        font-size: 1.125rem;
        font-weight: 600;
        color: var(--sap-gray-8);
        margin: 0;
    }
    
    .section-subtitle {
        font-size: 0.875rem;
        color: var(--sap-gray-6);
        margin-top: 0.25rem;
    }
    
    /* Invoice Management Specific Styles */
    .invoice-workflow-card {
        background: var(--sap-white);
        border: var(--sap-border);
        border-radius: 8px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        box-shadow: var(--sap-shadow);
        border-left: 4px solid var(--sap-blue);
    }
    
    .workflow-step {
        display: flex;
        align-items: center;
        padding: 1rem;
        border-radius: 4px;
        margin-bottom: 0.5rem;
        background: var(--sap-gray-1);
        border: 1px solid var(--sap-gray-3);
    }
    
    .workflow-step.active {
        background: #EAF2F8;
        border-color: var(--sap-blue-light);
    }
    
    .workflow-step.completed {
        background: #F1F8F3;
        border-color: var(--sap-success);
    }
    
    .workflow-step-number {
        background: var(--sap-blue);
        color: var(--sap-white);
        width: 24px;
        height: 24px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 0.75rem;
        font-weight: 600;
        margin-right: 1rem;
    }
    
    .gmail-integration-panel {
        background: linear-gradient(135deg, #F8F9FA 0%, #E9ECEF 100%);
        border: 2px solid var(--sap-blue-light);
        border-radius: 8px;
        padding: 1.5rem;
        margin-bottom: 1rem;
    }
    
    .customer-lookup-form {
        background: var(--sap-white);
        border: var(--sap-border);
        border-radius: 4px;
        padding: 1.5rem;
        box-shadow: var(--sap-shadow);
    }
    
    /* Enterprise Data Container */
    .data-container {
        background: var(--sap-white);
        border: var(--sap-border);
        border-radius: 4px;
        box-shadow: var(--sap-shadow);
        overflow: hidden;
    }
    
    .data-container-header {
        background: var(--sap-gray-2);
        border-bottom: var(--sap-border);
        padding: 1rem 1.5rem;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    
    .data-container-title {
        font-size: 1rem;
        font-weight: 600;
        color: var(--sap-gray-8);
        margin: 0;
    }
    
    .data-container-actions {
        display: flex;
        gap: 0.5rem;
    }
    
    /* Professional Buttons */
    .btn-primary {
        background: var(--sap-blue);
        color: var(--sap-white);
        border: none;
        border-radius: 4px;
        padding: 0.5rem 1rem;
        font-size: 0.875rem;
        font-weight: 600;
        cursor: pointer;
        transition: var(--enterprise-transition);
        text-decoration: none;
        display: inline-block;
    }
    
    .btn-primary:hover {
        background: var(--sap-blue-dark);
        transform: translateY(-1px);
    }
    
    .btn-secondary {
        background: var(--sap-white);
        color: var(--sap-gray-7);
        border: var(--sap-border);
        border-radius: 4px;
        padding: 0.5rem 1rem;
        font-size: 0.875rem;
        font-weight: 600;
        cursor: pointer;
        transition: var(--enterprise-transition);
    }
    
    .btn-secondary:hover {
        background: var(--sap-gray-2);
        border-color: var(--sap-blue);
    }
    
    .btn-group {
        display: flex;
        border-radius: 4px;
        overflow: hidden;
        border: var(--sap-border);
    }
    
    .btn-group button {
        border: none;
        background: var(--sap-white);
        padding: 0.5rem 1rem;
        font-size: 0.875rem;
        font-weight: 600;
        cursor: pointer;
        border-right: var(--sap-border);
        transition: var(--enterprise-transition);
    }
    
    .btn-group button:last-child {
        border-right: none;
    }
    
    .btn-group button:hover {
        background: var(--sap-gray-2);
    }
    
    .btn-group button.active {
        background: var(--sap-blue);
        color: var(--sap-white);
    }
    
    /* Professional Tables */
    .enterprise-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.875rem;
    }
    
    .enterprise-table th {
        background: var(--sap-gray-2);
        color: var(--sap-gray-8);
        font-weight: 600;
        text-align: left;
        padding: 0.75rem 1rem;
        border-bottom: 2px solid var(--sap-gray-4);
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .enterprise-table td {
        padding: 0.75rem 1rem;
        border-bottom: 1px solid var(--sap-gray-3);
        vertical-align: middle;
    }
    
    .enterprise-table tbody tr:hover {
        background: var(--sap-gray-1);
    }
    
    /* Professional Form Elements */
    .form-group {
        margin-bottom: 1rem;
    }
    
    .form-label {
        display: block;
        font-size: 0.875rem;
        font-weight: 600;
        color: var(--sap-gray-7);
        margin-bottom: 0.25rem;
    }
    
    .form-control {
        width: 100%;
        padding: 0.5rem 0.75rem;
        border: var(--sap-border);
        border-radius: 4px;
        font-size: 0.875rem;
        background: var(--sap-white);
        transition: var(--enterprise-transition);
    }
    
    .form-control:focus {
        outline: none;
        border-color: var(--sap-blue);
        box-shadow: 0 0 0 2px rgba(15, 105, 160, 0.1);
    }
    
    /* Professional Alerts */
    .alert {
        padding: 0.75rem 1rem;
        border-radius: 4px;
        margin-bottom: 1rem;
        font-size: 0.875rem;
        border-left: 4px solid;
    }
    
    .alert-success {
        background: #F1F8F3;
        border-left-color: var(--sap-success);
        color: #1E4F23;
    }
    
    .alert-warning {
        background: #FDF2E9;
        border-left-color: var(--sap-warning);
        color: #8B4513;
    }
    
    .alert-error {
        background: #FBEAEA;
        border-left-color: var(--sap-error);
        color: #721C24;
    }
    
    .alert-info {
        background: #EAF2F8;
        border-left-color: var(--sap-info);
        color: #1B4F72;
    }
    
    /* Professional Sidebar */
    .css-1d391kg {
        background: var(--sap-gray-2);
        border-right: var(--sap-border);
    }
    
    .sidebar-section {
        background: var(--sap-white);
        border: var(--sap-border);
        border-radius: 4px;
        margin-bottom: 1rem;
        overflow: hidden;
    }
    
    .sidebar-section-header {
        background: var(--sap-gray-2);
        padding: 0.75rem 1rem;
        border-bottom: var(--sap-border);
        font-weight: 600;
        font-size: 0.875rem;
        color: var(--sap-gray-8);
    }
    
    .sidebar-section-content {
        padding: 1rem;
    }
    
    /* Enterprise Metrics */
    .metric-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 0.5rem 0;
        border-bottom: 1px solid var(--sap-gray-3);
        font-size: 0.875rem;
    }
    
    .metric-row:last-child {
        border-bottom: none;
    }
    
    .metric-label {
        color: var(--sap-gray-7);
        font-weight: 400;
    }
    
    .metric-value {
        color: var(--sap-gray-8);
        font-weight: 600;
    }
    
    /* Professional Loading States */
    .loading-container {
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 2rem;
        color: var(--sap-gray-6);
    }
    
    .loading-spinner {
        width: 20px;
        height: 20px;
        border: 2px solid var(--sap-gray-4);
        border-top: 2px solid var(--sap-blue);
        border-radius: 50%;
        animation: spin 1s linear infinite;
        margin-right: 0.5rem;
    }
    
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    
    /* Enterprise Charts */
    .chart-container {
        background: var(--sap-white);
        border: var(--sap-border);
        border-radius: 4px;
        padding: 1rem;
        box-shadow: var(--sap-shadow);
        margin-bottom: 1rem;
    }
    
    .chart-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid var(--sap-gray-3);
    }
    
    .chart-title {
        font-size: 1rem;
        font-weight: 600;
        color: var(--sap-gray-8);
        margin: 0;
    }
    
    /* Professional Status Indicators */
    .status-indicator {
        display: inline-flex;
        align-items: center;
        padding: 0.25rem 0.5rem;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .status-open {
        background: #EAF2F8;
        color: var(--sap-blue);
    }
    
    .status-success {
        background: #F1F8F3;
        color: var(--sap-success);
    }
    
    .status-warning {
        background: #FDF2E9;
        color: var(--sap-warning);
    }
    
    .status-error {
        background: #FBEAEA;
        color: var(--sap-error);
    }
    
    /* Responsive Design */
    @media (max-width: 768px) {
        .enterprise-header {
            padding: 1rem;
        }
        
        .enterprise-header .user-info {
            float: none;
            display: block;
            margin-top: 0.5rem;
        }
        
        .kpi-grid {
            grid-template-columns: 1fr;
        }
        
        .data-container-header {
            flex-direction: column;
            gap: 0.5rem;
            align-items: flex-start;
        }
    }
    
    /* Hide Streamlit Elements */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    header { visibility: hidden; }
    
    /* Override Streamlit Default Styles */
    .stButton > button {
        background: var(--sap-blue);
        color: var(--sap-white);
        border: none;
        border-radius: 4px;
        padding: 0.5rem 1rem;
        font-size: 0.875rem;
        font-weight: 600;
        transition: var(--enterprise-transition);
        width: 100%;
    }
    
    .stButton > button:hover {
        background: var(--sap-blue-dark);
        border: none;
        color: var(--sap-white);
    }
    
    .stSelectbox > div > div {
        border: var(--sap-border);
        border-radius: 4px;
        font-size: 0.875rem;
    }
    
    .stTextInput > div > div > input {
        border: var(--sap-border);
        border-radius: 4px;
        font-size: 0.875rem;
        padding: 0.5rem 0.75rem;
    }
    
    .stDataFrame {
        border: var(--sap-border);
        border-radius: 4px;
        overflow: hidden;
    }
    
    </style>
    """, unsafe_allow_html=True)

# ============================================================================
# PROFESSIONAL CHART COMPONENTS
# ============================================================================

def create_enterprise_chart(df, chart_type, x_col, y_col, title=""):
    """Create professional, enterprise-grade charts"""
    
    # SAP color palette
    sap_colors = ['#0F69A0', '#427CAC', '#74A1C4', '#A5C4DB', '#D1E3F0']
    
    if chart_type == "bar":
        fig = px.bar(
            df, x=x_col, y=y_col, 
            title=title,
            color_discrete_sequence=sap_colors
        )
    elif chart_type == "line":
        fig = px.line(
            df, x=x_col, y=y_col,
            title=title,
            color_discrete_sequence=sap_colors
        )
        fig.update_traces(line=dict(width=2), marker=dict(size=4))
    elif chart_type == "area":
        fig = px.area(
            df, x=x_col, y=y_col,
            title=title,
            color_discrete_sequence=sap_colors
        )
    elif chart_type == "pie":
        pie_data = df.groupby(x_col)[y_col].sum().reset_index()
        fig = px.pie(
            pie_data, values=y_col, names=x_col,
            title=title,
            color_discrete_sequence=sap_colors
        )
    
    # Professional styling
    fig.update_layout(
        font=dict(family="Source Sans Pro, Arial, sans-serif", size=12),
        plot_bgcolor='white',
        paper_bgcolor='white',
        title=dict(
            font=dict(size=16, color='#333333'),
            x=0,
            pad=dict(b=20)
        ),
        margin=dict(l=40, r=40, t=60, b=40),
        height=400,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        )
    )
    
    # Clean grid lines
    fig.update_xaxes(
        gridcolor='#EDEDED',
        linecolor='#CCCCCC',
        title_font=dict(color='#666666'),
        tickfont=dict(color='#666666')
    )
    fig.update_yaxes(
        gridcolor='#EDEDED',
        linecolor='#CCCCCC',
        title_font=dict(color='#666666'),
        tickfont=dict(color='#666666')
    )
    
    return fig

# ============================================================================
# PROFESSIONAL COMPONENTS
# ============================================================================

def create_professional_kpi_card(title, value, trend=None, trend_direction="neutral"):
    """Create professional KPI card"""
    trend_class = f"trend-{trend_direction}"
    trend_symbol = {
        "positive": "↗",
        "negative": "↘", 
        "neutral": "→"
    }.get(trend_direction, "→")
    
    trend_html = f'<div class="kpi-trend {trend_class}">{trend_symbol} {trend}</div>' if trend else ""
    
    return f"""
    <div class="kpi-card">
        <div class="kpi-title">{title}</div>
        <div class="kpi-value">{value}</div>
        {trend_html}
    </div>
    """

def create_professional_view_toggle():
    """Create professional view toggle"""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        table_view = st.button("Table View", use_container_width=True, key="view_table")
    with col2:
        json_view = st.button("JSON View", use_container_width=True, key="view_json")
    with col3:
        chart_view = st.button("Chart View", use_container_width=True, key="view_chart")
    
    if table_view:
        return "table"
    elif json_view:
        return "json"
    elif chart_view:
        return "chart"
    else:
        return st.session_state.get('current_view', 'table')

def show_professional_alerts(alerts):
    """Display professional alerts"""
    for alert in alerts:
        alert_type = alert.get("type", "info")
        message = alert.get("message", "")
        
        if alert_type == "warning":
            st.markdown(f'<div class="alert alert-warning">{message}</div>', unsafe_allow_html=True)
        elif alert_type == "success":
            st.markdown(f'<div class="alert alert-success">{message}</div>', unsafe_allow_html=True)
        elif alert_type == "error":
            st.markdown(f'<div class="alert alert-error">{message}</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="alert alert-info">{message}</div>', unsafe_allow_html=True)

def format_status_indicator(status):
    """Format status with professional indicator"""
    status_classes = {
        'open': 'status-open',
        'paid': 'status-success',
        'delivered': 'status-success',
        'overdue': 'status-error',
        'cancelled': 'status-error',
        'pending': 'status-warning'
    }
    
    status_class = status_classes.get(status.lower(), 'status-open')
    return f'<span class="status-indicator {status_class}">{status}</span>'

# ============================================================================
# INVOICE MANAGEMENT COMPONENTS
# ============================================================================

def create_invoice_workflow_step(step_number, title, description, status="pending"):
    """Create a professional workflow step indicator"""
    status_class = {
        "pending": "",
        "active": "active",
        "completed": "completed"
    }.get(status, "")
    
    return f"""
    <div class="workflow-step {status_class}">
        <div class="workflow-step-number">{step_number}</div>
        <div>
            <strong>{title}</strong><br>
            <small>{description}</small>
        </div>
    </div>
    """

def render_gmail_integration_panel():
    """Render Gmail integration panel for invoice processing"""
    st.markdown("""
    <div class="gmail-integration-panel">
        <h3 style="margin-top: 0; color: #0F69A0;">📧 Gmail Invoice Processing</h3>
        <p style="margin-bottom: 1rem; color: #666;">Automated processing of customer invoice requests from Gmail</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("📨 Check Invoice Requests", use_container_width=True, key="check_gmail"):
            return "check_messages"
    
    with col2:
        if st.button("⚙️ Process All Requests", use_container_width=True, key="process_all"):
            return "process_all"
    
    return None

def render_customer_lookup_form():
    """Render customer lookup form"""
    st.markdown("""
    <div class="customer-lookup-form">
        <h4 style="margin-top: 0; color: #333;">🔍 Customer Lookup & Invoice Generation</h4>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        customer_email = st.text_input(
            "Customer Email Address",
            placeholder="customer@company.com",
            key="customer_email_lookup"
        )
    
    with col2:
        lookup_action = st.selectbox(
            "Action",
            ["Lookup Customer", "Get Latest Order", "Generate Invoice"],
            key="lookup_action"
        )
    
    if st.button("Execute Action", use_container_width=True, key="execute_lookup"):
        return {"email": customer_email, "action": lookup_action}
    
    return None

def render_invoice_aging_analysis():
    """Render invoice aging analysis chart"""
    # Create sample aging data
    aging_data = pd.DataFrame({
        'Age_Range': ['0-30 days', '31-60 days', '61-90 days', '90+ days'],
        'Invoice_Count': [45, 23, 12, 8],
        'Total_Amount': [125000, 87000, 45000, 32000]
    })
    
    # Create aging chart
    fig = px.bar(
        aging_data, 
        x='Age_Range', 
        y='Total_Amount',
        title='Invoice Aging Analysis',
        color='Invoice_Count',
        color_continuous_scale=['#D1E3F0', '#0F69A0']
    )
    
    fig.update_layout(
        font=dict(family="Source Sans Pro, Arial, sans-serif", size=12),
        plot_bgcolor='white',
        paper_bgcolor='white',
        height=350
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    return aging_data

def render_crystal_reports_section():
    """Render Crystal Reports generation section"""
    st.markdown("""
    <div class="invoice-workflow-card">
        <h4 style="margin-top: 0; color: #333;">📊 Crystal Reports Generation</h4>
        <p style="color: #666; margin-bottom: 1rem;">Generate professional PDF reports for invoices and orders</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        report_type = st.selectbox(
            "Report Type",
            ["Invoice Report", "Order Report", "Customer Statement"],
            key="crystal_report_type"
        )
    
    with col2:
        record_id = st.text_input(
            "Record ID",
            placeholder="INV-12345",
            key="crystal_record_id"
        )
    
    with col3:
        if st.button("Generate Report", use_container_width=True, key="generate_crystal"):
            return {"type": report_type, "id": record_id}
    
    return None

# ============================================================================
# CACHING AND PERFORMANCE (Maintained)
# ============================================================================

@st.cache_resource
def get_workflow():
    """Initialize workflow with caching"""
    return EnhancedSAPDataWorkflow()

@st.cache_data(ttl=300)
def execute_sap_query(query: str, output_format: str = "json"):
    """Execute SAP query with caching"""
    workflow = get_workflow()
    return workflow.invoke({
        "query": query,
        "output_format": output_format
    })

@st.cache_data(ttl=300)
def execute_gmail_action(action: str, **kwargs):
    """Execute Gmail-related actions with caching"""
    workflow = get_workflow()
    return workflow.invoke({
        "gmail_action": action,
        **kwargs
    })

@st.cache_data(ttl=300)
def get_single_kpi(kpi_type: str):
    """Get individual KPI with caching"""
    workflow = get_workflow()
    
    queries = {
        'revenue': "sum of invoice totals this month",
        'pending_orders': "count open orders", 
        'low_stock': "count items with stock below 10",
        'overdue': "count overdue invoices"
    }
    
    try:
        if kpi_type in queries:
            result = workflow.invoke({
                "query": queries[kpi_type],
                "output_format": "json"
            })
            if 'response' in result and result['response']:
                if kpi_type == 'revenue':
                    return extract_total_from_response(result['response'])
                else:
                    return extract_count_from_response(result['response'])
        return "N/A"
    except:
        return "Error"

def extract_total_from_response(response):
    """Extract total value from SAP response"""
    if isinstance(response, dict):
        if 'value' in response and isinstance(response['value'], list):
            total = sum(item.get('DocTotal', 0) for item in response['value'] if isinstance(item, dict))
            return f"${total:,.2f}"
    return "N/A"

def extract_count_from_response(response):
    """Extract count from SAP response"""
    if isinstance(response, dict):
        if 'value' in response and isinstance(response['value'], list):
            return str(len(response['value']))
    return "N/A"

def apply_business_alerts_professional(df):
    """Professional business alerts"""
    alerts = []
    
    if df.empty:
        return df, alerts
    
    # Check for low stock
    if 'QuantityOnStock' in df.columns and 'ReorderLevel' in df.columns:
        low_stock = df[df['QuantityOnStock'] < df['ReorderLevel']]
        if len(low_stock) > 0:
            alerts.append({
                "type": "warning",
                "message": f"Stock Alert: {len(low_stock)} items below reorder level"
            })
    
    # Check for high value transactions
    if 'DocTotal' in df.columns:
        high_value = df[df['DocTotal'] > 10000]
        if len(high_value) > 0:
            alerts.append({
                "type": "info",
                "message": f"High Value: {len(high_value)} transactions above $10,000"
            })
    
    # Check for overdue items
    if 'Status' in df.columns:
        overdue = df[df['Status'].str.lower() == 'overdue']
        if len(overdue) > 0:
            alerts.append({
                "type": "error",
                "message": f"Critical: {len(overdue)} overdue items require attention"
            })
    
    # Invoice-specific alerts
    if 'DaysOverdue' in df.columns:
        severely_overdue = df[df['DaysOverdue'] > 60]
        if len(severely_overdue) > 0:
            alerts.append({
                "type": "error",
                "message": f"Urgent: {len(severely_overdue)} invoices over 60 days overdue"
            })
    
    return df, alerts

# ============================================================================
# ENHANCED BUSINESS QUERIES WITH INVOICE MANAGEMENT
# ============================================================================

BUSINESS_QUERIES = {
    "Financial Analysis": {
        "Revenue Analysis": {
            "query": "monthly revenue analysis with customer breakdown",
            "description": "Comprehensive revenue analysis by month and customer"
        },
        "Invoice Aging": {
            "query": "invoice aging analysis with overdue categorization",
            "description": "Outstanding invoices categorized by aging periods"
        },
        "Payment Analysis": {
            "query": "payment trends and collection analysis",
            "description": "Payment performance and collection metrics"
        },
        "Cash Flow Forecast": {
            "query": "cash flow forecast based on open invoices",
            "description": "Projected cash flow from outstanding receivables"
        }
    },
    "Invoice Management": {
        "Overdue Invoices": {
            "query": "overdue invoices requiring immediate attention",
            "description": "Critical overdue invoices with customer details"
        },
        "Payment Tracking": {
            "query": "invoice payment status and history",
            "description": "Track payment status and collection activities"
        },
        "Invoice Distribution": {
            "query": "invoice distribution by customer and amount",
            "description": "Analysis of invoice volume and value distribution"
        },
        "Collection Activities": {
            "query": "collection activities and success rates",
            "description": "Track collection efforts and outcomes"
        }
    },
    "Sales Operations": {
        "Sales Performance": {
            "query": "sales team performance metrics",
            "description": "Individual and team sales performance indicators"
        },
        "Customer Analysis": {
            "query": "top customers by revenue and volume",
            "description": "Key customer analysis and segmentation"
        },
        "Order Management": {
            "query": "order fulfillment and delivery analysis",
            "description": "Order processing and delivery performance"
        }
    },
    "Inventory Control": {
        "Stock Levels": {
            "query": "inventory levels and reorder requirements",
            "description": "Current stock status and reorder recommendations"
        },
        "Warehouse Analysis": {
            "query": "warehouse utilization and efficiency",
            "description": "Warehouse performance and capacity analysis"
        },
        "Product Performance": {
            "query": "product movement and profitability analysis",
            "description": "Product performance and margin analysis"
        }
    }
}

# ============================================================================
# MAIN PROFESSIONAL APPLICATION WITH INVOICE MANAGEMENT
# ============================================================================

def main():
    # Apply professional CSS
    apply_enterprise_css()
    
    # Page configuration
    st.set_page_config(
        page_title="SAP Business One - Business Intelligence",
        page_icon="🏢",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Professional header
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    st.markdown(f"""
    <div class="enterprise-header">
        <div class="user-info">User: Administrator | Session: {current_time}</div>
        <h1>SAP Business One - Business Intelligence</h1>
        <div class="subtitle">Enterprise Data Analytics and Invoice Management Platform</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize session state
    if 'selected_data' not in st.session_state:
        st.session_state.selected_data = None
    if 'current_query' not in st.session_state:
        st.session_state.current_query = ""
    if 'current_view' not in st.session_state:
        st.session_state.current_view = "table"
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = "overview"
    
    # ========================================================================
    # MAIN NAVIGATION TABS
    # ========================================================================
    
    main_tabs = st.tabs(["📊 Overview", "📧 Invoice Management", "📈 Analytics", "⚙️ Administration"])
    
    # ========================================================================
    # TAB 1: OVERVIEW - PROFESSIONAL KPI DASHBOARD
    # ========================================================================
    
    with main_tabs[0]:
        st.markdown("""
        <div class="section-header">
            <div class="section-title">Key Performance Indicators</div>
            <div class="section-subtitle">Real-time business metrics and performance indicators</div>
        </div>
        """, unsafe_allow_html=True)
        
        # KPI Grid
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("Revenue Analysis", use_container_width=True, key="kpi_revenue"):
                st.session_state.current_query = "monthly revenue with customer breakdown"
                st.session_state.active_tab = "analytics"
                st.rerun()
            
            try:
                revenue_data = get_single_kpi('revenue')
                kpi_html = create_professional_kpi_card(
                    "Monthly Revenue", 
                    revenue_data, 
                    "8.5% vs. last month", 
                    "positive"
                )
                st.markdown(kpi_html, unsafe_allow_html=True)
            except:
                kpi_html = create_professional_kpi_card("Monthly Revenue", "Loading...", None, "neutral")
                st.markdown(kpi_html, unsafe_allow_html=True)
        
        with col2:
            if st.button("Order Management", use_container_width=True, key="kpi_orders"):
                st.session_state.current_query = "open sales orders analysis"
                st.session_state.active_tab = "analytics"
                st.rerun()
            
            try:
                orders_data = get_single_kpi('pending_orders')
                kpi_html = create_professional_kpi_card(
                    "Open Orders", 
                    orders_data, 
                    "12 new today", 
                    "neutral"
                )
                st.markdown(kpi_html, unsafe_allow_html=True)
            except:
                kpi_html = create_professional_kpi_card("Open Orders", "Loading...", None, "neutral")
                st.markdown(kpi_html, unsafe_allow_html=True)
        
        with col3:
            if st.button("Invoice Processing", use_container_width=True, key="kpi_invoices"):
                st.session_state.active_tab = "invoice_management"
                st.rerun()
            
            try:
                stock_data = get_single_kpi('low_stock')
                kpi_html = create_professional_kpi_card(
                    "Pending Invoices", 
                    "23", 
                    "Requires processing", 
                    "warning"
                )
                st.markdown(kpi_html, unsafe_allow_html=True)
            except:
                kpi_html = create_professional_kpi_card("Pending Invoices", "Loading...", None, "neutral")
                st.markdown(kpi_html, unsafe_allow_html=True)
        
        with col4:
            if st.button("Collections Status", use_container_width=True, key="kpi_collections"):
                st.session_state.current_query = "overdue invoices analysis"
                st.session_state.active_tab = "analytics"
                st.rerun()
            
            try:
                overdue_data = get_single_kpi('overdue')
                kpi_html = create_professional_kpi_card(
                    "Overdue Invoices", 
                    overdue_data, 
                    "Action required", 
                    "negative"
                )
                st.markdown(kpi_html, unsafe_allow_html=True)
            except:
                kpi_html = create_professional_kpi_card("Overdue Invoices", "Loading...", None, "neutral")
                st.markdown(kpi_html, unsafe_allow_html=True)
        
        # Quick Actions Section
        st.markdown("""
        <div class="section-header">
            <div class="section-title">Quick Actions</div>
            <div class="section-subtitle">Frequently used business operations</div>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("📧 Check Gmail Requests", use_container_width=True):
                st.session_state.active_tab = "invoice_management"
                st.rerun()
        
        with col2:
            if st.button("📊 Generate Reports", use_container_width=True):
                st.session_state.active_tab = "invoice_management"
                st.rerun()
        
        with col3:
            if st.button("🔍 Customer Lookup", use_container_width=True):
                st.session_state.active_tab = "invoice_management"
                st.rerun()
        
        with col4:
            if st.button("📈 Aging Analysis", use_container_width=True):
                st.session_state.current_query = "invoice aging analysis"
                st.session_state.active_tab = "analytics"
                st.rerun()
    
    # ========================================================================
    # TAB 2: INVOICE MANAGEMENT
    # ========================================================================
    
    with main_tabs[1]:
        st.markdown("""
        <div class="section-header">
            <div class="section-title">Invoice Management Center</div>
            <div class="section-subtitle">Automated invoice processing, customer service, and report generation</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Invoice Management Sub-tabs
        invoice_tabs = st.tabs(["📧 Gmail Processing", "🔍 Customer Lookup", "📊 Reports & Analytics", "🎫 Support Tickets"])
        
        # Gmail Processing Tab
        with invoice_tabs[0]:
            st.markdown("### Automated Email Invoice Processing")
            
            # Gmail Integration Panel
            gmail_action = render_gmail_integration_panel()
            
            if gmail_action == "check_messages":
                with st.spinner("Checking Gmail for invoice requests..."):
                    try:
                        result = execute_gmail_action("get_messages")
                        
                        if result.get('status') == 'success':
                            messages = result.get('messages', [])
                            
                            if messages:
                                st.success(f"Found {len(messages)} invoice requests in Gmail")
                                
                                # Display messages in a professional format
                                for i, msg in enumerate(messages):
                                    with st.expander(f"📧 {msg['sender']} - {msg['subject']}", expanded=(i==0)):
                                        col1, col2 = st.columns([2, 1])
                                        
                                        with col1:
                                            st.write(f"**From:** {msg['sender']}")
                                            st.write(f"**Subject:** {msg['subject']}")
                                            st.write(f"**Received:** {msg['received_at']}")
                                            st.write(f"**Message:** {msg['body']}")
                                        
                                        with col2:
                                            if st.button(f"Process Request", key=f"process_{i}"):
                                                with st.spinner("Processing request..."):
                                                    process_result = execute_gmail_action(
                                                        "process_message",
                                                        message_data=msg
                                                    )
                                                    
                                                    if process_result.get('status') == 'success':
                                                        st.success("✅ Invoice request processed successfully!")
                                                    else:
                                                        st.error("❌ Failed to process request")
                            else:
                                st.info("No invoice requests found in Gmail")
                        else:
                            st.error("Failed to connect to Gmail")
                            
                    except Exception as e:
                        st.error(f"Error checking Gmail: {str(e)}")
            
            elif gmail_action == "process_all":
                st.info("🔄 Processing all pending invoice requests... This may take a few minutes.")
                
                # Simulate batch processing
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for i in range(5):
                    time.sleep(1)
                    progress_bar.progress((i + 1) / 5)
                    status_text.text(f"Processing request {i + 1} of 5...")
                
                st.success("✅ All invoice requests have been processed successfully!")
            
            # Gmail Integration Workflow
            st.markdown("### Workflow Process")
            
            workflow_steps = [
                create_invoice_workflow_step(1, "Email Detection", "Monitor Gmail for invoice requests", "completed"),
                create_invoice_workflow_step(2, "Customer Identification", "Lookup customer in SAP B1", "completed"),
                create_invoice_workflow_step(3, "Order Retrieval", "Find customer's latest order", "active"),
                create_invoice_workflow_step(4, "Invoice Generation", "Generate Crystal Report", "pending"),
                create_invoice_workflow_step(5, "Email Delivery", "Send invoice to customer", "pending")
            ]
            
            for step in workflow_steps:
                st.markdown(step, unsafe_allow_html=True)
        
        # Customer Lookup Tab
        with invoice_tabs[1]:
            st.markdown("### Customer Information & Invoice Services")
            
            lookup_result = render_customer_lookup_form()
            
            if lookup_result:
                email = lookup_result['email']
                action = lookup_result['action']
                
                if email:
                    with st.spinner(f"Executing {action.lower()}..."):
                        if action == "Lookup Customer":
                            result = execute_gmail_action("lookup_partner", email_address=email)
                            
                            if result.get('status') == 'found':
                                st.success("✅ Customer found in SAP B1!")
                                
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    st.markdown("""
                                    **Customer Information:**
                                    - **Name:** {}
                                    - **ID:** {}
                                    - **Type:** {}
                                    """.format(
                                        result.get('name', 'N/A'),
                                        result.get('partner_id', 'N/A'),
                                        result.get('customer_type', 'N/A')
                                    ))
                                
                                with col2:
                                    st.markdown("""
                                    **Contact Details:**
                                    - **Email:** {}
                                    - **Phone:** {}
                                    - **Address:** {}
                                    """.format(
                                        result.get('email', 'N/A'),
                                        result.get('phone', 'N/A'),
                                        result.get('address', 'N/A')
                                    ))
                            else:
                                st.warning("⚠️ Customer not found in SAP B1")
                        
                        elif action == "Get Latest Order":
                            result = execute_gmail_action("get_latest_order", email_address=email)
                            
                            if result.get('status') == 'found':
                                st.success("✅ Latest order found!")
                                
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    st.markdown("""
                                    **Order Information:**
                                    - **Order ID:** {}
                                    - **Customer:** {}
                                    - **Date:** {}
                                    """.format(
                                        result.get('order_id', 'N/A'),
                                        result.get('customer_name', 'N/A'),
                                        result.get('order_date', 'N/A')
                                    ))
                                
                                with col2:
                                    st.markdown("""
                                    **Financial Details:**
                                    - **Amount:** {} {}
                                    - **Status:** {}
                                    """.format(
                                        result.get('currency', '$'),
                                        result.get('amount', 0),
                                        result.get('order_status', 'N/A')
                                    ))
                            else:
                                st.warning("⚠️ No orders found for this customer")
                        
                        elif action == "Generate Invoice":
                            # First get the order, then generate invoice
                            order_result = execute_gmail_action("get_latest_order", email_address=email)
                            
                            if order_result.get('status') == 'found':
                                invoice_id = order_result.get('order_id', '12345')
                                report_result = execute_gmail_action("generate_report", invoice_id=invoice_id)
                                
                                if report_result.get('status') == 'success':
                                    st.success("✅ Invoice generated successfully!")
                                    
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        st.markdown("""
                                        **Generated Report:**
                                        - **File:** {}
                                        - **Size:** {}
                                        - **Path:** {}
                                        """.format(
                                            report_result.get('report_filename', 'N/A'),
                                            report_result.get('file_size', 'N/A'),
                                            report_result.get('report_path', 'N/A')
                                        ))
                                    
                                    with col2:
                                        if st.button("📧 Send to Customer", use_container_width=True):
                                            st.success("✅ Invoice sent to customer via email!")
                                else:
                                    st.error("❌ Failed to generate invoice report")
                            else:
                                st.warning("⚠️ No order found to generate invoice from")
                else:
                    st.warning("⚠️ Please enter a customer email address")
        
        # Reports & Analytics Tab
        with invoice_tabs[2]:
            st.markdown("### Invoice Reports & Analytics")
            
            # Crystal Reports Section
            crystal_result = render_crystal_reports_section()
            
            if crystal_result:
                report_type = crystal_result['type']
                record_id = crystal_result['id']
                
                if record_id:
                    with st.spinner("Generating Crystal Report..."):
                        result = execute_gmail_action("generate_report", invoice_id=record_id)
                        
                        if result.get('status') == 'success':
                            st.success(f"✅ {report_type} generated successfully!")
                            
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                st.metric("File Size", result.get('file_size', 'N/A'))
                            
                            with col2:
                                st.metric("Report Type", report_type)
                            
                            with col3:
                                if st.button("📥 Download Report", use_container_width=True):
                                    st.success("Report download initiated!")
                        else:
                            st.error("❌ Failed to generate report")
                else:
                    st.warning("⚠️ Please enter a Record ID")
            
            # Invoice Aging Analysis
            st.markdown("### Invoice Aging Analysis")
            aging_data = render_invoice_aging_analysis()
            
            # Display aging summary
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("0-30 Days", f"${aging_data.iloc[0]['Total_Amount']:,.0f}", f"{aging_data.iloc[0]['Invoice_Count']} invoices")
            
            with col2:
                st.metric("31-60 Days", f"${aging_data.iloc[1]['Total_Amount']:,.0f}", f"{aging_data.iloc[1]['Invoice_Count']} invoices")
            
            with col3:
                st.metric("61-90 Days", f"${aging_data.iloc[2]['Total_Amount']:,.0f}", f"{aging_data.iloc[2]['Invoice_Count']} invoices")
            
            with col4:
                st.metric("90+ Days", f"${aging_data.iloc[3]['Total_Amount']:,.0f}", f"{aging_data.iloc[3]['Invoice_Count']} invoices")
        
        # Support Tickets Tab
        with invoice_tabs[3]:
            st.markdown("### Support Ticket Management")
            
            # Support ticket creation form
            st.markdown("""
            <div class="invoice-workflow-card">
                <h4 style="margin-top: 0; color: #333;">🎫 Create Support Ticket</h4>
                <p style="color: #666; margin-bottom: 1rem;">Create tickets for issues that require human intervention</p>
            </div>
            """, unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                ticket_title = st.text_input("Issue Title", placeholder="Invoice processing error")
                customer_email = st.text_input("Customer Email", placeholder="customer@company.com")
            
            with col2:
                priority = st.selectbox("Priority", ["Low", "Normal", "High", "Critical"])
                category = st.selectbox("Category", ["Invoice Issues", "Payment Problems", "Technical Support", "Data Inquiry"])
            
            description = st.text_area("Issue Description", placeholder="Describe the issue in detail...")
            
            if st.button("🎫 Create Support Ticket", use_container_width=True):
                if ticket_title and customer_email and description:
                    # Simulate ticket creation
                    ticket_id = f"SAV-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
                    
                    st.success(f"✅ Support ticket {ticket_id} created successfully!")
                    
                    # Show ticket details
                    st.markdown(f"""
                    **Ticket Details:**
                    - **Ticket ID:** {ticket_id}
                    - **Title:** {ticket_title}
                    - **Priority:** {priority}
                    - **Customer:** {customer_email}
                    - **Estimated Response:** 24 hours
                    """)
                else:
                    st.warning("⚠️ Please fill in all required fields")
            
            # Recent tickets display
            st.markdown("### Recent Support Tickets")
            
            recent_tickets = pd.DataFrame({
                'Ticket ID': ['SAV-20241215-143022', 'SAV-20241215-142011', 'SAV-20241215-141005'],
                'Title': ['Invoice not received', 'Payment processing error', 'Customer data update'],
                'Customer': ['customer1@company.com', 'customer2@company.com', 'customer3@company.com'],
                'Priority': ['High', 'Normal', 'Low'],
                'Status': ['Open', 'In Progress', 'Resolved'],
                'Created': ['2024-12-15 14:30', '2024-12-15 14:20', '2024-12-15 14:10']
            })
            
            # Format status column
            def format_ticket_status(status):
                status_colors = {
                    'Open': '🔴',
                    'In Progress': '🟡',
                    'Resolved': '🟢'
                }
                return f"{status_colors.get(status, '⚪')} {status}"
            
            recent_tickets['Status'] = recent_tickets['Status'].apply(format_ticket_status)
            
            st.dataframe(recent_tickets, use_container_width=True, hide_index=True)
    
    # ========================================================================
    # TAB 3: ANALYTICS - BUSINESS QUERY CATEGORIES
    # ========================================================================
    
    with main_tabs[2]:
        st.markdown("""
        <div class="section-header">
            <div class="section-title">Business Intelligence Analytics</div>
            <div class="section-subtitle">Advanced analytics and reporting for business insights</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Create tabs for query categories
        tabs = st.tabs(list(BUSINESS_QUERIES.keys()))
        
        for i, (category, queries) in enumerate(BUSINESS_QUERIES.items()):
            with tabs[i]:
                cols = st.columns(3)
                for j, (query_name, query_info) in enumerate(queries.items()):
                    with cols[j % 3]:
                        if st.button(query_name, key=f"query_{i}_{j}", use_container_width=True):
                            st.session_state.current_query = query_info['query']
                            st.success(f"Query loaded: {query_name}")
                            time.sleep(1)
                            st.rerun()
                        
                        st.caption(query_info['description'])
        
        # Custom Query Interface
        st.markdown("""
        <div class="section-header">
            <div class="section-title">Custom Query Builder</div>
            <div class="section-subtitle">Natural language queries for ad-hoc analysis</div>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns([4, 1])
        
        with col1:
            custom_query = st.text_input(
                "Business Query",
                value=st.session_state.current_query,
                placeholder="Enter your business question (e.g., 'Show customers with revenue over $50,000 this quarter')",
                key="custom_query"
            )
        
        with col2:
            if st.button("Execute Query", use_container_width=True):
                if custom_query.strip():
                    st.session_state.current_query = custom_query
                    st.rerun()
                else:
                    st.error("Please enter a query")
        
        # Results Display
        if st.session_state.current_query:
            st.markdown("""
            <div class="data-container">
                <div class="data-container-header">
                    <div class="data-container-title">Query Results</div>
                    <div class="data-container-actions">
            """, unsafe_allow_html=True)
            
            # View toggle
            view_mode = create_professional_view_toggle()
            st.session_state.current_view = view_mode
            
            st.markdown("</div></div>", unsafe_allow_html=True)
            
            # Execute query
            with st.spinner("Executing query..."):
                try:
                    result = execute_sap_query(st.session_state.current_query)
                    
                    if 'error' in result and result['error']:
                        st.markdown(f'<div class="alert alert-error">Query Error: {result["error"].get("message", "Unknown error")}</div>', unsafe_allow_html=True)
                    
                    elif 'response' in result and result['response']:
                        response_data = result['response']
                        
                        if isinstance(response_data, dict) and 'value' in response_data:
                            data_list = response_data['value']
                        elif isinstance(response_data, list):
                            data_list = response_data
                        else:
                            data_list = [response_data] if response_data else []
                        
                        if data_list:
                            # Convert to DataFrame
                            df = pd.DataFrame(data_list)
                            
                            # Apply row limit
                            max_rows = 100  # Default limit
                            if len(df) > max_rows:
                                df_display = df.head(max_rows)
                                st.info(f"Displaying first {max_rows} of {len(df)} records")
                            else:
                                df_display = df
                            
                            # Store in session state
                            st.session_state.selected_data = df_display
                            
                            # Business alerts
                            styled_df, alerts = apply_business_alerts_professional(df_display)
                            if alerts:
                                show_professional_alerts(alerts)
                            
                            # Display based on view mode
                            if view_mode == "table":
                                # Format status columns
                                if 'Status' in df_display.columns:
                                    df_display_formatted = df_display.copy()
                                    df_display_formatted['Status'] = df_display_formatted['Status'].apply(
                                        lambda x: format_status_indicator(x)
                                    )
                                else:
                                    df_display_formatted = df_display
                                
                                st.dataframe(
                                    df_display_formatted,
                                    use_container_width=True,
                                    height=500
                                )
                                
                                # Summary statistics
                                numeric_cols = df_display.select_dtypes(include=['number']).columns
                                if len(numeric_cols) > 0:
                                    st.markdown("**Summary Statistics**")
                                    
                                    summary_data = []
                                    for col in numeric_cols:
                                        summary_data.append({
                                            'Metric': col,
                                            'Total': f"{df_display[col].sum():,.2f}",
                                            'Average': f"{df_display[col].mean():,.2f}",
                                            'Count': len(df_display)
                                        })
                                    
                                    summary_df = pd.DataFrame(summary_data)
                                    st.dataframe(summary_df, use_container_width=True)
                            
                            elif view_mode == "json":
                                # Display JSON with proper formatting
                                st.json(data_list[:50])  # Limit for performance
                                if len(data_list) > 50:
                                    st.info("Showing first 50 records in JSON format")
                            
                            elif view_mode == "chart":
                                # Chart configuration
                                col1, col2, col3 = st.columns(3)
                                
                                with col1:
                                    chart_type = st.selectbox("Chart Type", ["bar", "line", "area", "pie"])
                                
                                with col2:
                                    x_col = st.selectbox("X-Axis", df_display.columns)
                                
                                with col3:
                                    numeric_cols = df_display.select_dtypes(include=['number']).columns
                                    if len(numeric_cols) > 0:
                                        y_col = st.selectbox("Y-Axis", numeric_cols)
                                    else:
                                        st.warning("No numeric columns available for charting")
                                        y_col = None
                                
                                if y_col:
                                    # Create and display chart
                                    fig = create_enterprise_chart(
                                        df_display.head(100),  # Limit for performance
                                        chart_type, x_col, y_col,
                                        f"{chart_type.title()} Chart: {y_col} by {x_col}"
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                        
                        else:
                            st.markdown('<div class="alert alert-info">Query executed successfully but returned no data</div>', unsafe_allow_html=True)
                    
                    # Technical details
                    with st.expander("Technical Details"):
                        if 'odata_url' in result:
                            st.code(result['odata_url'], language="text")
                        if 'intent' in result:
                            st.text(f"Query Intent: {result['intent']}")
                        st.text(f"Execution Time: {datetime.now().strftime('%H:%M:%S')}")
                        st.text(f"Records Returned: {len(df_display) if 'df_display' in locals() else 0}")
                
                except Exception as e:
                    st.markdown(f'<div class="alert alert-error">Application Error: {str(e)}</div>', unsafe_allow_html=True)
            
            st.markdown("</div>", unsafe_allow_html=True)  # Close data container
    
    # ========================================================================
    # TAB 4: ADMINISTRATION
    # ========================================================================
    
    with main_tabs[3]:
        st.markdown("""
        <div class="section-header">
            <div class="section-title">System Administration</div>
            <div class="section-subtitle">System configuration, monitoring, and maintenance tools</div>
        </div>
        """, unsafe_allow_html=True)
        
        admin_tabs = st.tabs(["🔧 System Status", "📊 Performance Metrics", "⚙️ Configuration", "📋 Audit Logs"])
        
        with admin_tabs[0]:
            st.markdown("### System Health Monitor")
            
            # System status indicators
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("SAP B1 Connection", "✅ Connected", "99.9% uptime")
            
            with col2:
                st.metric("Gmail Integration", "✅ Active", "12 msgs processed")
            
            with col3:
                st.metric("Crystal Reports", "✅ Operational", "45 reports generated")
            
            with col4:
                st.metric("Database Status", "✅ Healthy", "2.3ms avg response")
        
        with admin_tabs[1]:
            st.markdown("### Performance Analytics")
            
            # Performance charts would go here
            st.info("Performance monitoring dashboard - Integration with system metrics")
        
        with admin_tabs[2]:
            st.markdown("### System Configuration")
            
            # Configuration settings
            st.info("System configuration panel - Email settings, SAP connections, report templates")
        
        with admin_tabs[3]:
            st.markdown("### Audit Trail")
            
            # Audit logs
            st.info("Audit log viewer - User activities, system changes, security events")
    
    # ========================================================================
    # PROFESSIONAL SIDEBAR
    # ========================================================================
    
    with st.sidebar:
        st.markdown("""
        <div class="sidebar-section">
            <div class="sidebar-section-header">System Information</div>
            <div class="sidebar-section-content">
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="metric-row">
            <span class="metric-label">Server Status</span>
            <span class="metric-value">🟢 Online</span>
        </div>
        <div class="metric-row">
            <span class="metric-label">Last Sync</span>
            <span class="metric-value">2 min ago</span>
        </div>
        <div class="metric-row">
            <span class="metric-label">Active Users</span>
            <span class="metric-value">12</span>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("</div></div>", unsafe_allow_html=True)
        
        # Quick filters
        st.markdown("""
        <div class="sidebar-section">
            <div class="sidebar-section-header">Quick Filters</div>
            <div class="sidebar-section-content">
        """, unsafe_allow_html=True)
        
        # Date Range
        date_range = st.selectbox(
            "Period",
            ["Current Month", "Last Month", "Current Quarter", "Last Quarter", "Current Year", "Custom Range"],
            key="date_filter"
        )
        
        if date_range == "Custom Range":
            start_date = st.date_input("From Date", datetime.now() - timedelta(days=30))
            end_date = st.date_input("To Date", datetime.now())
        
        # Entity Filters
        entity_type = st.selectbox(
            "Entity Type",
            ["All", "Customers", "Vendors", "Items"],
            key="entity_filter"
        )
        
        # Status Filter
        status_filter = st.selectbox(
            "Status",
            ["All", "Active", "Inactive", "Pending"],
            key="status_filter"
        )
        
        st.markdown("</div></div>", unsafe_allow_html=True)
        
        # Export Options
        if st.session_state.selected_data is not None:
            st.markdown("""
            <div class="sidebar-section">
                <div class="sidebar-section-header">Export Data</div>
                <div class="sidebar-section-content">
            """, unsafe_allow_html=True)
            
            df = st.session_state.selected_data
            
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"sap_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
            
            excel_buffer = BytesIO()
            df.to_excel(excel_buffer, index=False, engine='openpyxl')
            excel_buffer.seek(0)
            
            st.download_button(
                label="📊 Download Excel",
                data=excel_buffer.getvalue(),
                file_name=f"sap_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
            
            st.markdown("</div></div>", unsafe_allow_html=True)
    
    # ========================================================================
    # PROFESSIONAL FOOTER
    # ========================================================================
    
    st.markdown("---")
    st.markdown(f"""
    <div style="text-align: center; color: #666666; padding: 1rem; font-size: 0.875rem;">
        <strong>SAP Business One - Business Intelligence Platform</strong><br>
        Version 2.0 | Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Status: Operational | Invoice Management: Active
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()