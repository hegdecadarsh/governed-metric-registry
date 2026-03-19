import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
from datetime import datetime

st.set_page_config(page_title="Metric Registry", page_icon=":material/analytics:", layout="wide")

session = get_active_session()

DATABASE = "METRIC_REGISTRY_DB"
SCHEMA = "REGISTRY"
FULL_PATH = f"{DATABASE}.{SCHEMA}"

st.markdown("""
<style>
    .main-header { font-size: 2.2rem; font-weight: 700; color: #1E3A5F; }
    .ai-response { background: linear-gradient(135deg, #f0f4ff 0%, #e8f0fe 100%); padding: 1rem; border-radius: 8px; border-left: 4px solid #4285f4; }
    .risk-high { color: #dc3545; font-weight: bold; }
    .risk-medium { color: #ffc107; font-weight: bold; }
    .risk-low { color: #28a745; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=60)
def get_metrics():
    try:
        return session.sql(f"SELECT * FROM {FULL_PATH}.METRIC_REGISTRY ORDER BY CREATED_AT DESC").to_pandas()
    except:
        return None

@st.cache_data(ttl=300)
def get_pending_approvals():
    try:
        return session.sql(f"""
            SELECT * FROM {FULL_PATH}.METRIC_REGISTRY 
            WHERE STATUS = 'PENDING_APPROVAL' 
            ORDER BY CREATED_AT DESC
        """).to_pandas()
    except:
        return None

def parse_json_response(text):
    """Extract and parse JSON from AI response, handling markdown code blocks"""
    if not text or text.strip() == '':
        return None
    
    import re
    json_match = re.search(r'```(?:json)?\s*([\s\S]*?)```', text)
    if json_match:
        text = json_match.group(1).strip()
    
    text = text.strip()
    if text.startswith('```'):
        text = text[3:]
    if text.endswith('```'):
        text = text[:-3]
    text = text.strip()
    
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find('{')
        end = text.rfind('}') + 1
        if start != -1 and end > start:
            try:
                return json.loads(text[start:end])
            except:
                pass
        return None

def ai_suggest_sql_format(metric_sql):
    """Suggest SQL modifications to use standard METRIC_DATE and METRIC_VALUE columns"""
    prompt = f"""You are a SQL expert. The user has provided a metric SQL query. 
For the metric chat app to work correctly, the query MUST output:
- A date column aliased as METRIC_DATE
- A value column aliased as METRIC_VALUE
- Optional dimension columns (like COUNTRY, PRODUCT_CATEGORY) with uppercase names

Analyze this SQL and return a corrected version:
{metric_sql}

Return a JSON object:
{{
  "needs_changes": true or false,
  "issues": ["list of issues found"],
  "suggested_sql": "the corrected SQL with proper column aliases"
}}

IMPORTANT: Return ONLY the JSON object, no explanation."""

    try:
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-70b',
                '{prompt.replace("'", "''")}'
            ) as response
        """).collect()[0]['RESPONSE']
        
        parsed = parse_json_response(result)
        if parsed:
            return parsed
        return {"error": "Could not parse AI response"}
    except Exception as e:
        return {"error": str(e)}

def ai_analyze_metric(metric_name, metric_sql, metric_description):
    prompt = f"""You are a data governance expert. Analyze this metric definition and return a JSON object.

Metric Name: {metric_name}
SQL Definition: {metric_sql}
Description: {metric_description}

Return a JSON object with exactly these fields:
{{
  "enhanced_description": "A clear, business-friendly description (2-3 sentences)",
  "logic_validation": "Assessment of the SQL logic",
  "suggested_dimensions": ["dimension1", "dimension2", "dimension3"],
  "suggested_measures": ["measure1", "measure2"],
  "potential_risks": ["risk1", "risk2"],
  "risk_level": "LOW or MEDIUM or HIGH",
  "recommendations": ["recommendation1", "recommendation2"]
}}

IMPORTANT: Return ONLY the JSON object, no explanation, no markdown."""

    try:
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-70b',
                '{prompt.replace("'", "''")}'
            ) as response
        """).collect()[0]['RESPONSE']
        
        if not result or result.strip() == '':
            return {"error": "Empty response from AI model"}
        
        parsed = parse_json_response(result)
        if parsed:
            return parsed
        
        return {
            "enhanced_description": "AI analysis completed. Please review the metric manually.",
            "logic_validation": "Unable to parse - manual review recommended",
            "suggested_dimensions": ["time_period", "category", "region"],
            "suggested_measures": ["count", "sum", "average"],
            "potential_risks": ["Data quality should be verified", "Business logic should be confirmed"],
            "risk_level": "MEDIUM",
            "recommendations": ["Review SQL logic manually", "Validate with business stakeholders"],
            "raw_response": result[:500]
        }
    except Exception as e:
        return {"error": str(e)}

def ai_explain_metric(metric_name, metric_sql):
    prompt = f"""Explain this metric in simple business terms that a non-technical stakeholder can understand:

Metric Name: {metric_name}
SQL: {metric_sql}

Provide:
1. What this metric measures in plain English
2. How it's calculated (simplified)
3. When to use this metric
4. Example interpretation

Keep the explanation concise and jargon-free."""

    try:
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-70b',
                '{prompt.replace("'", "''")}'
            ) as response
        """).collect()[0]['RESPONSE']
        return result
    except Exception as e:
        return f"Error: {str(e)}"

def ai_approval_recommendation(metric_name, metric_sql, description, risk_level, potential_risks):
    prompt = f"""You are a data governance reviewer. Analyze this metric and return a JSON recommendation.

Metric Name: {metric_name}
SQL Definition: {metric_sql}
Description: {description}
Risk Level: {risk_level}
Potential Risks: {potential_risks}

Return a JSON object with exactly these fields:
{{
  "recommendation": "APPROVE or REJECT or NEEDS_CHANGES",
  "confidence": 85,
  "reasoning": "Brief explanation (2-3 sentences)",
  "concerns": ["concern1", "concern2"],
  "suggested_changes": ["change1", "change2"],
  "compliance_check": "Brief compliance assessment"
}}

IMPORTANT: Return ONLY the JSON object, no explanation, no markdown."""

    try:
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-70b',
                '{prompt.replace("'", "''")}'
            ) as response
        """).collect()[0]['RESPONSE']
        
        if not result or result.strip() == '':
            return {"error": "Empty response from AI model"}
        
        parsed = parse_json_response(result)
        if parsed:
            return parsed
        
        return {
            "recommendation": "NEEDS_CHANGES",
            "confidence": 50,
            "reasoning": "AI response could not be parsed. Manual review recommended.",
            "concerns": ["AI analysis incomplete"],
            "suggested_changes": ["Request manual review from data governance team"],
            "compliance_check": "Manual verification required",
            "raw_response": result[:500]
        }
    except Exception as e:
        return {"error": str(e)}

def safe_get(row, column, default='N/A'):
    """Safely get value from dataframe row, handling None/NaN/bool"""
    try:
        if row is None or isinstance(row, bool):
            return default
        if hasattr(row, 'get') and callable(row.get):
            val = row.get(column, default)
        elif hasattr(row, '__getitem__'):
            val = row[column]
        else:
            return default
        if val is None:
            return default
        if isinstance(val, float) and (str(val) == 'nan' or val != val):
            return default
        return val
    except:
        return default

def clear_cache():
    get_metrics.clear()
    get_pending_approvals.clear()

with st.sidebar:
    st.markdown("### :material/analytics: Metric Registry")
    st.markdown("AI-Powered Metric Governance")
    st.markdown("---")
    
    st.markdown("#### :material/smart_toy: AI Features")
    st.markdown("""
    - Auto-generate descriptions
    - Validate metric logic
    - Suggest dimensions
    - Risk assessment
    - Plain English explanations
    """)
    
    st.markdown("---")
    if st.button(":material/refresh: Refresh Data", use_container_width=True):
        clear_cache()
        st.rerun()

st.markdown('<p class="main-header">:material/analytics: Metric Registry with AI Governance</p>', unsafe_allow_html=True)
st.caption("Enterprise Metric Management with Cortex AI-Powered Validation")
st.markdown("---")

tab1, tab2, tab3, tab4, tab5 = st.tabs([":material/list: Registry", ":material/add_circle: Define Metric", ":material/edit: Edit Metric", ":material/check_circle: Approvals", ":material/search: Explore"])

with tab1:
    st.subheader("Metric Registry")
    
    metrics_df = get_metrics()
    
    if metrics_df is not None and not metrics_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Metrics", len(metrics_df))
        with col2:
            approved = len(metrics_df[metrics_df['STATUS'] == 'APPROVED'])
            st.metric("Approved", approved)
        with col3:
            pending = len(metrics_df[metrics_df['STATUS'] == 'PENDING_APPROVAL'])
            st.metric("Pending", pending)
        with col4:
            rejected = len(metrics_df[metrics_df['STATUS'] == 'REJECTED'])
            st.metric("Rejected", rejected)
        
        st.markdown("---")
        
        status_filter = st.selectbox("Filter by Status", ["All", "APPROVED", "PENDING_APPROVAL", "REJECTED", "DRAFT"])
        
        filtered_df = metrics_df if status_filter == "All" else metrics_df[metrics_df['STATUS'] == status_filter]
        
        if not filtered_df.empty:
            display_cols = ['METRIC_NAME', 'DOMAIN', 'STATUS', 'RISK_LEVEL', 'AI_DESCRIPTION', 'SUGGESTED_DIMENSIONS', 'SUGGESTED_MEASURES', 'POTENTIAL_RISKS', 'CREATED_BY', 'CREATED_AT']
            available_cols = [c for c in display_cols if c in filtered_df.columns]
            st.dataframe(filtered_df[available_cols], use_container_width=True, hide_index=True)
        else:
            st.info("No metrics found with selected filter.")
    else:
        st.info(":material/info: No metrics in registry yet. Create your first metric!")

with tab2:
    st.subheader("Define New Metric")
    st.markdown("Create a metric and let AI validate and enhance it.")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("#### Step 1: Basic Information")
        metric_name = st.text_input("Metric Name *", placeholder="e.g., monthly_active_users", key="def_metric_name")
        domain = st.selectbox("Domain *", ["Sales", "Marketing", "Finance", "Operations", "Customer", "Product", "HR"], key="def_domain")
        metric_sql = st.text_area(
            "Metric SQL Definition *",
            height=150,
            placeholder="""SELECT 
    order_date AS METRIC_DATE,
    country AS COUNTRY,
    SUM(amount) AS METRIC_VALUE
FROM orders
GROUP BY 1, 2""",
            key="def_metric_sql"
        )
        
        st.info(":material/info: **Required columns:** Your SQL must output `METRIC_DATE` (date) and `METRIC_VALUE` (numeric). Dimension columns should be UPPERCASE.")
        
        if metric_sql and st.button(":material/auto_fix_high: Check & Suggest SQL Format", key="check_sql_btn"):
            with st.spinner("Checking SQL format..."):
                suggestion = ai_suggest_sql_format(metric_sql)
            
            if "error" not in suggestion:
                if suggestion.get("needs_changes", False):
                    st.warning(":material/warning: SQL needs modifications for chat app compatibility")
                    if suggestion.get("issues"):
                        st.markdown("**Issues found:**")
                        for issue in suggestion.get("issues", []):
                            st.markdown(f"- {issue}")
                    if suggestion.get("suggested_sql"):
                        st.markdown("**Suggested SQL:**")
                        st.code(suggestion.get("suggested_sql"), language="sql")
                        if st.button(":material/content_copy: Use Suggested SQL", key="use_suggested_sql"):
                            st.session_state["def_metric_sql"] = suggestion.get("suggested_sql")
                            st.rerun()
                else:
                    st.success(":material/check_circle: SQL format looks good!")
            else:
                st.error(f"Error: {suggestion.get('error')}")
        description = st.text_area(
            "Initial Description",
            placeholder="Brief description of what this metric measures...",
            height=80,
            key="def_description"
        )
        owner_email = st.text_input("Owner Email *", placeholder="owner@company.com", key="def_owner_email")
        
        st.markdown("---")
        if st.button(":material/smart_toy: Analyze with AI", use_container_width=True, disabled=not(metric_name and metric_sql)):
            with st.spinner(":material/smart_toy: AI is analyzing your metric..."):
                analysis = ai_analyze_metric(metric_name, metric_sql, description)
            
            if "error" not in analysis:
                dims = analysis.get('suggested_dimensions', [])
                measures = analysis.get('suggested_measures', [])
                risks = analysis.get('potential_risks', [])
                risk = analysis.get('risk_level', 'LOW')
                recs = analysis.get('recommendations', [])
                
                st.session_state['define_ai_description'] = analysis.get('enhanced_description', '')
                st.session_state['define_dimensions'] = ', '.join(dims) if isinstance(dims, list) else str(dims)
                st.session_state['define_measures'] = ', '.join(measures) if isinstance(measures, list) else str(measures)
                st.session_state['define_risks'] = ', '.join(risks) if isinstance(risks, list) else str(risks)
                st.session_state['define_risk_level'] = risk if risk in ['LOW', 'MEDIUM', 'HIGH'] else 'LOW'
                st.session_state['define_recommendations'] = recs
                st.session_state['ai_analyzed'] = True
                st.rerun()
            else:
                st.error(f"AI Analysis failed: {analysis.get('error')}")
        
        if st.session_state.get('ai_analyzed', False):
            st.success(":material/check: AI analysis complete! Review and edit the fields below, then submit.")
            
            st.markdown("---")
            st.markdown("#### Step 2: Review AI Suggestions")
            st.caption("Edit these fields as needed before submitting.")
            
            ai_description = st.text_area(
                "AI Description",
                value=st.session_state.get('define_ai_description', ''),
                placeholder="AI-enhanced business description...",
                height=80,
                key="def_ai_desc"
            )
            
            col_dim, col_meas = st.columns(2)
            with col_dim:
                suggested_dimensions = st.text_area(
                    "Suggested Dimensions",
                    value=st.session_state.get('define_dimensions', ''),
                    placeholder="e.g., time_period, region, category (comma-separated)",
                    height=80,
                    key="def_dims"
                )
            with col_meas:
                suggested_measures = st.text_area(
                    "Suggested Measures",
                    value=st.session_state.get('define_measures', ''),
                    placeholder="e.g., count, sum, average (comma-separated)",
                    height=80,
                    key="def_meas"
                )
            
            potential_risks = st.text_area(
                "Potential Risks",
                value=st.session_state.get('define_risks', ''),
                placeholder="e.g., data quality issues, calculation complexity (comma-separated)",
                height=60,
                key="def_risks"
            )
            
            risk_options = ["LOW", "MEDIUM", "HIGH"]
            current_risk = st.session_state.get('define_risk_level', 'LOW')
            risk_idx = risk_options.index(current_risk) if current_risk in risk_options else 0
            risk_level = st.selectbox(
                "Risk Level",
                options=risk_options,
                index=risk_idx,
                key="def_risk_level"
            )
            
            recs = st.session_state.get('define_recommendations', [])
            if recs:
                st.markdown("**AI Recommendations:**")
                for rec in recs:
                    st.info(rec)
            
            st.markdown("---")
            st.markdown("#### Step 3: Submit")
            col_a, col_b = st.columns(2)
            with col_a:
                if st.button(":material/send: Submit for Approval", type="primary", use_container_width=True):
                    try:
                        dims_list = [d.strip() for d in suggested_dimensions.split(',') if d.strip()] if suggested_dimensions else []
                        meas_list = [m.strip() for m in suggested_measures.split(',') if m.strip()] if suggested_measures else []
                        risks_list = [r.strip() for r in potential_risks.split(',') if r.strip()] if potential_risks else []
                        
                        insert_sql = f"""
                            INSERT INTO {FULL_PATH}.METRIC_REGISTRY (
                                METRIC_NAME, DOMAIN, METRIC_SQL, DESCRIPTION,
                                AI_DESCRIPTION, SUGGESTED_DIMENSIONS, SUGGESTED_MEASURES,
                                POTENTIAL_RISKS, RISK_LEVEL, AI_RECOMMENDATIONS,
                                STATUS, CREATED_BY, OWNER_EMAIL
                            ) VALUES (
                                '{metric_name}',
                                '{domain}',
                                '{metric_sql.replace("'", "''")}',
                                '{description.replace("'", "''")}',
                                '{ai_description.replace("'", "''")}',
                                '{json.dumps(dims_list).replace("'", "''")}',
                                '{json.dumps(meas_list).replace("'", "''")}',
                                '{json.dumps(risks_list).replace("'", "''")}',
                                '{risk_level}',
                                '{json.dumps(recs).replace("'", "''")}',
                                'PENDING_APPROVAL',
                                CURRENT_USER(),
                                '{owner_email}'
                            )
                        """
                        session.sql(insert_sql).collect()
                        clear_cache()
                        for key in ['define_ai_description', 'define_dimensions', 'define_measures', 'define_risks', 'define_risk_level', 'define_recommendations', 'ai_analyzed']:
                            if key in st.session_state:
                                del st.session_state[key]
                        st.success(f":material/check: Metric '{metric_name}' submitted for approval!")
                        st.balloons()
                    except Exception as e:
                        st.error(f"Error: {e}")
            with col_b:
                if st.button(":material/save: Save as Draft", use_container_width=True):
                    try:
                        dims_list = [d.strip() for d in suggested_dimensions.split(',') if d.strip()] if suggested_dimensions else []
                        meas_list = [m.strip() for m in suggested_measures.split(',') if m.strip()] if suggested_measures else []
                        risks_list = [r.strip() for r in potential_risks.split(',') if r.strip()] if potential_risks else []
                        
                        insert_sql = f"""
                            INSERT INTO {FULL_PATH}.METRIC_REGISTRY (
                                METRIC_NAME, DOMAIN, METRIC_SQL, DESCRIPTION,
                                AI_DESCRIPTION, SUGGESTED_DIMENSIONS, SUGGESTED_MEASURES,
                                POTENTIAL_RISKS, RISK_LEVEL,
                                STATUS, CREATED_BY, OWNER_EMAIL
                            ) VALUES (
                                '{metric_name}',
                                '{domain}',
                                '{metric_sql.replace("'", "''")}',
                                '{description.replace("'", "''")}',
                                '{ai_description.replace("'", "''")}',
                                '{json.dumps(dims_list).replace("'", "''")}',
                                '{json.dumps(meas_list).replace("'", "''")}',
                                '{json.dumps(risks_list).replace("'", "''")}',
                                '{risk_level}',
                                'DRAFT',
                                CURRENT_USER(),
                                '{owner_email}'
                            )
                        """
                        session.sql(insert_sql).collect()
                        clear_cache()
                        for key in ['define_ai_description', 'define_dimensions', 'define_measures', 'define_risks', 'define_risk_level', 'define_recommendations', 'ai_analyzed']:
                            if key in st.session_state:
                                del st.session_state[key]
                        st.success(f":material/check: Metric '{metric_name}' saved as draft!")
                    except Exception as e:
                        st.error(f"Error: {e}")
    
    with col2:
        st.markdown("#### :material/lightbulb: Tips")
        st.info("""
        **Naming Convention:**
        - Use snake_case
        - Be descriptive
        - Include time grain if applicable
        
        **SQL Best Practices:**
        - Use clear column aliases
        - Include date dimensions
        - Document any filters
        """)

with tab3:
    st.subheader("Edit Metric")
    st.markdown("Modify existing metrics (DRAFT, REJECTED, or APPROVED status).")
    
    metrics_df = get_metrics()
    
    if metrics_df is not None and not metrics_df.empty:
        editable_df = metrics_df[metrics_df['STATUS'].isin(['DRAFT', 'REJECTED', 'APPROVED'])]
        
        if not editable_df.empty:
            metric_options = editable_df['METRIC_NAME'].tolist()
            selected_metric = st.selectbox("Select Metric to Edit", options=[""] + metric_options)
            
            if selected_metric:
                metric_row = editable_df[editable_df['METRIC_NAME'] == selected_metric].iloc[0]
                metric_id = safe_get(metric_row, 'METRIC_ID', '')
                current_status = safe_get(metric_row, 'STATUS', 'DRAFT')
                
                st.markdown("---")
                
                if safe_get(metric_row, 'APPROVAL_COMMENT', ''):
                    st.warning(f":material/feedback: **Previous Feedback:** {safe_get(metric_row, 'APPROVAL_COMMENT', '')}")
                
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.markdown("#### Step 1: Basic Information")
                    edit_metric_name = st.text_input(
                        "Metric Name *", 
                        value=safe_get(metric_row, 'METRIC_NAME', ''),
                        key="edit_metric_name"
                    )
                    
                    domains = ["Sales", "Marketing", "Finance", "Operations", "Customer", "Product", "HR"]
                    current_domain = safe_get(metric_row, 'DOMAIN', 'Sales')
                    domain_idx = domains.index(current_domain) if current_domain in domains else 0
                    edit_domain = st.selectbox("Domain *", domains, index=domain_idx, key="edit_domain")
                    
                    edit_metric_sql = st.text_area(
                        "Metric SQL Definition *",
                        value=safe_get(metric_row, 'METRIC_SQL', ''),
                        height=150,
                        key="edit_metric_sql"
                    )
                    
                    edit_description = st.text_area(
                        "Description",
                        value=safe_get(metric_row, 'DESCRIPTION', ''),
                        height=80,
                        key="edit_description"
                    )
                    
                    edit_owner_email = st.text_input(
                        "Owner Email *",
                        value=safe_get(metric_row, 'OWNER_EMAIL', ''),
                        key="edit_owner_email"
                    )
                    
                    st.markdown("---")
                    if st.button(":material/smart_toy: Analyze with AI", use_container_width=True, disabled=not(edit_metric_name and edit_metric_sql), key="edit_ai_btn"):
                        with st.spinner(":material/smart_toy: AI is analyzing your metric..."):
                            analysis = ai_analyze_metric(edit_metric_name, edit_metric_sql, edit_description)
                        
                        if "error" not in analysis:
                            dims = analysis.get('suggested_dimensions', [])
                            measures = analysis.get('suggested_measures', [])
                            risks = analysis.get('potential_risks', [])
                            risk = analysis.get('risk_level', 'LOW')
                            recs = analysis.get('recommendations', [])
                            
                            st.session_state['edit_ai_desc'] = analysis.get('enhanced_description', '')
                            st.session_state['edit_dims'] = ', '.join(dims) if isinstance(dims, list) else str(dims)
                            st.session_state['edit_meas'] = ', '.join(measures) if isinstance(measures, list) else str(measures)
                            st.session_state['edit_risks_field'] = ', '.join(risks) if isinstance(risks, list) else str(risks)
                            st.session_state['edit_risk_level_select'] = risk if risk in ['LOW', 'MEDIUM', 'HIGH'] else 'LOW'
                            st.session_state['edit_recommendations'] = recs
                            st.session_state['edit_ai_analyzed'] = True
                            st.rerun()
                        else:
                            st.error(f"AI Analysis failed: {analysis.get('error')}")
                    
                    existing_ai_desc = safe_get(metric_row, 'AI_DESCRIPTION', '')
                    existing_dims = safe_get(metric_row, 'SUGGESTED_DIMENSIONS', '[]')
                    existing_meas = safe_get(metric_row, 'SUGGESTED_MEASURES', '[]')
                    existing_risks = safe_get(metric_row, 'POTENTIAL_RISKS', '[]')
                    existing_risk_level = safe_get(metric_row, 'RISK_LEVEL', 'LOW')
                    
                    try:
                        dims_parsed = json.loads(existing_dims) if isinstance(existing_dims, str) else existing_dims
                        dims_str = ', '.join(dims_parsed) if isinstance(dims_parsed, list) else str(existing_dims)
                    except:
                        dims_str = str(existing_dims) if existing_dims else ''
                    
                    try:
                        meas_parsed = json.loads(existing_meas) if isinstance(existing_meas, str) else existing_meas
                        meas_str = ', '.join(meas_parsed) if isinstance(meas_parsed, list) else str(existing_meas)
                    except:
                        meas_str = str(existing_meas) if existing_meas else ''
                    
                    try:
                        risks_parsed = json.loads(existing_risks) if isinstance(existing_risks, str) else existing_risks
                        risks_str = ', '.join(risks_parsed) if isinstance(risks_parsed, list) else str(existing_risks)
                    except:
                        risks_str = str(existing_risks) if existing_risks else ''
                    
                    if 'edit_ai_desc' not in st.session_state:
                        st.session_state['edit_ai_desc'] = existing_ai_desc or ''
                    if 'edit_dims' not in st.session_state:
                        st.session_state['edit_dims'] = dims_str or ''
                    if 'edit_meas' not in st.session_state:
                        st.session_state['edit_meas'] = meas_str or ''
                    if 'edit_risks_field' not in st.session_state:
                        st.session_state['edit_risks_field'] = risks_str or ''
                    if 'edit_risk_level_select' not in st.session_state:
                        risk_options = ["LOW", "MEDIUM", "HIGH"]
                        st.session_state['edit_risk_level_select'] = existing_risk_level if existing_risk_level in risk_options else 'LOW'
                    
                    if st.session_state.get('edit_ai_analyzed', False):
                        st.success(":material/check: AI analysis complete! Review and edit the fields below.")
                    
                    st.markdown("---")
                    st.markdown("#### Step 2: AI-Suggested Fields")
                    st.caption("Edit these fields as needed before submitting.")
                    
                    edit_ai_description = st.text_area(
                        "AI Description",
                        placeholder="AI-enhanced business description...",
                        height=80,
                        key="edit_ai_desc"
                    )
                    
                    col_dim, col_meas = st.columns(2)
                    with col_dim:
                        edit_suggested_dimensions = st.text_area(
                            "Suggested Dimensions",
                            placeholder="e.g., time_period, region (comma-separated)",
                            height=80,
                            key="edit_dims"
                        )
                    with col_meas:
                        edit_suggested_measures = st.text_area(
                            "Suggested Measures",
                            placeholder="e.g., count, sum (comma-separated)",
                            height=80,
                            key="edit_meas"
                        )
                    
                    edit_potential_risks = st.text_area(
                        "Potential Risks",
                        placeholder="e.g., data quality issues (comma-separated)",
                        height=60,
                        key="edit_risks_field"
                    )
                    
                    risk_options = ["LOW", "MEDIUM", "HIGH"]
                    edit_risk_level = st.selectbox(
                        "Risk Level",
                        options=risk_options,
                        key="edit_risk_level_select"
                    )
                    
                    recs = st.session_state.get('edit_recommendations', [])
                    if recs:
                        st.markdown("**AI Recommendations:**")
                        for rec in recs:
                            st.info(rec)
                    
                    st.markdown("---")
                    st.markdown("#### Step 3: Save / Submit")
                    
                    if current_status == 'APPROVED':
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            if st.button(":material/undo: Unapprove (Back to Draft)", use_container_width=True, type="secondary", key="unapprove_btn"):
                                try:
                                    dims_list = [d.strip() for d in edit_suggested_dimensions.split(',') if d.strip()] if edit_suggested_dimensions else []
                                    meas_list = [m.strip() for m in edit_suggested_measures.split(',') if m.strip()] if edit_suggested_measures else []
                                    risks_list = [r.strip() for r in edit_potential_risks.split(',') if r.strip()] if edit_potential_risks else []
                                    
                                    update_sql = f"""
                                        UPDATE {FULL_PATH}.METRIC_REGISTRY 
                                        SET METRIC_NAME = '{edit_metric_name}',
                                            DOMAIN = '{edit_domain}',
                                            METRIC_SQL = '{edit_metric_sql.replace("'", "''")}',
                                            DESCRIPTION = '{edit_description.replace("'", "''")}',
                                            AI_DESCRIPTION = '{edit_ai_description.replace("'", "''")}',
                                            SUGGESTED_DIMENSIONS = '{json.dumps(dims_list).replace("'", "''")}',
                                            SUGGESTED_MEASURES = '{json.dumps(meas_list).replace("'", "''")}',
                                            POTENTIAL_RISKS = '{json.dumps(risks_list).replace("'", "''")}',
                                            RISK_LEVEL = '{edit_risk_level}',
                                            OWNER_EMAIL = '{edit_owner_email}',
                                            STATUS = 'DRAFT',
                                            APPROVAL_COMMENT = 'Unapproved for revision'
                                        WHERE METRIC_ID = {metric_id}
                                    """
                                    session.sql(update_sql).collect()
                                    clear_cache()
                                    for key in ['edit_ai_desc', 'edit_dims', 'edit_meas', 'edit_risks_field', 'edit_risk_level_select', 'edit_recommendations', 'edit_ai_analyzed']:
                                        if key in st.session_state:
                                            del st.session_state[key]
                                    st.success(f":material/check: Metric '{edit_metric_name}' unapproved and set to DRAFT!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                        with col_b:
                            if st.button(":material/save: Save Changes", use_container_width=True, key="save_approved_btn"):
                                try:
                                    dims_list = [d.strip() for d in edit_suggested_dimensions.split(',') if d.strip()] if edit_suggested_dimensions else []
                                    meas_list = [m.strip() for m in edit_suggested_measures.split(',') if m.strip()] if edit_suggested_measures else []
                                    risks_list = [r.strip() for r in edit_potential_risks.split(',') if r.strip()] if edit_potential_risks else []
                                    
                                    update_sql = f"""
                                        UPDATE {FULL_PATH}.METRIC_REGISTRY 
                                        SET METRIC_NAME = '{edit_metric_name}',
                                            DOMAIN = '{edit_domain}',
                                            METRIC_SQL = '{edit_metric_sql.replace("'", "''")}',
                                            DESCRIPTION = '{edit_description.replace("'", "''")}',
                                            AI_DESCRIPTION = '{edit_ai_description.replace("'", "''")}',
                                            SUGGESTED_DIMENSIONS = '{json.dumps(dims_list).replace("'", "''")}',
                                            SUGGESTED_MEASURES = '{json.dumps(meas_list).replace("'", "''")}',
                                            POTENTIAL_RISKS = '{json.dumps(risks_list).replace("'", "''")}',
                                            RISK_LEVEL = '{edit_risk_level}',
                                            OWNER_EMAIL = '{edit_owner_email}'
                                        WHERE METRIC_ID = {metric_id}
                                    """
                                    session.sql(update_sql).collect()
                                    clear_cache()
                                    st.success(f":material/check: Metric '{edit_metric_name}' updated!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                        with col_c:
                            if st.button(":material/delete: Delete", use_container_width=True, key="delete_approved_btn"):
                                try:
                                    session.sql(f"DELETE FROM {FULL_PATH}.METRIC_REGISTRY WHERE METRIC_ID = {metric_id}").collect()
                                    clear_cache()
                                    st.success(f":material/check: Metric deleted!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                    else:
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            if st.button(":material/send: Submit for Approval", type="primary", use_container_width=True, key="submit_edit_btn"):
                                try:
                                    dims_list = [d.strip() for d in edit_suggested_dimensions.split(',') if d.strip()] if edit_suggested_dimensions else []
                                    meas_list = [m.strip() for m in edit_suggested_measures.split(',') if m.strip()] if edit_suggested_measures else []
                                    risks_list = [r.strip() for r in edit_potential_risks.split(',') if r.strip()] if edit_potential_risks else []
                                    
                                    update_sql = f"""
                                        UPDATE {FULL_PATH}.METRIC_REGISTRY 
                                        SET METRIC_NAME = '{edit_metric_name}',
                                            DOMAIN = '{edit_domain}',
                                            METRIC_SQL = '{edit_metric_sql.replace("'", "''")}',
                                            DESCRIPTION = '{edit_description.replace("'", "''")}',
                                            AI_DESCRIPTION = '{edit_ai_description.replace("'", "''")}',
                                            SUGGESTED_DIMENSIONS = '{json.dumps(dims_list).replace("'", "''")}',
                                            SUGGESTED_MEASURES = '{json.dumps(meas_list).replace("'", "''")}',
                                            POTENTIAL_RISKS = '{json.dumps(risks_list).replace("'", "''")}',
                                            RISK_LEVEL = '{edit_risk_level}',
                                            OWNER_EMAIL = '{edit_owner_email}',
                                            STATUS = 'PENDING_APPROVAL',
                                            APPROVAL_COMMENT = NULL
                                        WHERE METRIC_ID = {metric_id}
                                    """
                                    session.sql(update_sql).collect()
                                    clear_cache()
                                    for key in ['edit_ai_desc', 'edit_dims', 'edit_meas', 'edit_risks_field', 'edit_risk_level_select', 'edit_recommendations', 'edit_ai_analyzed']:
                                        if key in st.session_state:
                                            del st.session_state[key]
                                    st.success(f":material/check: Metric '{edit_metric_name}' submitted for approval!")
                                    st.balloons()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                        with col_b:
                            if st.button(":material/save: Save as Draft", use_container_width=True, key="save_draft_edit_btn"):
                                try:
                                    dims_list = [d.strip() for d in edit_suggested_dimensions.split(',') if d.strip()] if edit_suggested_dimensions else []
                                    meas_list = [m.strip() for m in edit_suggested_measures.split(',') if m.strip()] if edit_suggested_measures else []
                                    risks_list = [r.strip() for r in edit_potential_risks.split(',') if r.strip()] if edit_potential_risks else []
                                    
                                    update_sql = f"""
                                        UPDATE {FULL_PATH}.METRIC_REGISTRY 
                                        SET METRIC_NAME = '{edit_metric_name}',
                                            DOMAIN = '{edit_domain}',
                                            METRIC_SQL = '{edit_metric_sql.replace("'", "''")}',
                                            DESCRIPTION = '{edit_description.replace("'", "''")}',
                                            AI_DESCRIPTION = '{edit_ai_description.replace("'", "''")}',
                                            SUGGESTED_DIMENSIONS = '{json.dumps(dims_list).replace("'", "''")}',
                                            SUGGESTED_MEASURES = '{json.dumps(meas_list).replace("'", "''")}',
                                            POTENTIAL_RISKS = '{json.dumps(risks_list).replace("'", "''")}',
                                            RISK_LEVEL = '{edit_risk_level}',
                                            OWNER_EMAIL = '{edit_owner_email}',
                                            STATUS = 'DRAFT'
                                        WHERE METRIC_ID = {metric_id}
                                    """
                                    session.sql(update_sql).collect()
                                    clear_cache()
                                    st.success(f":material/check: Metric '{edit_metric_name}' saved as draft!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                        with col_c:
                            if st.button(":material/delete: Delete", use_container_width=True, key="delete_edit_btn"):
                                try:
                                    session.sql(f"DELETE FROM {FULL_PATH}.METRIC_REGISTRY WHERE METRIC_ID = {metric_id}").collect()
                                    clear_cache()
                                    st.success(f":material/check: Metric deleted!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")
                
                with col2:
                    st.markdown("#### Current Status")
                    status = safe_get(metric_row, 'STATUS', 'UNKNOWN')
                    if status == 'DRAFT':
                        st.info(f":material/edit: {status}")
                    elif status == 'REJECTED':
                        st.error(f":material/cancel: {status}")
                    elif status == 'APPROVED':
                        st.success(f":material/check_circle: {status}")
                    
                    st.markdown("#### Metadata")
                    st.caption(f"**Created By:** {safe_get(metric_row, 'CREATED_BY', 'N/A')}")
                    st.caption(f"**Created At:** {safe_get(metric_row, 'CREATED_AT', 'N/A')}")
        else:
            st.info(":material/info: No editable metrics. Only DRAFT, REJECTED, or APPROVED metrics can be edited.")
    else:
        st.info(":material/info: No metrics in registry.")

with tab4:
    st.subheader("Approval Workflow")
    st.markdown("Review metrics with AI-powered recommendations.")
    
    pending_df = get_pending_approvals()
    
    if pending_df is not None and not pending_df.empty:
        st.info(f":material/pending: {len(pending_df)} metric(s) pending approval")
        
        for idx, row in pending_df.iterrows():
            metric_id = safe_get(row, 'METRIC_ID', idx)
            metric_name = safe_get(row, 'METRIC_NAME', 'Unknown')
            domain = safe_get(row, 'DOMAIN', 'Unknown')
            metric_sql = safe_get(row, 'METRIC_SQL', '')
            description = safe_get(row, 'DESCRIPTION', '')
            ai_description = safe_get(row, 'AI_DESCRIPTION', '')
            risk_level = safe_get(row, 'RISK_LEVEL', 'UNKNOWN')
            owner_email = safe_get(row, 'OWNER_EMAIL', '')
            created_by = safe_get(row, 'CREATED_BY', '')
            potential_risks = safe_get(row, 'POTENTIAL_RISKS', '[]')
            suggested_dims = safe_get(row, 'SUGGESTED_DIMENSIONS', '[]')
            suggested_measures = safe_get(row, 'SUGGESTED_MEASURES', '[]')
            
            with st.expander(f":material/analytics: {metric_name} - {domain}", expanded=False):
                st.markdown("### :material/smart_toy: AI Approval Assistant")
                
                if st.button(f":material/psychology: Get AI Recommendation", key=f"ai_rec_btn_{metric_id}", use_container_width=True):
                    with st.spinner("AI is analyzing this metric for approval..."):
                        ai_rec = ai_approval_recommendation(
                            metric_name, metric_sql, description, risk_level, potential_risks
                        )
                    
                    if ai_rec and isinstance(ai_rec, dict):
                        if "error" in ai_rec:
                            st.error(f"AI Analysis failed: {ai_rec.get('error')}")
                        else:
                            st.session_state[f'ai_rec_result_{metric_id}'] = ai_rec
                            st.rerun()
                
                if f'ai_rec_result_{metric_id}' in st.session_state:
                    ai_rec = st.session_state[f'ai_rec_result_{metric_id}']
                    
                    if not isinstance(ai_rec, dict):
                        ai_rec = {"recommendation": "UNKNOWN", "confidence": 0, "reasoning": "Invalid AI response", "concerns": [], "suggested_changes": [], "compliance_check": "N/A"}
                    
                    rec_col1, rec_col2, rec_col3 = st.columns(3)
                    with rec_col1:
                        rec = ai_rec.get('recommendation', 'UNKNOWN')
                        if rec == 'APPROVE':
                            st.success(f":material/check_circle: {rec}")
                        elif rec == 'REJECT':
                            st.error(f":material/cancel: {rec}")
                        else:
                            st.warning(f":material/edit: {rec}")
                    
                    with rec_col2:
                        confidence = ai_rec.get('confidence', 0)
                        st.metric("AI Confidence", f"{confidence}%")
                    
                    with rec_col3:
                        st.markdown("**Compliance:**")
                        st.caption(ai_rec.get('compliance_check', 'N/A'))
                    
                    st.markdown("**AI Reasoning:**")
                    st.markdown(f'<div class="ai-response">{ai_rec.get("reasoning", "N/A")}</div>', unsafe_allow_html=True)
                    
                    concerns = ai_rec.get('concerns', [])
                    if concerns:
                        st.markdown("**Concerns:**")
                        for c in concerns:
                            st.warning(c)
                    
                    changes = ai_rec.get('suggested_changes', [])
                    if changes:
                        st.markdown("**Suggested Changes:**")
                        for ch in changes:
                            st.info(ch)
                
                st.markdown("---")
                
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.markdown("**SQL Definition:**")
                    st.code(metric_sql if metric_sql else 'No SQL provided', language='sql')
                    
                    st.markdown("**Original Description:**")
                    st.write(description if description else 'No description provided')
                    
                    if ai_description:
                        st.markdown("**AI-Enhanced Description:**")
                        st.markdown(f'<div class="ai-response">{ai_description}</div>', unsafe_allow_html=True)
                    
                    if suggested_dims and suggested_dims != '[]':
                        st.markdown("**Suggested Dimensions:**")
                        try:
                            dims = json.loads(suggested_dims) if isinstance(suggested_dims, str) else suggested_dims
                            for d in dims:
                                st.markdown(f"- {d}")
                        except:
                            st.write(suggested_dims)
                    
                    if suggested_measures and suggested_measures != '[]':
                        st.markdown("**Suggested Measures:**")
                        try:
                            measures = json.loads(suggested_measures) if isinstance(suggested_measures, str) else suggested_measures
                            for m in measures:
                                st.markdown(f"- {m}")
                        except:
                            st.write(suggested_measures)
                
                with col2:
                    st.markdown("**Risk Level:**")
                    if risk_level == 'HIGH':
                        st.error(f":material/warning: {risk_level}")
                    elif risk_level == 'MEDIUM':
                        st.warning(f":material/info: {risk_level}")
                    elif risk_level == 'LOW':
                        st.success(f":material/check: {risk_level}")
                    else:
                        st.info(f":material/help: {risk_level}")
                    
                    if potential_risks and potential_risks != '[]':
                        st.markdown("**Potential Risks:**")
                        try:
                            risks = json.loads(potential_risks) if isinstance(potential_risks, str) else potential_risks
                            for r in risks:
                                st.caption(f"⚠️ {r}")
                        except:
                            st.caption(potential_risks)
                    
                    st.markdown("**Owner:**")
                    st.write(owner_email if owner_email else 'Not specified')
                    
                    st.markdown("**Created By:**")
                    st.write(created_by if created_by else 'Unknown')
                
                st.markdown("---")
                st.markdown("### Approval Decision")
                
                approval_comment = st.text_area(
                    "Approval Comment *", 
                    key=f"comment_{metric_id}",
                    placeholder="Enter your approval decision rationale..."
                )
                
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    if st.button(":material/check_circle: Approve", key=f"approve_{metric_id}", type="primary", use_container_width=True):
                        if not approval_comment:
                            st.error("Please provide an approval comment.")
                        else:
                            try:
                                session.sql(f"""
                                    UPDATE {FULL_PATH}.METRIC_REGISTRY 
                                    SET STATUS = 'APPROVED',
                                        APPROVED_BY = CURRENT_USER(),
                                        APPROVED_AT = CURRENT_TIMESTAMP(),
                                        APPROVAL_COMMENT = '{approval_comment.replace("'", "''")}'
                                    WHERE METRIC_ID = {metric_id}
                                """).collect()
                                clear_cache()
                                st.success(f":material/check: Metric '{metric_name}' approved!")
                                st.balloons()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")
                
                with col_b:
                    if st.button(":material/cancel: Reject", key=f"reject_{metric_id}", use_container_width=True):
                        if not approval_comment:
                            st.error("Please provide a rejection reason.")
                        else:
                            try:
                                session.sql(f"""
                                    UPDATE {FULL_PATH}.METRIC_REGISTRY 
                                    SET STATUS = 'REJECTED',
                                        APPROVED_BY = CURRENT_USER(),
                                        APPROVED_AT = CURRENT_TIMESTAMP(),
                                        APPROVAL_COMMENT = '{approval_comment.replace("'", "''")}'
                                    WHERE METRIC_ID = {metric_id}
                                """).collect()
                                clear_cache()
                                st.warning(f"Metric '{metric_name}' rejected.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")
                
                with col_c:
                    if st.button(":material/edit: Request Changes", key=f"changes_{metric_id}", use_container_width=True):
                        if not approval_comment:
                            st.error("Please specify what changes are needed.")
                        else:
                            try:
                                session.sql(f"""
                                    UPDATE {FULL_PATH}.METRIC_REGISTRY 
                                    SET STATUS = 'DRAFT',
                                        APPROVAL_COMMENT = '{approval_comment.replace("'", "''")}'
                                    WHERE METRIC_ID = {metric_id}
                                """).collect()
                                clear_cache()
                                st.info(f"Metric '{metric_name}' returned for changes.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error: {e}")
    else:
        st.success(":material/check_circle: No metrics pending approval!")
        st.markdown("""\n        **Workflow Status:**
        - All submitted metrics have been reviewed
        - Check the Registry tab to see approved metrics
        - New submissions will appear here automatically
        """)

with tab5:
    st.subheader("Explore Metrics")
    st.markdown("Get AI-powered explanations of approved metrics.")
    
    metrics_df = get_metrics()
    
    if metrics_df is not None:
        approved_metrics = metrics_df[metrics_df['STATUS'] == 'APPROVED'] if not metrics_df.empty else metrics_df
        
        if not approved_metrics.empty:
            selected_metric = st.selectbox(
                "Select a metric to explore",
                options=approved_metrics['METRIC_NAME'].tolist()
            )
            
            if selected_metric:
                metric_row = approved_metrics[approved_metrics['METRIC_NAME'] == selected_metric].iloc[0]
                
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.markdown(f"### {metric_row['METRIC_NAME']}")
                    st.markdown(f"**Domain:** {metric_row['DOMAIN']}")
                    st.markdown(f"**Owner:** {metric_row.get('OWNER_EMAIL', 'N/A')}")
                    
                    st.markdown("#### SQL Definition")
                    st.code(metric_row['METRIC_SQL'], language='sql')
                    
                    st.markdown("#### AI-Enhanced Description")
                    st.markdown(f'<div class="ai-response">{metric_row.get("AI_DESCRIPTION", metric_row.get("DESCRIPTION", "N/A"))}</div>', unsafe_allow_html=True)
                
                with col2:
                    if metric_row.get('SUGGESTED_DIMENSIONS'):
                        st.markdown("#### Dimensions")
                        try:
                            dims = json.loads(metric_row['SUGGESTED_DIMENSIONS'])
                            for d in dims:
                                st.markdown(f"- {d}")
                        except:
                            st.write(metric_row['SUGGESTED_DIMENSIONS'])
                    
                    if metric_row.get('SUGGESTED_MEASURES'):
                        st.markdown("#### Related Measures")
                        try:
                            measures = json.loads(metric_row['SUGGESTED_MEASURES'])
                            for m in measures:
                                st.markdown(f"- {m}")
                        except:
                            st.write(metric_row['SUGGESTED_MEASURES'])
                
                st.markdown("---")
                
                if st.button(":material/smart_toy: Explain in Plain English", use_container_width=True):
                    with st.spinner("Generating explanation..."):
                        explanation = ai_explain_metric(metric_row['METRIC_NAME'], metric_row['METRIC_SQL'])
                    
                    st.markdown("### Plain English Explanation")
                    st.markdown(f'<div class="ai-response">{explanation}</div>', unsafe_allow_html=True)
        else:
            st.info("No approved metrics to explore yet.")
    else:
        st.info("Connect to view metrics.")

st.markdown("---")
st.caption("Governed Metric Registry v1.0 | Powered by Data Registry")
