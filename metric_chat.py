import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
import re

st.set_page_config(
    page_title="Metric Chat",
    page_icon=":material/chat:",
    layout="wide"
)

DATABASE = "METRIC_REGISTRY_DB"
SCHEMA = "REGISTRY"
FULL_PATH = f"{DATABASE}.{SCHEMA}"

session = get_active_session()

st.markdown("""
<style>
    .main-header { font-size: 2rem; font-weight: bold; margin-bottom: 0.5rem; }
    .ai-response { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                   padding: 1rem; border-radius: 10px; color: white; margin: 0.5rem 0; }
    .metric-card { background: #f8f9fa; padding: 1rem; border-radius: 8px; 
                   border-left: 4px solid #667eea; margin: 0.5rem 0; }
    .stChatMessage { border-radius: 10px; }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)
def get_approved_metrics():
    try:
        return session.sql(f"""
            SELECT * FROM {FULL_PATH}.METRIC_REGISTRY 
            WHERE STATUS = 'APPROVED' 
            ORDER BY METRIC_NAME
        """).to_pandas()
    except:
        return None

def chat_with_metrics(question, approved_metrics_df):
    metrics_context = ""
    for _, row in approved_metrics_df.iterrows():
        metrics_context += f"""
        Metric: {row.get('METRIC_NAME', '')}
        Domain: {row.get('DOMAIN', '')}
        Description: {row.get('AI_DESCRIPTION') or row.get('DESCRIPTION', '')}
        SQL: {row.get('METRIC_SQL', '')}
        Dimensions: {row.get('SUGGESTED_DIMENSIONS', '')}
        ---
        """
    
    prompt = f"""You are a helpful data analyst assistant. You have access to the following approved business metrics:

{metrics_context}

User Question: {question}

Based on the metrics available, provide a helpful response. If the user asks for data:
1. Identify which metric(s) are relevant
2. Provide the SQL query to answer their question (modify the metric SQL as needed)
3. Explain what the query does

IMPORTANT: When providing SQL queries, wrap them in ```sql code blocks.

If the user asks about metric definitions or concepts, explain clearly.
If no relevant metric exists, suggest what metrics might be needed.

Format your response in markdown."""

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

def execute_query(sql):
    try:
        result = session.sql(sql).to_pandas()
        return {"success": True, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e)}

def extract_sql_from_response(response):
    sql_pattern = r'```sql\s*([\s\S]*?)```'
    matches = re.findall(sql_pattern, response, re.IGNORECASE)
    return [sql.strip() for sql in matches if sql.strip()]

with st.sidebar:
    st.markdown("### :material/chat: Metric Chat")
    st.markdown("AI-Powered Metric Assistant")
    st.markdown("---")
    
    approved_df = get_approved_metrics()
    
    if approved_df is not None and not approved_df.empty:
        st.success(f"{len(approved_df)} metrics available")
        
        st.markdown("#### Available Metrics")
        for _, row in approved_df.iterrows():
            with st.expander(f":material/analytics: {row['METRIC_NAME']}"):
                st.caption(f"**Domain:** {row['DOMAIN']}")
                st.caption(row.get('AI_DESCRIPTION') or row.get('DESCRIPTION', 'No description'))
    else:
        st.warning("No approved metrics")
    
    st.markdown("---")
    if st.button(":material/refresh: Refresh", key="refresh_btn", use_container_width=True):
        get_approved_metrics.clear()
        st.rerun()
    
    if st.button(":material/delete: Clear Chat", key="clear_btn", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

st.markdown('<p class="main-header">:material/chat: Metric Chat Assistant</p>', unsafe_allow_html=True)
st.caption("Ask questions about your approved metrics using natural language")
st.markdown("---")

approved_df = get_approved_metrics()

if approved_df is None or approved_df.empty:
    st.warning(":material/warning: No approved metrics available.")
    st.markdown("""
    **To use Metric Chat:**
    1. Go to Metric Registry app
    2. Define and approve metrics
    3. Return here to chat!
    """)
    st.stop()

if "messages" not in st.session_state:
    st.session_state.messages = []

for idx, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        
        if message["role"] == "assistant" and message.get("queries"):
            for q_idx, sql in enumerate(message["queries"]):
                with st.expander(f":material/code: SQL Query {q_idx + 1}", expanded=False):
                    st.code(sql, language="sql")
                    
                    result_key = f"result_{idx}_{q_idx}"
                    if result_key in st.session_state:
                        result = st.session_state[result_key]
                        if result["success"]:
                            st.success(f"Returned {len(result['data'])} rows")
                            st.dataframe(result["data"], use_container_width=True)
                        else:
                            st.error(f"Error: {result['error']}")
                    else:
                        if st.button(":material/play_arrow: Run Query", key=f"run_{idx}_{q_idx}", use_container_width=True):
                            with st.spinner("Executing..."):
                                result = execute_query(sql)
                            st.session_state[result_key] = result
                            st.rerun()

if prompt := st.chat_input("Ask about your metrics... (e.g., 'What is net revenue?' or 'Show revenue by country')"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            response = chat_with_metrics(prompt, approved_df)
        
        st.markdown(response)
        
        queries = extract_sql_from_response(response)
        
        if queries:
            st.markdown("---")
            for q_idx, sql in enumerate(queries):
                with st.expander(f":material/code: SQL Query {q_idx + 1}", expanded=True):
                    st.code(sql, language="sql")
        
        st.session_state.messages.append({
            "role": "assistant", 
            "content": response,
            "queries": queries
        })
        st.rerun()

st.markdown("---")
col1, col2 = st.columns(2)
with col1:
    st.markdown("**Example Questions:**")
    st.markdown("""
    - What is the net revenue metric?
    - Show me revenue by country
    - How is churn rate calculated?
    - What metrics are in the Finance domain?
    """)
with col2:
    st.markdown("**Tips:**")
    st.markdown("""
    - Ask about metric definitions
    - Request data with filters
    - Compare metrics across domains
    - Ask for SQL modifications
    """)

st.markdown("---")
st.caption("Metric Chat v1.0 | Powered by Metric Registry")
