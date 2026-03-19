# governed-metric-registry
Trusted metrics for human and AI - Governed at the source

Overview 

Governed Metric Registry is a Snowflake-native platform for defining, approving, governing, and consuming enterprise business metrics. 

Unlike traditional semantic layers that focus on query abstraction for BI tools, this project focuses on metric trust and decision governance: 

Metrics are defined once 

Explicitly approved by the business 

Executed centrally in Snowflake 

Safely consumed by BI tools, Streamlit apps, and AI agents 

Enforced with role-aware access controls 

The registry ensures that humans and AI always compute metrics consistently, without exposing raw data or embedding logic in dashboards. 

What This Is (and Is Not) 

What this project is 

A central registry of approved business metrics 

A governance layer for KPI definitions and ownership 

An execution control plane for metrics in Snowflake 

An AI-safe interface for natural-language metric queries 

A single source of truth for enterprise KPIs 

What this project is not 

Not a BI semantic layer replacement 

Not a dimension / join modeling framework 

Not dashboard-specific 

Not free-form SQL over raw tables 

Semantic layers optimize query usability. Governed metric registries optimize decision trust. 

Key Capabilities 

Governed Metric Definitions 

Central table-driven registry of metric SQL 

Business ownership and approval metadata 

Domain, grain, and description per metric 

Version-ready design (future-proof) 

Role-Aware Enforcement 

Metrics execute only if the caller’s Snowflake role is authorized 

Built on Snowflake RBAC, Row Access Policies, and Masking Policies 

No hardcoded filters or application-level logic 

AI-Assisted Metric Access 

Natural language questions translated into SQL 

AI can execute only approved metrics 

Refusal behavior when metric is not registered, role is not authorized, or the question attempts raw data access 

Streamlit Governance UI 

Register metrics 

Approve or disable metrics 

Assign metric ownership 

No direct SQL editing required by business users. 

Auditability & Compliance 

Clear ownership per metric 

Centralized execution path 

Compatible with Snowflake access history and query logs 

High-Level Architecture 

Users / BI / AI → Governed Metric Registry → Approved Metric SQL Execution → Snowflake (Secure Views / Policies) 

Core principle: Consumers never query raw tables directly. They query metrics, and the registry decides what SQL is allowed, who can run it, and what data is visible. 

Example: Governed Metric Execution 

CALL RUN_METRIC(metric_name => 'TOTAL_NET_REVENUE', filters => OBJECT_CONSTRUCT('quarter', 'Q4')); 

AI Governance Model 

AI access rules: 

AI can only reference registered metrics 

AI cannot invent SQL 

AI cannot query raw tables 

AI must respect Snowflake role permissions 

Integration Patterns 

BI Tools 

Tableau / Power BI / Looker consume metric views 

Metric logic stays centralized 

Streamlit Apps 

Dashboards built directly on governed metrics 

AI / LLMs 

Natural language to governed metric execution 

No hallucinated KPIs 

Technology Stack 

Snowflake: Secure Views, Row Access Policies, Masking Policies, Stored Procedures 

Streamlit in Snowflake: Governance UI 

Snowflake Cortex: Natural language interpretation (optional) 


Getting Started (High Level) 

Deploy registry tables and procedures in Snowflake 

Apply security policies 

Launch Streamlit governance UI 

Register and approve initial metrics 

Connect BI tools or AI agents

License 

This project is licensed under the Apache License 2.0. See the LICENSE file for details. 

Why This Matters 

Inconsistent metrics break trust. Uncontrolled AI amplifies inconsistency. 

Governed Metric Registry ensures: 

One definition per KPI 

Business ownership 

AI with guardrails 

Snowflake-native enforcement 

Tagline 

Governed Metric Registry — where metrics are approved before they are computed. 