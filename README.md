# Autonomous-Data-Command-Center
An autonomous data engineering pipeline powered by PostgreSQL (pg_cron) and Python. It auto-heals data quality issues, executes 28 advanced business queries, and delivers a daily AI-synthesized executive dashboard using local LLMs.

# 🏢 Enterprise Autonomous Data Command Center


## 📌 Overview
The **Autonomous Data Command Center** is an end-to-end data engineering, analytics, and intelligence pipeline. Designed to mimic a Fortune 500 reporting environment, it autonomously cleans raw operational data, executes 28 complex analytical workloads, and acts as a virtual Chief Data Officer (CDO) by synthesizing the results into strategic business advice via an automated daily email and interactive Power BI dashboards.

## 🛠️ The Tech Stack & Architecture
This project utilizes a robust, containerized modern data stack to handle everything from raw data ingestion to AI-driven reporting.

* **🐧 Linux & 🐳 Docker (Infrastructure Layer):** The foundation of the project. Docker is used to containerize the PostgreSQL database, the Ollama LLM server, and the Python execution environment on a Linux host. This ensures 100% environment reproducibility, isolated dependencies, and scalable deployments.
  
* **🐘 PostgreSQL (Data Warehouse & Compute Layer):**
  Serves as the central nervous system. It stores the operational schemas (Customers, Orders, Inventory, Promos) and handles heavy analytical compute. The database enforces strict schema integrity constraints and utilizes advanced SQL (CTEs, window functions) to prevent Cartesian fan-outs during metric aggregation.
  
* **⏱️ pg_cron (In-Database Orchestration):**
  A native PostgreSQL extension acting as the heartbeat of the pipeline. It schedules and triggers the PL/pgSQL data quality checks and auto-healing functions autonomously, keeping orchestration strictly inside the database layer and eliminating the need for heavy external schedulers like Airflow.
  
* **🐍 Python (Middleware & Dispatch Layer):**
  The orchestration script (`daily_brief.py`). It uses `psycopg2` to extract the 28-metric analytical payload, interfaces with the local AI via REST requests, wraps the aggregated data in a responsive HTML layout, and dispatches the final Command Center brief via `smtplib`.
  
* **🤖 Local AI / Ollama (Intelligence Layer):**
  Powered by `qwen2.5-coder` running locally via Ollama. It ingests the daily metric payload (Revenue drops, Churn risk, Promo ROI) and performs zero-shot reasoning to generate the "Diagnostic Readout" and "Priority Directives," turning raw numbers into actionable executive strategy.
  
* **📊 Power BI (Interactive Visualization Layer):**
  While the Python pipeline delivers the daily high-level HTML brief, Power BI connects directly to the PostgreSQL database for deep-dive analytics. It visualizes the `dq_issues` (Data Quality) logs, tracks the `data_fix_audit` auto-healing success rate, and allows stakeholders to interactively slice RFM cohorts, inventory levels, and margin analyses using DAX.

## 🚀 Key Engineering Features

1. **🛡️ Self-Healing Data Pipeline:** Custom PL/pgSQL functions automatically identify and fix data anomalies (e.g., orphaned records, broken revenue math) prior to reporting.
2. **🧠 The 28-Query Analytics Engine:** Extracts complex business metrics including:
   * **Customer Intel:** RFM Segmentation (Recency, Frequency, Monetary), Cohort Retention Curves, and Customer Lifetime Value (LTV).
   * **Product Health:** Real-time stock alerts, Category Margins, and Dead Stock identification.
   * **Marketing:** Promotion Penetration and absolute Campaign ROI.
3. **✉️ Zero-Dependency HTML Dispatch:** The executive email uses clean, native HTML lists and formatting to ensure it renders perfectly on mobile and desktop email clients without CSS clipping.

## 📸 Output Previews

**1. The Automated Executive Email (Python + AI)**
*(Insert screenshot of the automated email with the AI strategic synthesis here)*

**2. The Auto-Healing Audit Log (PostgreSQL)**
*(Insert screenshot of the `data_fix_audit` table showing records fixed in real-time here)*

**3. The Interactive Dashboard (Power BI)**
*(Insert screenshot of your Power BI dashboard visualizing the sales and customer tiers here)*

## 👤 Author
**Sabir Naik** *Data & Analytics Professional* [[LinkedIn Profile](https://www.linkedin.com/in/sabir-naik-3b580b215/?skipRedirect=true)] | sabirnaik537@gmail.com
