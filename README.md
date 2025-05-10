# ⚡ Real-Time AI-Powered Data Engineering Pipeline with Databricks & GPT

This open-source project showcases a **real-time, AI-enhanced data pipeline** built using the **modern Lakehouse architecture** on **Azure Databricks**. It simulates both **batch and streaming data**, processes it through **Bronze → Silver → Gold layers**, integrates **OpenAI’s GPT** to generate smart sales summaries, and finally renders everything in a live **Databricks dashboard**.

> 🚀 Perfect for showcasing Data Engineering, Streaming, GPT Integration, Secure Azure Mounting, and End-to-End Workflow Orchestration skills — just like what modern data teams at tech-forward companies need.

---

## 💡 What You'll See in This Project
![Real-Time Dashboard](real_time_dashboard/dashboard.JPG)


- 🔁 **Real-Time Streaming + Batch Ingestion** using Parquet & Delta formats
- 🧹 **Dirty Data Cleansing & Schema Enforcement**
- 🪙 **Bronze → Silver → Gold Lakehouse Layers** with structured processing
- 🤖 **AI-Driven GPT Summaries** generated from Gold data
- 📊 **Real-Time Sales Dashboard** inside Databricks
- 🔐 **Enterprise-Grade Security** with Azure Key Vault + Secret Scopes
- 📈 **Fully Orchestrated Pipeline** using Databricks Jobs + Delta Live Tables (DLT)

---

## 🧱 Project Architecture

This project follows a modern **Lakehouse + AI design pattern** built on Databricks and Azure. Here's the high-level architecture:

- 🔄 **Data Simulation**: Generates both batch and streaming Parquet files
- 🧪 **Ingestion Layer**:
  - Batch → Ingested via PySpark notebooks to Silver layer
  - Streaming → Ingested using **Delta Live Tables (DLT)** and stored in Silver
- 🧹 **Transformation Layer**: 
  - Cleansed, validated, enriched in Silver
  - Processed into Gold with analytics-ready columns
- 🤖 **AI Insights Layer**: 
  - Uses **OpenAI GPT-3.5** to summarize Gold data into natural language insights
- 📊 **Visualization Layer**:
  - Real-time Databricks dashboard (sales trends, categories, GPT summary)
- 🔐 **Security Layer**:
  - Azure Key Vault + Secret Scopes for secure mounting and key management
- ⚙️ **Orchestration**:
  - A Unified Databricks Job manages batch, streaming, AI, and dashboard end-to-end

---

### 📌 Architecture Diagram

![Project Architecture](real_time_dashboard/ArchitectureDiagram.png)

