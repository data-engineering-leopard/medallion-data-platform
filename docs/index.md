# My PySpark Project

A production grade data engineering project built with PySpark, following the **Medallion Architecture** pattern.

## Overview

This project ingests data from multiple source systems, cleans and validates it through a series of layers, and produces business-ready dimensional model tables.

## Architecture

- **Bronze** — Raw data ingested exactly as received from source systems
- **Silver** — Cleaned, validated and SCD2 tracked data
- **Gold** — Business ready dimensional model tables

## Source Systems

| Source | Data Events |
|---|---|
| Online TCG | Customers, Orders |
| Salesforce | Leads |

## Quick Start

### Prerequisites
- Python 3.11+
- Java 17
- PyCharm

### Installation

```bash
git clone https://github.com/data-engineering-leopard/medallion-data-platform.git
cd medallion-data-platform
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run the Pipeline

```bash
PYTHONPATH=src python src/my_project/pipeline.py
```

### Run the Tests

```bash
pytest tests/ -v
```

## Project Structure
