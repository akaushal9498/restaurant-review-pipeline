# restaurant-review-pipeline
# Swiggy & Amazon Reviews Data Pipeline

This project demonstrates a full data pipeline that ingests, transforms, and loads data from two sources:
- **Swiggy restaurants data** (static JSON, updated weekly)
- **Amazon fine food reviews** (simulated streaming from SQLite)

The processed data is loaded into a data warehouse (e.g., Amazon Redshift), with support for monitoring, logging, and scheduling using Apache Airflow.

---

## 🚀 Project Structure
```bash
project_root/
├── data/                         # Raw input data
├── dags/                         # Airflow DAGs
├── ingestion/                    # Scripts to ingest raw data
├── warehouse_loader/            # Load scripts to Redshift
├── utils/                        # Configs, logger, DB connectors
├── tests/                        # Unit tests for transformations
├── walkthrough.mp4              # Demo walkthrough (video)
├── README.md
├── requirements.txt
```

