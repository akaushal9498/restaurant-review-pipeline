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
├── transformation/              # Data cleaning and normalization
├── warehouse_loader/            # Load scripts to Redshift
├── utils/                        # Configs, logger, DB connectors
├── tests/                        # Unit tests for transformations
├── walkthrough.mp4              # Demo walkthrough (video)
├── README.md
├── requirements.txt
```

---

## 🔁 Pipeline Flow

### 1. Ingestion
- `ingest_swiggy.py`: Reads Swiggy restaurant JSON
- `ingest_amazon_stream.py`: Simulates a streaming insert from SQLite

### 2. Transformation
- `clean_swiggy.py`: Normalizes and deduplicates restaurant data
- `clean_amazon.py`: Cleans and deduplicates review data

### 3. Load
- `load_to_redshift.py`: Uploads data to Amazon Redshift (or mocks if not configured)

### 4. Orchestration
- `restaurant_pipeline.py`: Airflow DAG with separate branches for Swiggy and Amazon

---

## 🧪 Testing
Run unit tests:
```bash
python -m unittest discover tests/
```

---

## ⚙️ Setup
```bash
# Create venv & install requirements
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Setup Airflow (local)
airflow db init
airflow users create ...
airflow scheduler & airflow webserver
```

---

## 📊 Monitoring & Logging
- Logs written via custom logger in `utils/logger.py`
- Add support for performance metrics in Airflow logs or integrate with Prometheus/Grafana

## 📥 Dataset Sources
- **Swiggy JSON**: [Swiggy Restaurants Dataset](https://www.kaggle.com/datasets/ashishjangra27/swiggy-restaurants-dataset)
- **Amazon Reviews (SQLite)**: [Amazon Fine Food Reviews](https://www.kaggle.com/datasets/ajaysh/amazon-fine-food-reviews)


---

## 📌 Notes
- Pipeline assumes Redshift config is in `utils/config.py`
- Modify `STREAM_OUTPUT_PATH`, `SQLITE_DB_PATH`, and Redshift creds as needed