# restaurant-review-pipeline

# 🍽️ Restaurant Review Pipeline

This project provides a complete **data pipeline** for processing both **batch and streaming restaurant review data**. It is divided into two main components:

- **Batch processing** for Swiggy reviews  
- **Streaming processing** for Amazon reviews

---

## 📁 Project Structure

restaurant-review-pipeline/
├── dags/
│   └── swiggy_batch_etl.py          # Airflow DAG for batch processing
├── data/
│   ├── data.json                     # Sample data file
│   └── database.sqlite               # SQLite database file
├── ingestion/
│   ├── data/                         # Data directory
│   ├── amazon_review_stream.py       # Amazon review stream processor
│   ├── constants.py                  # Project constants
│   ├── helper_flatten.py             # Helper for flattening data
│   ├── ingest_amazon_stream.py       # Amazon review ingestion
│   ├── ingest_swiggy.py              # Swiggy review ingestion
│   └── utils.py                      # Utility functions
├── jar_files/
│   └── sqlite-jdbc-3.49.1.0.jar      # JDBC driver for SQLite
├── warehouse_loader/                 # Data warehouse loading scripts
├── README.md                         
└── requirements.txt                  # Python dependencies

---

## 🎬 Recording Links

- **Batch (Swiggy)**: [Watch here](https://drive.google.com/file/d/1ORKnLmGTc9T8KMrsyCFSov0cimUk7UH5/view?usp=sharing)  
- **Streaming (Amazon)**: [Watch here](https://drive.google.com/file/d/1-3bangBOIj-sr0g0ZKTVKbQdn0dxyvXE/view?usp=sharing)

---

## 📦 Batch Processing: Swiggy Reviews

### 📌 Description

The batch pipeline:
- Flattens nested Swiggy review JSON
- Ingests the processed data
- Loads it into **Amazon Redshift**

### ▶️ How to Run

1. Ensure **Apache Airflow** is installed and configured.
2. Place `swiggy_batch_etl.py` in your Airflow DAGs directory.
3. Airflow will auto-detect the DAG.
4. Trigger the DAG manually or let it run as per schedule.

### 🧩 Dependencies

- Apache Airflow
- Python packages listed in `requirements.txt`

---

## 🔁 Streaming Processing: Amazon Reviews

### 📌 Description

The streaming pipeline:
- Simulates a stream of Amazon restaurant reviews
- Processes the stream in 5-second intervals
- Ingests the reviews into the system

### ▶️ How to Run

**Terminal 1: Start the stream**
python ingestion/ingest_amazon_stream.py

**Terminal 2: Start the ingestion
The ingestion script reads new data every 5 seconds.


🧩 Dependencies
Python 3.x

Python packages listed in requirements.txt


📦 Requirements
Install all required Python packages:

pip install -r requirements.txt


✅ Assumptions
Swiggy batch data is available in the correct format and path

Amazon streaming data is consistently emitted by the simulator

Redshift cluster is accessible and properly configured

Airflow is fully operational

Your Python environment can read/write files and connect to databases

🧰 Troubleshooting
❌ Batch failure? → Check Airflow logs in the UI

💤 Streaming stopped? → Ensure both simulator and ingestion scripts are running

🔑 Database errors? → Double-check SQLite and Redshift credentials and paths