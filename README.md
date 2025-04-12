# restaurant-review-pipeline
Overview
This project provides a data pipeline for processing both batch and streaming restaurant review data. The pipeline consists of two main components:

Batch processing for Swiggy reviews

Streaming processing for Amazon reviews

Recording Links

Batch - https://drive.google.com/file/d/1ORKnLmGTc9T8KMrsyCFSov0cimUk7UH5/view?usp=sharing
Streaming - https://drive.google.com/file/d/1-3bangBOIj-sr0g0ZKTVKbQdn0dxyvXE/view?usp=sharing

Project Structure
Copy
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

Batch Processing (Swiggy Reviews)
Description
The batch processing pipeline:

Flattens the nested Swiggy review data

Ingests the processed data

Loads it into Redshift

How to Run
Ensure you have Apache Airflow installed and configured

Place the swiggy_batch_etl.py file in your Airflow dags directory

The DAG will automatically be picked up by Airflow

Trigger the DAG manually or wait for its scheduled execution

Dependencies
Apache Airflow

Python dependencies listed in requirements.txt

Streaming Processing (Amazon Reviews)
Description
The streaming pipeline:

Simulates a stream of Amazon restaurant reviews

Processes the stream in 5-second intervals

Ingests the data into the system

How to Run
Run the stream simulator:

bash
Copy
python ingestion/amazon_review_stream.py
In a separate terminal, run the ingestion process:

bash
Copy
python ingestion/ingest_amazon_stream.py
The ingestion process will read from the stream every 5 seconds

Dependencies
Python 3.x

Dependencies listed in requirements.txt

Data Warehouse Loading
Both batch and streaming processed data will be loaded into Redshift. Ensure you have:

Redshift credentials properly configured

Necessary permissions set up

Warehouse loader scripts in place

Requirements
Install all required dependencies by running:

bash
Copy
pip install -r requirements.txt
Assumptions
For batch processing, data is available in the expected format in the specified location

For streaming, the stream source is reliable and consistently available

Redshift cluster is properly configured and accessible

Airflow is properly set up for batch processing

Python environment has all necessary permissions to read/write files and access databases

Troubleshooting
If batch processing fails, check Airflow logs for specific errors

If streaming stops, verify both the stream simulator and ingestion process are running

For database connection issues, verify credentials and network access