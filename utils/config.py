import os
from dotenv import load_dotenv

load_dotenv()

REDSHIFT_CONN_STRING = os.getenv("REDSHIFT_CONN")
S3_BUCKET = os.getenv("S3_BUCKET")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))