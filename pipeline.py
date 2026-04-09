import boto3
import pandas as pd
from dotenv import load_dotenv
import os
import io
import botocore
from pathlib import Path

# Load environment variables from .env file
load_dotenv()

# Initialize S3 client using credentials from environment variables
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

# Define Medallion Architecture layers and their corresponding S3 buckets
LAYERS = {
    'bronze': 'oussama-bronze-layer',  # Raw data as-is
    'silver': 'oussama-silver-layer',  # Cleaned and standardized data
    'gold':   'oussama-gold-layer',    # Aggregated and business-ready data
}

# Use dynamic base path to support any installation environment
BASE_DIR = Path(__file__).parent
local_folder = BASE_DIR / "Data"

# Create S3 buckets only if they don't already exist
for layer, bucket in LAYERS.items():
    try:
        s3_client.head_bucket(Bucket=bucket)
        print(f"⚠️  [{layer.upper()}] Already exists: {bucket}")
    except botocore.exceptions.ClientError:
        s3_client.create_bucket(Bucket=bucket)
        print(f"✅ [{layer.upper()}] Created: {bucket}")


def upload_to_bronze():
    """
    Upload local CSV files to the Bronze layer.
    Skips files that already exist in S3 to avoid duplicates.
    """
    for file_name in os.listdir(local_folder):
        local_path = local_folder / file_name
        if local_path.is_file() and file_name.endswith('.csv'):
            try:
                # Check if file already exists in Bronze
                s3_client.head_object(Bucket=LAYERS['bronze'], Key=f'raw/{file_name}')
                print(f"⚠️  {file_name} already in Bronze, skipping...")
            except botocore.exceptions.ClientError:
                # Upload file if it doesn't exist
                s3_client.upload_file(str(local_path), LAYERS['bronze'], f'raw/{file_name}')
                print(f"📤 Uploaded: {file_name}")


def transform_bronze_to_silver():
    """
    Transform raw Bronze data into clean Silver data:
    - Parse dates and numeric columns
    - Remove duplicates
    - Fill missing values
    - Save cleaned data back to S3 Silver layer
    """
    # Read raw data from Bronze layer
    obj = s3_client.get_object(Bucket=LAYERS['bronze'], Key='raw/SalesData.csv')
    bronze_df = pd.read_csv(obj['Body'])
    silver_df = bronze_df.copy()

    # Convert date column to datetime format
    silver_df['Date'] = pd.to_datetime(silver_df['Date'], errors='coerce')

    # Convert revenue columns to numeric, invalid values become NaN
    for col in ["Forecasted Monthly Revenue", "Weighted Revenue"]:
        silver_df[col] = pd.to_numeric(silver_df[col], errors='coerce')

    # Remove duplicate rows
    silver_df = silver_df.drop_duplicates()

    # Fill missing numeric values with 0
    for col in silver_df.select_dtypes(include='number').columns:
        silver_df[col] = silver_df[col].fillna(0)

    # Fill missing text values with "Unknown"
    for col in silver_df.select_dtypes(include='object').columns:
        silver_df[col] = silver_df[col].fillna("Unknown")

    # Save cleaned data to Silver layer using in-memory buffer
    buffer = io.StringIO()
    silver_df.to_csv(buffer, index=False)
    s3_client.put_object(
        Bucket=LAYERS['silver'],
        Key='cleaned/SalesData.csv',
        Body=buffer.getvalue()
    )
    print(f"📊 Bronze: {len(bronze_df)} rows")
    print(f"✨ Silver: {len(silver_df)} clean rows")
    return silver_df


def transform_silver_to_gold():
    """
    Transform clean Silver data into aggregated Gold data:
    - Group by Region and Segment
    - Calculate total revenue, deals, average revenue, and closed deals
    - Save business-ready data to S3 Gold layer
    """
    # Read cleaned data from Silver layer
    obj = s3_client.get_object(Bucket=LAYERS['silver'], Key='cleaned/SalesData.csv')
    silver_df = pd.read_csv(obj['Body'])

    # Aggregate data by Region and Segment
    gold_df = silver_df.groupby(['Region', 'Segment']).agg(
        Total_Revenue = ('Weighted Revenue', 'sum'),
        Total_Deals   = ('Lead Name', 'count'),
        Avg_Revenue   = ('Weighted Revenue', 'mean'),
        Closed_Deals  = ('Closed Opportunity', 'sum')
    ).reset_index()

    # Round average revenue to 2 decimal places
    gold_df['Avg_Revenue'] = gold_df['Avg_Revenue'].round(2)

    # Save aggregated data to Gold layer using in-memory buffer
    buffer = io.StringIO()
    gold_df.to_csv(buffer, index=False)
    s3_client.put_object(
        Bucket=LAYERS['gold'],
        Key='aggregated/SalesData.csv',
        Body=buffer.getvalue()
    )
    print(f"🏆 Gold: {len(gold_df)} aggregated rows")
    return gold_df


if __name__ == "__main__":
    upload_to_bronze()
    transform_bronze_to_silver()
    transform_silver_to_gold()
    print("🚀 Pipeline completed successfully!")