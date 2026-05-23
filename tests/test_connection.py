"""
Test BigQuery connection and basic dataset access.
Run with: python tests/test_connection.py
"""
from google.cloud import bigquery
from google.oauth2 import service_account

def test_bigquery_connection():
    credentials = service_account.Credentials.from_service_account_file(
        'config/credentials/service_account.json',
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    client = bigquery.Client(
        credentials=credentials,
        project='darkroom-497016'
    )

    query = """
        SELECT COUNT(*) as total_events
        FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
        WHERE _TABLE_SUFFIX = '20201101'
    """

    result = list(client.query(query).result())
    print(f"✅ BigQuery connection successful!")
    print(f"✅ Events on 2020-11-01: {result[0].total_events:,}")

if __name__ == "__main__":
    test_bigquery_connection()