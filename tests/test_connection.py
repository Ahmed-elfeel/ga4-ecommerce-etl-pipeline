"""
OBJECTIVE:
-To test if the BigQuery connection is working and if the service account key file is valid.

INSTRUCTIONS:
1- To run the test, you need to have the service account key file in the config/credentials directory.
The file is called service_account.json.
2- Run with: python tests/test_connection.py

REQUIREMENTS:
-To get a service account key file, create a GCP service account with BigQuery Data Editor and BigQuery Job User roles and place the JSON key at config/credentials/service_account.json

OUTPUT:
-If the connection is successful, you will see the message "✅ BigQuery connection successful!" and the number of events on 2020-11-01.
-If the connection is not successful, you will see the error message.

"""

# IMPORTS
from google.cloud import bigquery
from google.oauth2 import service_account

# FUNCTIONS
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

    # EXECUTE QUERY
    result = list(client.query(query).result())
    print(f"✅ BigQuery connection successful!")
    print(f"✅ Events on 2020-11-01: {result[0].total_events:,}")

# MAIN
if __name__ == "__main__":
    # RUN TEST
    test_bigquery_connection()