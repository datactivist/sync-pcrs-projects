import requests
from io import StringIO
import pandas as pd
from dotenv import dotenv_values
import os


config = {
    **dotenv_values(".env.shared"),  # load shared development variables
    **dotenv_values(".env"),  # load sensitive variables
    **os.environ,  # override loaded values with environment variables
}

# Define your Airtable credentials
AIRTABLE_ACCESS_TOKEN = config["AIRTABLE_ACCESS_TOKEN"]
AIRTABLE_BASE_ID = config["AIRTABLE_BASE_ID"]
AIRTABLE_TABLE_NAME = config["AIRTABLE_TABLE_NAME"]

# Airtable API URL
airtable_base_url = (
    f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
)

# CSV Export URL
CSV_EXPORT_URL = config["CSV_EXPORT_URL"]

# Headers for authorization
headers = {"Authorization": f"Bearer {AIRTABLE_ACCESS_TOKEN}"}

PIVOT_COLUMN = config["PIVOT_COLUMN"]
COLUMNS_TO_CHECK = config["COLUMNS_TO_CHECK"].split(",")


def get_airtable_data(airtable_base_url):
    """
    Fetches data from Airtable

    Parameters
    ----------
    airtable_base_url : str
        The base URL for the Airtable API

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the data from Airtable
    """

    all_records = []
    offset = None

    while True:
        params = {}
        if offset:
            params["offset"] = offset

        # Request to Airtable API
        response = requests.get(airtable_base_url, headers=headers, params=params)
        response_data = response.json()

        # Append the records to the list
        records = response_data.get("records", [])
        all_records.extend(records)

        # Check if there's more data to fetch (pagination)
        offset = response_data.get("offset")
        if not offset:
            break

    print(f"Found {len(all_records)} records in Airtable")

    # Extract the data from the records
    data = []
    for record in all_records:
        record_data = record.get("fields", {})
        record_data["airtable_record_id"] = record.get("id")
        data.append(record_data)

    # Convert the data to a DataFrame
    df = pd.DataFrame(data)

    return df


def get_csv_export_data(csv_export_url):
    """Download and read the CSV export from PCRS

    Parameters
    ----------
    csv_export_url : str
        The URL to download the CSV export from PCRS

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the data from the CSV export
    """

    # Step 1: Download the CSV file
    response = requests.get(csv_export_url)

    # Check if the request was successful
    if response.status_code != 200:
        return f"Error: Unable to download CSV file, status code {response.status_code}"

    # Step 2: Read the CSV file
    csv_data = response.text

    # Step 3: Convert CSV data to DataFrame
    df = pd.read_csv(StringIO(csv_data))

    print(f"Found {len(df)} records in PCRS CSV export")

    return df


def synchronize_different_records(airtable_df, pcrs_df):
    """Synchronize records with differences between Airtable and PCRS

    For each record, check if any columns to check are different
    between Airtable and PCRS. If they are different, update the record in Airtable with the
    values from PCRS.

    Parameters
    ----------
    airtable_df : pd.DataFrame
        The DataFrame containing the data from Airtable

    pcrs_df : pd.DataFrame
        The DataFrame containing the data from the PCRS CSV export
    """

    print("Synchronizing different records...")

    # Ensure both DataFrames have the same PIVOT_COLUMN values
    common_ids = airtable_df[PIVOT_COLUMN].isin(pcrs_df[PIVOT_COLUMN])
    airtable_df = airtable_df[common_ids]
    pcrs_df = pcrs_df[pcrs_df[PIVOT_COLUMN].isin(airtable_df[PIVOT_COLUMN])]

    # Set index to PIVOT_COLUMN for both DataFrames
    airtable_df = airtable_df.set_index(PIVOT_COLUMN)
    pcrs_df = pcrs_df.set_index(PIVOT_COLUMN)

    # Align the DataFrames to ensure they have the same index
    airtable_df, pcrs_df = airtable_df.align(pcrs_df, join="inner", axis=0)

    # Identify records where columns differ
    different_records = airtable_df[
        ~airtable_df[COLUMNS_TO_CHECK].eq(pcrs_df[COLUMNS_TO_CHECK]).all(axis=1)
    ]

    print(f"Found {len(different_records)} records with differences")

    # Call Airtable API to update the records
    for index, row in different_records.iterrows():

        airtable_record_id = row["airtable_record_id"]

        # Prepare the updated fields from pcrs_df
        updated_data = pcrs_df.loc[index, COLUMNS_TO_CHECK].to_dict()

        record_url = f"{airtable_base_url}/{airtable_record_id}"
        data = {"fields": updated_data}

        # PATCH request instead of PUT to keep the existing fields not in PCRS
        response = requests.patch(record_url, headers=headers, json=data)

        if response.status_code != 200:
            print(
                f"Error updating record {airtable_record_id}, status code {response.status_code}"
            )
        else:
            print(f"Successfully updated record {airtable_record_id}")


def synchronize_missing_records(airtable_df, pcrs_df):
    """Synchronize records missing in Airtable

    For each record in PCRS that is missing in Airtable, create a new record in Airtable.

    Parameters
    ----------
    airtable_df : pd.DataFrame
        The DataFrame containing the data from Airtable

    pcrs_df : pd.DataFrame
        The DataFrame containing the data from the PCRS CSV export
    """

    print("Synchronizing missing records...")

    # Find records in PCRS that are missing in Airtable
    missing_records = pcrs_df[~pcrs_df[PIVOT_COLUMN].isin(airtable_df[PIVOT_COLUMN])]

    print(f"Found {len(missing_records)} records missing in Airtable")

    # Call Airtable API to create the missing records
    for _, row in missing_records.iterrows():
        record_url = airtable_base_url

        # drop all the data for which we don't have a column in Airtable
        data = row[COLUMNS_TO_CHECK].to_dict()
        data[PIVOT_COLUMN] = row[PIVOT_COLUMN]

        data = {
            "records": [
                {
                    "fields": data,
                }
            ]
        }

        print(f"Creating new record with data: {data}")

        response = requests.post(record_url, headers=headers, json=data)

        if response.status_code != 200:
            print(f"Error creating record, status code {response.status_code}")
            print(response.json())
        else:
            print(f"Successfully created record")


def synchronize_deleted_records(airtable_df, pcrs_df):
    """Synchronize records deleted in PCRS

    For each record in Airtable that is missing in PCRS, print the record

    Parameters
    ----------
    airtable_df : pd.DataFrame
        The DataFrame containing the data from Airtable

    pcrs_df : pd.DataFrame
        The DataFrame containing the data from the PCRS CSV export
    """

    print("Synchronizing deleted records...")

    # Find records in Airtable that are missing in PCRS
    deleted_records = airtable_df[
        ~airtable_df[PIVOT_COLUMN].isin(pcrs_df[PIVOT_COLUMN])
    ]

    print(f"Found {len(deleted_records)} records missing in PCRS")

    if not deleted_records.empty:
        print(deleted_records.head())


# Main process
if __name__ == "__main__":

    print("Step 1: Loading current data from Airtable...")

    airtable_df = get_airtable_data(airtable_base_url)
    print()

    print("Step 2: Loading current data from pcrs CSV export...")

    pcrs_df = get_csv_export_data(CSV_EXPORT_URL)
    print()

    print("Step 3: Synchronize data between Airtable and PCRS...")

    synchronize_different_records(airtable_df, pcrs_df)
    print()

    synchronize_missing_records(airtable_df, pcrs_df)
    print()

    synchronize_deleted_records(airtable_df, pcrs_df)
    print()

    print("Synchronization completed!")
