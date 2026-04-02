import pandas as pd
import requests
import csv
import os
from datetime import datetime
from holidays import Germany

# Function to read token from token.csv
def read_token_from_csv(filename='/content/drive/Shareddrives/Ecoplanet_main/02_Product/07_Procurement/tools/hedger/src/token.csv'):
    with open(filename, mode='r') as file:
        csv_reader = csv.reader(file)
        token = next(csv_reader)[0]  # Get the token from the first row, first column
    return token

# Function to get request
def get_request(url, headers=None, params=None):
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json(), response.status_code, response.reason
        else:
            return response.text, response.status_code, response.reason
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}, None, None

def get_german_holidays(year):
    """Returns a list of German national holiday dates for a given year."""

    # Get all German national holidays
    de_holidays = Germany(years=year)
    # Convert to list of dates
    return list(de_holidays.keys())

def parse_to_dataframe(response_data):
    rows = []

    for element in response_data.get('Elements', []):
        for timespan in element.get('TimeSpans', []):
            row = {
                'date': pd.to_datetime(timespan['TimeFrom']).tz_localize(None),
                'price': timespan['Value']
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    df.set_index('date', inplace=True)

    # Get holidays for the years in the data
    years = df.index.year.unique()
    holidays = []
    for year in years:
        holidays.extend(get_german_holidays(year))

    # Create isPeak flag considering both time and holidays
    df['isPeak'] = (
        (df.index.hour >= 8) &
        (df.index.hour < 20) &
        (df.index.weekday < 5) &
        (~pd.DatetimeIndex(df.index.date).isin(holidays))
    )

    df['Q'] = df.index.quarter

    return df

def fetch_pfc(year):
    # Read the token from the CSV
    token = read_token_from_csv()
    url = "https://api.montelnews.com/fundamental/get"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    params = {
        "FundamentalKey": "PRICEIT_POWER_HPFC_EEX_DE_GREEN_SETTLEMENT_DYN",
        "FromDate": f"{year}-01-01",
        "ToDate": f"{year + 1}-01-01",
        "SortType": "ascending"
    }

    response_data, status_code, reason = get_request(url, headers, params)
    pfc = parse_to_dataframe(response_data)

    # Save to CSV
    # Use the specific daily folder path if called from fetch_daily_pfc logic,
    # but here we return the df. The save logic is also in fetch_daily_pfc.
    # However, legacy code might expect this to save to the root PFC folder too,
    # but let's stick to the file structure seen in the original file.
    # The original code saved to: .../data/prices/PFC_{year}.csv
    base_path = '/content/drive/Shareddrives/Ecoplanet_main/02_Product/07_Procurement/tools/hedger/data/prices'
    # pfc.to_csv(f'{base_path}/PFC_{year}.csv') # Original behavior, optional to keep or comment

    return pfc

def fetch_daily_pfc():
    today = datetime.now().strftime('%y%m%d')
    current_year = datetime.now().year
    pfc_dir = '/content/drive/Shareddrives/Ecoplanet_main/02_Product/07_Procurement/tools/hedger/data/prices/PFC'
    daily_dir = f'{pfc_dir}/{today}'

    # Create directories if they don't exist
    os.makedirs(daily_dir, exist_ok=True)

    # Fetch PFC for current year AND next 4 years
    # MODIFIED: Changed range start from current_year + 1 to current_year
    for year in range(current_year, current_year + 5):
        print(f'Fetching PFC data for {year}')
        output_path = f'{daily_dir}/PFC_{year}.csv'

        # Check if exists, if not fetch
        # (Or force update if needed, but original logic was 'if not os.path.exists')
        if not os.path.exists(output_path):
            try:
                pfc = fetch_pfc(year)
                pfc.to_csv(output_path)
                print(f'PFC data for {year} saved to {output_path}')
            except Exception as e:
                print(f"Error fetching/saving {year}: {e}")
        else:
            print(f'PFC data for {year} already exists')

def get_latest_pfc(year):
    pfc_dir = '/content/drive/Shareddrives/Ecoplanet_main/02_Product/07_Procurement/tools/hedger/data/prices/PFC'
    # Get the most recent directory
    dirs = [d for d in os.listdir(pfc_dir) if os.path.isdir(os.path.join(pfc_dir, d))]
    if not dirs:
        raise FileNotFoundError("No PFC data directories found")

    latest_dir = max(dirs)
    pfc_path = f'{pfc_dir}/{latest_dir}/PFC_{year}.csv'

    if not os.path.exists(pfc_path):
        raise FileNotFoundError(f"No PFC data found for year {year}")

    return pd.read_csv(pfc_path, index_col='date', parse_dates=['date'])
