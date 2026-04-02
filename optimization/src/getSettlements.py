import pandas as pd
import requests
import csv
import os
from datetime import datetime
import getPFC
import functions


# Function to read token from token.csv
def read_token_from_csv(filename='/content/drive/Shareddrives/Ecoplanet_main/02_Product/07_Procurement/tools/hedger/src/token.csv'):
    with open(filename, mode='r') as file:
        csv_reader = csv.reader(file)
        token = next(csv_reader)[0]  # Get the token from the first row, first column
    return token


# Function to fetch and return futures data with adjusted Period names
def fetch_futures(year, period=None):
    """
    Fetch futures data for a specific year and optionally a specific period.
    period can be '1', '2', '3', '4', or 'Y'
    """
    products = ['EEX DEB', 'EEX DEP']
    
    # Map internal period format to API format
    period_mapping = {
        '1': f'Q1-{year}',
        '2': f'Q2-{year}',
        '3': f'Q3-{year}',
        '4': f'Q4-{year}',
        'Y': f'CAL-{year}'
    }
    
    # If period specified, only fetch that one, otherwise fetch all
    periods_to_fetch = [period_mapping[period]] if period else period_mapping.values()
    
    # Create combinations for the requested periods
    combinations = [f"{product} {period}" for product in products for period in periods_to_fetch]
    
    token = read_token_from_csv()
    url = "https://api.montelnews.com/derivatives/quote/get"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "SymbolKeys": combinations,
        "Fields": "PrevSettlement"
    }
    
    response_data, status_code, reason = get_request(url, headers, params)
    
    # Initialize result dictionary
    result = {'base': None, 'peak': None}
    
    if status_code == 200:
        elements = response_data.get('Elements', [])
        
        for element in elements:
            symbol_key = element['SymbolKey']
            prev_settlement = element['PrevSettlement']
            
            if prev_settlement is None:
                continue
                
            parts = symbol_key.split(' ')
            product = ' '.join(parts[:2])
            api_period = parts[-1]
            
            if 'DEB' in product:
                result['base'] = prev_settlement
            elif 'DEP' in product:
                result['peak'] = prev_settlement
    
    return result

def fetch_daily_futures():
    today = datetime.now().strftime('%y%m%d')
    current_year = datetime.now().year
    futures_dir = f'/content/drive/Shareddrives/Ecoplanet_main/02_Product/07_Procurement/tools/hedger/data/prices/futures'
    os.makedirs(futures_dir, exist_ok=True)
    
    periods = ['1', '2', '3', '4', 'Y']
    
    for year in range(current_year + 1, current_year + 5):
        output_path = f'{futures_dir}/futures_{year}.csv'
        
        # Initialize empty DataFrame
        futures = pd.DataFrame(index=periods, columns=['base', 'peak'])
        futures.index.name = 'Period'
        
        # Try to get PFC data once for the year
        try:
            pfc_data = getPFC.get_latest_pfc(year)
            pfc_futures = functions.calculate_futures_from_PFC(pfc_data)
        except Exception as e:
            print(f"Failed to get PFC data for {year}: {e}")
            continue
        
        # Process each period independently
        for period in periods:
            try:
                # Try to get futures from API for this period
                api_data = fetch_futures(year, period)
                
                # If we got base price from API, use it
                if api_data['base'] is not None:
                    futures.loc[period, 'base'] = api_data['base']
                else:
                    # Use PFC data as fallback for base
                    futures.loc[period, 'base'] = pfc_futures.loc[period, 'base']
                
                # If we got peak price from API, use it
                if api_data['peak'] is not None:
                    futures.loc[period, 'peak'] = api_data['peak']
                else:
                    # Use PFC data as fallback for peak
                    futures.loc[period, 'peak'] = pfc_futures.loc[period, 'peak']
                
            except Exception as e:
                print(f"Error processing period {period} for {year}: {e}")
                # Use PFC data as fallback for this period
                futures.loc[period, 'base'] = pfc_futures.loc[period, 'base']
                futures.loc[period, 'peak'] = pfc_futures.loc[period, 'peak']
        
        # Save the combined data
        futures.to_csv(output_path)
        print(f'Futures data for {year} saved to {output_path}')

def get_latest_futures(year):
    futures_path = f'/content/drive/Shareddrives/Ecoplanet_main/02_Product/07_Procurement/tools/hedger/data/prices/futures/futures_{year}.csv'
    
    if not os.path.exists(futures_path):
        raise FileNotFoundError(f"No futures data found for year {year}")
    
    return pd.read_csv(futures_path)

def get_request(url, headers, params):
    try:
        response = requests.get(url, headers=headers, params=params)
        return response.json(), response.status_code, response.reason
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
        return None, 500, str(e)

# Example call to fetch futures
#futures_25_df = fetch_futures(2026)
#print(futures_25_df)
