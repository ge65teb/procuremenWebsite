"""
Data Fetchers Module

Combines all API data fetching functionality:
- Token management (getMontelToken)
- Spot price fetching (getSpot)
- PFC price fetching (getPFC)
- Futures data fetching (getSettlements)
- Daily check functionality
"""

import pandas as pd
import requests
import csv
import os
import time
import pytz
import psycopg2
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient
from holidays import Germany

import config


# ==================== TOKEN MANAGEMENT ====================

def get_data_from_db(db_url, query):
    """
    Connect to the PostgreSQL database and execute the given query.
    
    Parameters:
    - db_url (str): Database connection URL
    - query (str): SQL query to execute
    
    Returns:
    - list of tuples: The query results
    """
    try:
        with psycopg2.connect(db_url) as conn:
            print("Connected to the database successfully.")
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
        return rows
    except Exception as e:
        print(f"Database error: {e}")
        return []

def save_token_to_csv(filename, rows):
    """Save token data to CSV file."""
    try:
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)
        print(f"Token data successfully written to {filename}")
    except Exception as e:
        print(f"Error writing token to CSV: {e}")

def fetch_montel_token():
    """Fetch Montel token from database and save to CSV."""
    rows = get_data_from_db(config.DB_URL, config.TOKEN_QUERY)
    if rows:
        token_path = config.CONFIG_DIR / config.TOKEN_FILE
        save_token_to_csv(token_path, rows)
        return True
    else:
        print("No token data to save.")
        return False

def read_token_from_csv(filename=None):
    """Read token from CSV file."""
    if filename is None:
        filename = config.CONFIG_DIR / config.TOKEN_FILE
    
    try:
        with open(filename, mode='r') as file:
            csv_reader = csv.reader(file)
            token = next(csv_reader)[0]
        return token
    except Exception as e:
        print(f"Error reading token from CSV: {e}")
        return None


# ==================== DAILY CHECK ====================

def check_daily_run():
    """Check if daily tasks need to be run based on date flag."""
    flag_file = config.BASE_DIR / config.DAILY_RUN_FILE
    today = config.get_daily_date()
    
    # Check if file exists
    if flag_file.exists():
        with open(flag_file, 'r') as f:
            last_run = f.read().strip()
        
        # If last run was today, return False
        if last_run == today:
            return False
    
    # If file doesn't exist or last run wasn't today
    with open(flag_file, 'w') as f:
        f.write(today)
    return True


# ==================== SPOT PRICE FETCHING ====================

def fetch_avg_spot_price():
    """Fetch day-ahead prices and return the average price."""
    client = EntsoePandasClient(api_key=config.ENTSOE_API_TOKEN)
    
    # Set up date range
    end_date = pd.Timestamp.now(tz='UTC')
    start_date = end_date - timedelta(days=360)
    time_step = pd.DateOffset(months=1)
    
    all_data = pd.DataFrame()
    current_start = start_date
    
    while current_start < end_date:
        current_end = min(current_start + time_step, end_date)
        success = False
        retry_count = 0
        
        while not success and retry_count < config.MAX_API_RETRIES:
            try:
                df = client.query_day_ahead_prices(
                    config.ENTSOE_BIDDING_ZONE,
                    start=current_start,
                    end=current_end
                )
                
                # Convert timezone
                df.index = df.index.tz_convert(config.TIMEZONE)
                all_data = pd.concat([all_data, df])
                
                print(f"Fetched data from {current_start} to {current_end}")
                success = True
                
            except Exception as e:
                retry_count += 1
                wait_time = retry_count * config.RETRY_DELAY_SECONDS
                print(f"Attempt {retry_count} failed. Retrying in {wait_time} seconds...")
                print(f"Error: {e}")
                time.sleep(wait_time)
        
        if not success:
            print(f"Failed to fetch data after {config.MAX_API_RETRIES} attempts")
            return None
            
        current_start = current_end
    
    try:
        # Save data
        output_path = config.DA_PRICES_DIR / 'DA_prices_year.csv'
        all_data.to_csv(output_path, index=True, header=['Price (EUR/MWh)'])
        print(f"Data saved to {output_path}")
        
        avg_price = float(all_data.mean())
        print(f"Average price: {avg_price:.2f} EUR/MWh")
        return avg_price
        
    except Exception as e:
        print(f"Failed to process final data: {e}")
        return None

def fetch_current_year_prices():
    """Fetch current year prices combining spot and PFC data."""
    client = EntsoePandasClient(api_key=config.ENTSOE_API_TOKEN)
    token = read_token_from_csv()
    
    current_year = config.get_current_year()
    year_start = pd.Timestamp(f'{current_year}-01-01')
    year_end = pd.Timestamp(f'{current_year}-12-31 23:59:59')
    current_time = pd.Timestamp.now()
    
    # Convert to UTC for API calls
    year_start_utc = year_start.tz_localize('UTC')
    current_time_utc = current_time.tz_localize('UTC')
    
    # Fetch historical spot prices
    spot_data = pd.DataFrame()
    current_start = year_start_utc
    time_step = pd.DateOffset(months=1)
    
    while current_start < current_time_utc:
        current_end = min(current_start + time_step, current_time_utc)
        try:
            df = client.query_day_ahead_prices(config.ENTSOE_BIDDING_ZONE, 
                                             start=current_start, end=current_end)
            df.index = df.index.tz_convert(config.TIMEZONE).tz_localize(None)
            if isinstance(df, pd.Series):
                df = df.to_frame(name='Price (EUR/MWh)')
            spot_data = pd.concat([spot_data, df])
            print(f"Fetched spot data from {current_start} to {current_end}")
        except Exception as e:
            print(f"Failed to fetch spot data: {e}")
            return None
        current_start = current_end
    
    # Fetch PFC prices for rest of year
    url = f"{config.MONTEL_API_BASE_URL}{config.MONTEL_FUNDAMENTAL_ENDPOINT}"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "FundamentalKey": config.PFC_FUNDAMENTAL_KEY,
        "FromDate": current_time.strftime('%Y-%m-%d'),
        "ToDate": year_end.strftime('%Y-%m-%d'),
        "SortType": "ascending"
    }
    
    response_data, status_code, reason = make_api_request(url, headers, params)
    if status_code == 200:
        pfc_df = parse_pfc_response(response_data)
        if isinstance(pfc_df, pd.Series):
            pfc_df = pfc_df.to_frame(name='Price (EUR/MWh)')
        else:
            pfc_df = pfc_df.rename(columns={'price': 'Price (EUR/MWh)'})
            pfc_df.index.name = 'Time'
        
        if pfc_df.index.tz is not None:
            pfc_df.index = pfc_df.index.tz_localize(None)
        
        all_data = pd.concat([spot_data, pfc_df])
    else:
        print("Failed to fetch PFC data")
        return None
    
    # Process and save combined data
    all_data = all_data.sort_index()
    all_data = all_data.loc[~all_data.index.duplicated(keep='first')]
    
    complete_index = pd.date_range(
        start=f'{current_year}-01-01 00:00:00',
        end=f'{current_year}-12-31 23:00:00',
        freq='h'
    )
    
    final_data = all_data.reindex(complete_index)
    final_data.index.name = 'Time'
    final_data = final_data.dropna()
    
    if len(final_data.columns) > 1:
        final_data = final_data[['Price (EUR/MWh)']]
    
    output_path = config.DA_PRICES_DIR / f'DA_prices_{current_year % 100:02d}.csv'
    final_data.to_csv(output_path)
    print(f"Combined spot and PFC prices saved to {output_path}")
    
    return final_data


# ==================== PFC PRICE FETCHING ====================

def get_german_holidays(year):
    """Returns a list of German national holiday dates for a given year."""
    de_holidays = Germany(years=year)
    return list(de_holidays.keys())

def parse_pfc_response(response_data):
    """Parse PFC API response to DataFrame."""
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
    
    # Create isPeak flag
    df['isPeak'] = (
        (df.index.hour >= 8) & 
        (df.index.hour < 20) & 
        (df.index.weekday < 5) & 
        (~pd.DatetimeIndex(df.index.date).isin(holidays))
    )
    
    df['Q'] = df.index.quarter
    return df

def fetch_pfc_data(year):
    """Fetch PFC data for a specific year."""
    token = read_token_from_csv()
    url = f"{config.MONTEL_API_BASE_URL}{config.MONTEL_FUNDAMENTAL_ENDPOINT}"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "FundamentalKey": config.PFC_FUNDAMENTAL_KEY,
        "FromDate": f"{year}-01-01",
        "ToDate": f"{year + 1}-01-01",
        "SortType": "ascending"
    }
    
    response_data, status_code, reason = make_api_request(url, headers, params)
    if status_code == 200:
        pfc = parse_pfc_response(response_data)
        output_path = config.PFC_DIR / f'PFC_{year}.csv'
        pfc.to_csv(output_path)
        return pfc
    else:
        print(f"Failed to fetch PFC data for {year}: {status_code} - {reason}")
        return None

def fetch_daily_pfc():
    """Fetch PFC data for next 4 years."""
    today = config.get_daily_date()
    current_year = config.get_current_year()
    daily_dir = config.PFC_DIR / today
    daily_dir.mkdir(exist_ok=True)
    
    for year in range(current_year + 1, current_year + 5):
        print(f'Fetching PFC data for {year}')
        output_path = daily_dir / f'PFC_{year}.csv'
        if not output_path.exists():
            pfc = fetch_pfc_data(year)
            if pfc is not None:
                pfc.to_csv(output_path)
                print(f'PFC data for {year} saved to {output_path}')

def get_latest_pfc(year):
    """Get the latest PFC data for a specific year."""
    dirs = [d for d in config.PFC_DIR.iterdir() if d.is_dir()]
    if not dirs:
        raise FileNotFoundError("No PFC data directories found")
    
    latest_dir = max(dirs, key=lambda x: x.name)
    pfc_path = latest_dir / f'PFC_{year}.csv'
    
    if not pfc_path.exists():
        raise FileNotFoundError(f"No PFC data found for year {year}")
    
    return pd.read_csv(pfc_path, index_col='date', parse_dates=['date'])


# ==================== FUTURES DATA FETCHING ====================

def fetch_futures_data(year, period=None):
    """Fetch futures data for a specific year and optionally a specific period."""
    period_mapping = {
        '1': f'Q1-{year}',
        '2': f'Q2-{year}',
        '3': f'Q3-{year}',
        '4': f'Q4-{year}',
        'Y': f'CAL-{year}'
    }
    
    periods_to_fetch = [period_mapping[period]] if period else period_mapping.values()
    combinations = [f"{product} {period}" for product in config.FUTURES_API_PRODUCTS 
                   for period in periods_to_fetch]
    
    token = read_token_from_csv()
    url = f"{config.MONTEL_API_BASE_URL}{config.MONTEL_DERIVATIVES_ENDPOINT}"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "SymbolKeys": combinations,
        "Fields": "PrevSettlement"
    }
    
    response_data, status_code, reason = make_api_request(url, headers, params)
    
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
            
            if 'DEB' in product:
                result['base'] = prev_settlement
            elif 'DEP' in product:
                result['peak'] = prev_settlement
    
    return result

def fetch_daily_futures():
    """Fetch futures data for next 4 years."""
    current_year = config.get_current_year()
    
    for year in range(current_year + 1, current_year + 5):
        output_path = config.FUTURES_DIR / f'futures_{year}.csv'
        
        futures = pd.DataFrame(index=config.FUTURES_PRODUCTS, 
                             columns=config.FUTURES_TYPES)
        futures.index.name = 'Period'
        
        # Try to get PFC data for fallback
        try:
            from . import utilities
            pfc_data = get_latest_pfc(year)
            pfc_futures = utilities.calculate_futures_from_PFC(pfc_data)
        except Exception as e:
            print(f"Failed to get PFC data for {year}: {e}")
            continue
        
        # Process each period
        for period in config.FUTURES_PRODUCTS:
            try:
                api_data = fetch_futures_data(year, period)
                
                # Use API data if available, otherwise use PFC data
                futures.loc[period, 'base'] = (api_data['base'] 
                                             if api_data['base'] is not None 
                                             else pfc_futures.loc[period, 'base'])
                futures.loc[period, 'peak'] = (api_data['peak'] 
                                             if api_data['peak'] is not None 
                                             else pfc_futures.loc[period, 'peak'])
                
            except Exception as e:
                print(f"Error processing period {period} for {year}: {e}")
                futures.loc[period, 'base'] = pfc_futures.loc[period, 'base']
                futures.loc[period, 'peak'] = pfc_futures.loc[period, 'peak']
        
        futures.to_csv(output_path)
        print(f'Futures data for {year} saved to {output_path}')

def get_latest_futures(year):
    """Get the latest futures data for a specific year."""
    futures_path = config.FUTURES_DIR / f'futures_{year}.csv'
    
    if not futures_path.exists():
        raise FileNotFoundError(f"No futures data found for year {year}")
    
    return pd.read_csv(futures_path)


# ==================== UTILITY FUNCTIONS ====================

def make_api_request(url, headers=None, params=None):
    """Make API request with error handling."""
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json(), response.status_code, response.reason
        else:
            return response.text, response.status_code, response.reason
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}, None, None 