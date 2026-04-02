# Function to fetch day-ahead prices and return the average price
import pandas as pd
from entsoe import EntsoePandasClient
from datetime import datetime, timedelta
import time
import pytz
import os
import getPFC

# Function to fetch day-ahead prices and return the average price
def fetch_avg_price():
    # Read API token from environment or configuration
    API_TOKEN = 'b2432805-be69-481b-a32d-b850a6f11aa6'

    # Initialize the EntsoePandasClient with the API token
    client = EntsoePandasClient(api_key=API_TOKEN)

    # Set up the parameters for the API request
    bidding_zone = 'DE_LU'  # Bidding zone for DE-LU

    # Define the end date as current UTC date
    end_date = pd.Timestamp.now(tz='UTC')
    # Define the start date as 365 days before the end date
    start_date = end_date - timedelta(days=360)
    
    # Define a one-month time step
    time_step = pd.DateOffset(months=1)

    # DataFrame to store all results
    all_data = pd.DataFrame()

    # Fetch data in chunks
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + time_step, end_date)
        success = False
        max_retries = 3
        retry_count = 0
        
        while not success and retry_count < max_retries:
            try:
                # Fetch day-ahead prices for the current period
                df = client.query_day_ahead_prices(bidding_zone, 
                                                 start=current_start, 
                                                 end=current_end)

                # Convert the 'index' to the local timezone
                df.index = df.index.tz_convert('Europe/Berlin')

                # Append to the result DataFrame
                all_data = pd.concat([all_data, df])

                print(f"Fetched data from {current_start} to {current_end}")
                success = True
                
            except Exception as e:
                retry_count += 1
                wait_time = retry_count * 5  # Increasing wait time with each retry
                print(f"Attempt {retry_count} failed. Retrying in {wait_time} seconds...")
                print(f"Error: {e}")
                time.sleep(wait_time)
        
        if not success:
            print(f"Failed to fetch data after {max_retries} attempts for period {current_start} to {current_end}")
            return None

        # Move to the next period
        current_start = current_end

    try:
        # Save the complete dataset to CSV
        all_data.to_csv('./prices/DA_prices/DA_prices_year.csv', index=True, header=['Price (EUR/MWh)'])
        print("Data saved to './prices/DA_prices/DA_prices_year.csv'")

        # Calculate and return the average price
        avg_price = float(all_data.mean())
        print(f"Average price: {avg_price:.2f} EUR/MWh")
        return avg_price
        
    except Exception as e:
        print(f"Failed to process final data: {e}")
        return None

def fetch_current_year_prices():
    # Initialize API clients and get token
    API_TOKEN = 'b2432805-be69-481b-a32d-b850a6f11aa6'
    client = EntsoePandasClient(api_key=API_TOKEN)
    token = getPFC.read_token_from_csv()
    
    # Get current year's start and end (tz-naive)
    current_year = datetime.now().year
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
    
    # Fetch spot prices up to current time
    while current_start < current_time_utc:
        current_end = min(current_start + time_step, current_time_utc)
        try:
            df = client.query_day_ahead_prices('DE_LU', start=current_start, end=current_end)
            # Convert to Berlin time and then remove timezone
            df.index = df.index.tz_convert('Europe/Berlin').tz_localize(None)
            # Convert Series to DataFrame with proper column name
            if isinstance(df, pd.Series):
                df = df.to_frame(name='Price (EUR/MWh)')
            spot_data = pd.concat([spot_data, df])
            print(f"Fetched spot data from {current_start} to {current_end}")
        except Exception as e:
            print(f"Failed to fetch spot data: {e}")
            return None
        current_start = current_end
    
    # Fetch PFC prices for the rest of the year
    url = "https://api.montelnews.com/fundamental/get"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "FundamentalKey": "PRICEIT_POWER_HPFC_EEX_DE_GREEN_SETTLEMENT_DYN",
        "FromDate": current_time.strftime('%Y-%m-%d'),
        "ToDate": year_end.strftime('%Y-%m-%d'),
        "SortType": "ascending"
    }
    
    response_data, status_code, reason = getPFC.get_request(url, headers, params)
    if status_code == 200:
        pfc_df = getPFC.parse_to_dataframe(response_data)
        if isinstance(pfc_df, pd.Series):
            pfc_df = pfc_df.to_frame(name='Price (EUR/MWh)')
        else:
            pfc_df = pfc_df.rename(columns={'price': 'Price (EUR/MWh)'})
            pfc_df.index.name = 'Time'
        
        # Ensure PFC data is tz-naive
        if pfc_df.index.tz is not None:
            pfc_df.index = pfc_df.index.tz_localize(None)
        
        # Combine spot and PFC data
        all_data = pd.concat([spot_data, pfc_df])
    else:
        print("Failed to fetch PFC data")
        return None
    
    # Sort index and remove duplicates (prefer spot prices over PFC)
    all_data = all_data.sort_index()
    all_data = all_data.loc[~all_data.index.duplicated(keep='first')]
    
    # Create a complete hourly index for the current year
    complete_index = pd.date_range(
        start=f'{current_year}-01-01 00:00:00',
        end=f'{current_year}-12-31 23:00:00',
        freq='h'
    )
    
    # Reindex the data to match the complete timeline
    final_data = all_data.reindex(complete_index)
    final_data.index.name = 'Time'
    
    # Drop any NaN values
    final_data = final_data.dropna()
    
    # Ensure only Time and Price (EUR/MWh) columns
    if len(final_data.columns) > 1:
        final_data = final_data[['Price (EUR/MWh)']]
    
    # Save the complete dataset
    output_dir = f'./prices/DA_prices'
    os.makedirs(output_dir, exist_ok=True)
    output_path = f'{output_dir}/DA_prices_{current_year % 100:02d}.csv'
    
    # Save to CSV with proper formatting
    final_data.to_csv(output_path)
    print(f"Combined spot and PFC prices saved to {output_path}")
    
    return final_data

def fetch_year_prices(year):
    # Initialize API clients and get token
    API_TOKEN = 'b2432805-be69-481b-a32d-b850a6f11aa6'
    client = EntsoePandasClient(api_key=API_TOKEN)
    token = getPFC.read_token_from_csv()
    
    # Set fixed dates for 2024
    year_start = pd.Timestamp(f'{year}-01-01', tz='UTC')
    year_end = pd.Timestamp(f'{year}-12-31 23:59:59', tz='UTC')
    current_time = pd.Timestamp.now(tz='UTC')
    
    # Fetch historical spot prices
    all_data = pd.DataFrame()
    current_start = year_start
    time_step = pd.DateOffset(months=1)
    
    while current_start < min(current_time, year_end):
        current_end = min(current_start + time_step, current_time)
        try:
            df = client.query_day_ahead_prices('DE_LU', start=current_start, end=current_end)
            # Convert to Berlin time and handle DST transitions
            df.index = df.index.tz_convert('Europe/Berlin')
            # Convert Series to DataFrame with proper column name
            if isinstance(df, pd.Series):
                df = df.to_frame(name='Price (EUR/MWh)')
            all_data = pd.concat([all_data, df])
            print(f"Fetched spot data from {current_start} to {current_end}")
        except Exception as e:
            print(f"Failed to fetch spot data: {e}")
            return None
        current_start = current_end
    
    # If we need future prices (PFC)
    if current_time < year_end:
        url = "https://api.montelnews.com/fundamental/get"
        headers = {"Authorization": f"Bearer {token}"}
        params = {
            "FundamentalKey": "PRICEIT_POWER_HPFC_EEX_DE_GREEN_SETTLEMENT_DYN",
            "FromDate": current_time.strftime('%Y-%m-%d'),
            "ToDate": year_end.strftime('%Y-%m-%d'),
            "SortType": "ascending"
        }
        
        response_data, status_code, reason = getPFC.get_request(url, headers, params)
        if status_code == 200:
            pfc_df = getPFC.parse_to_dataframe(response_data)
            if isinstance(pfc_df, pd.Series):
                pfc_df = pfc_df.to_frame(name='Price (EUR/MWh)')
            else:
                pfc_df = pfc_df.rename(columns={'price': 'Price (EUR/MWh)'})
            all_data = pd.concat([all_data, pfc_df])
    
    # Remove timezone info and sort index
    all_data.index = all_data.index.tz_localize(None)
    all_data = all_data.sort_index()
    
    # Handle any duplicate indices by taking the first value
    all_data = all_data.loc[~all_data.index.duplicated(keep='first')]
    
    # Create a complete hourly index for the year
    complete_index = pd.date_range(
        start=f'{year}-01-01 00:00:00',
        end=f'{year}-12-31 23:00:00',
        freq='h'
    )
    
    # Reindex the data to match the complete timeline
    final_data = all_data.reindex(complete_index)
    final_data.index.name = 'Time'
    
    # Drop any NaN values
    final_data = final_data.dropna()
    
    # Save the complete dataset
    output_dir = f'./prices/DA_prices'
    os.makedirs(output_dir, exist_ok=True)
    output_path = f'{output_dir}/DA_prices_{year % 100:02d}.csv'
    
    # Save to CSV with proper formatting
    final_data.to_csv(output_path)
    print(f"Combined spot and PFC prices saved to {output_path}")
    
    return final_data

# Call the function
if __name__ == "__main__":
    print(fetch_avg_price())
