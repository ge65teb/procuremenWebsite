import os
import pandas as pd
import shutil
from datetime import datetime, timedelta
import sys
import getPFC
import calendar  # Import calendar for leap year calculations


def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(base_path, relative_path)


def adjust_year(df, hedge_year):
    """
    Adjust the dates in the DataFrame to the hedge year while maintaining weekday alignment.
    Handles leap year cases and ensures all days until year-end are included.
    """
    df = df.copy()
    
    # Ensure we're working with the Date as index
    if 'Date' in df.columns:
        df.set_index('Date', inplace=True)
    
    # Convert index to datetime if it's not already
    df.index = pd.to_datetime(df.index)
    
    # Store original dates before shifting
    df['original_date'] = df.index
    
    # Apply the date shifting function to the index
    new_dates = [shift_date_to_2025(date, hedge_year) for date in df.index]
    df.index = pd.DatetimeIndex(new_dates)
    
    # Remove rows where shifted date is not in hedge_year
    df = df[df.index.year == hedge_year]
    
    # Remove duplicates, keeping first occurrence
    df = df[~df.index.duplicated(keep='first')]
    
    # Sort by the shifted date
    df.sort_index(inplace=True)
    
    # Check for missing dates at the end of the year
    last_date = pd.Timestamp(f"{hedge_year}-12-31 23:00:00")
    if df.index.max() < last_date:
        # Find the missing dates
        missing_dates = []
        current_last_date = df.index.max()
        
        # For each missing day
        for day in range(current_last_date.day + 1, 32):
            # Find matching day/month entries in original data
            matching_dates = df['original_date'][
                (df['original_date'].dt.month == 12) & 
                (df['original_date'].dt.day == day)
            ]
            
            if not matching_dates.empty:
                for orig_date in matching_dates:
                    new_row = df[df['original_date'] == orig_date].copy()
                    new_index = orig_date.replace(year=hedge_year)
                    new_row.index = pd.DatetimeIndex([new_index])
                    missing_dates.append(new_row)
        
        if missing_dates:
            # Concatenate with main DataFrame
            df = pd.concat([df] + missing_dates)
            df.sort_index(inplace=True)
    
    # Drop the temporary column
    df = df.drop('original_date', axis=1)
    
    return df


def shift_date_to_2025(date, hedge_year):
    """
    Shift a single date to the hedge_year, matching the day of the week.
    """
    original_weekday = date.weekday()
    day = date.day
    month = date.month
    
    # Handle leap year issues
    try:
        shifted_date = date.replace(year=hedge_year)
    except ValueError:
        # If the day doesn't exist in hedge_year (e.g., Feb 29), adjust
        if month == 2 and day == 29:
            shifted_date = date.replace(year=hedge_year, day=28)
        else:
            raise
    
    # Adjust to match the original weekday
    shifted_weekday = shifted_date.weekday()
    days_difference = (shifted_weekday - original_weekday) % 7
    adjusted_date = shifted_date - pd.Timedelta(days=days_difference)
    
    # Ensure the adjusted date falls within hedge_year
    if adjusted_date.year != hedge_year:
        adjusted_date += pd.Timedelta(days=7)
    
    return adjusted_date


def calculate_profile_cost(df, today):
    df = df.copy()

    # Determine the years present in the data
    load_years = df.index.year.unique()

    # Initialize list to collect spot prices
    spot_prices_list = []

    for load_year in load_years:
        print(f"Processing year: {load_year}")
        load_year_int = int(load_year)
        load_year_str = f"{load_year_int % 100:02d}"

        try:
            # Load spot prices for each year
            spot_prices_year = pd.read_csv(f'/content/drive/Shareddrives/Ecoplanet_main/02_Product/07_Procurement/tools/hedger/data/prices/DA_prices/DA_prices_{load_year_str}.csv', parse_dates=['Time'])
            spot_prices_year.rename(columns={'Time': 'date', 'Price (EUR/MWh)': 'price'}, inplace=True)
            spot_prices_list.append(spot_prices_year)
            print(f"Spot prices for year {load_year_int} loaded successfully.")
        except FileNotFoundError:
            print(f"DA_prices_{load_year_str}.csv not found, using PFC prices instead for year {load_year}.")
            pfc_prices = pd.read_csv(f'/content/drive/Shareddrives/Ecoplanet_main/02_Product/07_Procurement/tools/hedger/data/prices/PFC/{today}/PFC_{load_year}.csv', parse_dates=['date'])
            spot_prices_list.append(pfc_prices[['date', 'price']])

    # Combine spot prices from all years
    spot_prices = pd.concat(spot_prices_list)
    spot_prices.set_index('date', inplace=True)

    # Merge the data on the index (date)
    merged_data = pd.merge(df, spot_prices, how='outer', left_index=True, right_index=True)
    merged_data.to_csv('merged_data.csv')


    merged_data.dropna(inplace=True)
    merged_data.to_csv('full_merged_data.csv')

    # Calculate weighted price and average price
    weighted_price = (merged_data['Load'] * merged_data['price']).sum() / merged_data['Load'].sum()
    avg_price = merged_data['price'].mean()
    profile_cost = weighted_price / avg_price - 1

    return profile_cost


def preprocess_data(df, pfc):
    merged_data = pd.merge(df, pfc, how='inner', left_index=True, right_index=True)

    # Drop NAs
    merged_data.dropna(inplace=True)

    return merged_data

def merge_renewables(processed_df, hedge_year):
    """
    Load renewables data, adjust the year to match hedge_year, and merge with processed_df.
    
    Args:
        processed_df: DataFrame with processed load data
        hedge_year: Target year for the hedge
        
    Returns:
        DataFrame with renewables data merged
    """
    # Load renewables data with comma as decimal separator
    renewables_path = "/content/drive/Shareddrives/Ecoplanet_main/02_Product/07_Procurement/tools/hedger/data/ppa/renewables_data.csv"
    renewables_df = pd.read_csv(renewables_path, decimal=',')  # Add decimal=',' parameter
    
    # Explicitly convert Wind and Solar columns to float
    renewables_df['Wind'] = renewables_df['Wind'].astype(float)
    renewables_df['Solar'] = renewables_df['Solar'].astype(float)
    
    # Print sample values to verify conversion
    print("Wind distribution sample:", renewables_df['Wind'].head().values)
    print("Solar distribution sample:", renewables_df['Solar'].head().values)
    
    # Convert 'Date' to datetime using the correct format
    renewables_df['Date'] = pd.to_datetime(renewables_df['Date'], format='%d.%m.%y %H:%M')
    
    # Check if target hedge_year is a leap year
    is_leap_year = hedge_year % 4 == 0
    
    # If the target year is not a leap year, remove Feb 29 entries before year replacement
    if not is_leap_year:
        renewables_df = renewables_df[~((renewables_df['Date'].dt.month == 2) & 
                                       (renewables_df['Date'].dt.day == 29))]
    
    # Now safely replace year in 'Date' column with hedge_year
    renewables_df['Date'] = renewables_df['Date'].apply(
        lambda x: x.replace(year=hedge_year)
    )
    
    # Set 'Date' as index
    renewables_df.set_index('Date', inplace=True)
    
    # Merge with processed_df (left join to keep all rows from processed_df)
    merged_df = pd.merge(processed_df, renewables_df, how='left', 
                         left_index=True, right_index=True)
    
    return merged_df


def process_and_archive_csv_files(df, file_path, archive_dir, temp_dir, filename, today):
    # Extract parameters
    min_tranche_size = df['Min tranche size'][0]
    hedge_year = int(df['Year'][0])
    # Values are now numeric (percentages), just convert to decimals
    hedge_fraction = df['Hedge fraction'][0] 
    PPA_hedge_fraction = df['PPA_fraction'][0]
    solar_ppa_price = df['Solar PPA price'][0]
    wind_ppa_price = df['Wind PPA price'][0]
    
    # Get PFC prices
    print('Fetching PFC data')
    pfc = getPFC.get_latest_pfc(hedge_year)
    print('PFC data fetched successfully.')

    # Drop unnecessary columns
    df.drop(columns=['Min tranche size', 'Year', 'Hedge fraction', 'PPA_fraction', 'Solar PPA price', 'Wind PPA price'], inplace=True)

    # Convert 'Date' to datetime and set as index
    df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%y %H:%M')
    df.set_index('Date', inplace=True)

    # Resample to hourly data and sum the 'Load'
    df = df.resample('h').sum()

    # Convert 'Load' from kW to MW
    df['Load'] /= 1000

    # Calculate total volume
    total_volume = df['Load'].sum()

    # Calculate profile cost
    profile_cost = calculate_profile_cost(df, today)

    # Adjust year
    df = adjust_year(df, hedge_year)

    # Preprocess the data
    processed_df = preprocess_data(df, pfc)

    # Merge renewables data
    processed_df = merge_renewables(processed_df, hedge_year)

    # Move original file to archive
    shutil.move(file_path, os.path.join(archive_dir, filename))

    # Save the processed file in the temp directory
    processed_df.to_csv(os.path.join(temp_dir, filename), index=True)

    return processed_df, min_tranche_size, hedge_year, hedge_fraction, PPA_hedge_fraction, solar_ppa_price, wind_ppa_price, total_volume, profile_cost


