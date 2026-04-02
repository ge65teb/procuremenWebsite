"""
Data Processors Module

Combines all data preprocessing functionality:
- Data preprocessing (preprocess_data)
- Year adjustment for hedge periods
- Profile cost calculation
- Renewables data merging
- CSV file processing and archiving
"""

import os
import pandas as pd
import shutil
from datetime import datetime, timedelta
import calendar

import config


# ==================== DATA PREPROCESSING ====================

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
    new_dates = [shift_date_to_hedge_year(date, hedge_year) for date in df.index]
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


def shift_date_to_hedge_year(date, hedge_year):
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
    """Calculate the profile cost based on load and spot prices."""
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
            spot_prices_path = config.DA_PRICES_DIR / f'DA_prices_{load_year_str}.csv'
            spot_prices_year = pd.read_csv(spot_prices_path, parse_dates=['Time'])
            spot_prices_year.rename(columns={'Time': 'date', 'Price (EUR/MWh)': 'price'}, inplace=True)
            spot_prices_list.append(spot_prices_year)
            print(f"Spot prices for year {load_year_int} loaded successfully.")
        except FileNotFoundError:
            print(f"DA_prices_{load_year_str}.csv not found, using PFC prices instead for year {load_year}.")
            pfc_prices_path = config.PFC_DIR / today / f'PFC_{load_year}.csv'
            pfc_prices = pd.read_csv(pfc_prices_path, parse_dates=['date'])
            spot_prices_list.append(pfc_prices[['date', 'price']])

    # Combine spot prices from all years
    spot_prices = pd.concat(spot_prices_list)
    spot_prices.set_index('date', inplace=True)

    # Merge the data on the index (date)
    merged_data = pd.merge(df, spot_prices, how='outer', left_index=True, right_index=True)
    
    # Save intermediate files for debugging
    merged_data.to_csv(config.BASE_DIR / 'merged_data.csv')
    
    merged_data.dropna(inplace=True)
    merged_data.to_csv(config.BASE_DIR / 'full_merged_data.csv')

    # Calculate weighted price and average price
    weighted_price = (merged_data['Load'] * merged_data['price']).sum() / merged_data['Load'].sum()
    avg_price = merged_data['price'].mean()
    profile_cost = weighted_price / avg_price - 1

    return profile_cost


def preprocess_data(df, pfc):
    """Merge data with PFC prices."""
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
    renewables_path = config.PPA_DIR / config.RENEWABLES_FILE
    renewables_df = pd.read_csv(renewables_path, decimal=',')
    
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
        lambda x: x.replace(year=hedge_year) if x.month != 2 or x.day != 29 
        else x.replace(year=hedge_year, day=28) if not is_leap_year else x.replace(year=hedge_year)
    )
    
    # Set Date as index for merging
    renewables_df.set_index('Date', inplace=True)
    
    # Merge with processed_df
    merged_df = pd.merge(processed_df, renewables_df, left_index=True, right_index=True, how='left')
    
    # Fill missing renewables data with 0 (if any)
    merged_df['Wind'] = merged_df['Wind'].fillna(0)
    merged_df['Solar'] = merged_df['Solar'].fillna(0)
    
    print(f"Renewables data merged successfully. Wind range: {merged_df['Wind'].min():.4f} - {merged_df['Wind'].max():.4f}")
    print(f"Solar range: {merged_df['Solar'].min():.4f} - {merged_df['Solar'].max():.4f}")
    
    return merged_df


def extract_parameters_from_filename(filename):
    """Extract processing parameters from filename."""
    # Default values
    min_tranche_size = config.DEFAULT_MIN_TRANCHE_SIZE
    hedge_year = config.get_current_year() + 1
    hedge_fraction = config.DEFAULT_HEDGE_FRACTION
    ppa_hedge_fraction = config.DEFAULT_PPA_FRACTION
    solar_ppa_price = 50.0  # Default value
    wind_ppa_price = 45.0   # Default value
    
    # Try to extract from filename if it follows a specific pattern
    # This would need to be customized based on your filename conventions
    parts = filename.replace('.csv', '').split('_')
    
    # Example parsing logic - customize as needed
    for i, part in enumerate(parts):
        if part.lower() == 'hedge' and i + 1 < len(parts):
            try:
                hedge_fraction = float(parts[i + 1]) / 100.0
            except ValueError:
                pass
        elif part.lower() == 'ppa' and i + 1 < len(parts):
            try:
                ppa_hedge_fraction = float(parts[i + 1]) / 100.0
            except ValueError:
                pass
        elif len(part) == 4 and part.isdigit():
            try:
                hedge_year = int(part)
            except ValueError:
                pass
    
    return min_tranche_size, hedge_year, hedge_fraction, ppa_hedge_fraction, solar_ppa_price, wind_ppa_price


def process_and_archive_csv_files(df, file_path, archive_dir, temp_dir, filename, today):
    """
    Process and archive CSV files with parameter extraction from header row.
    
    Returns:
        tuple: (preprocessed_df, min_tranche_size, hedge_year, hedge_fraction, 
                ppa_hedge_fraction, solar_ppa_price, wind_ppa_price, total_volume, profile_cost)
    """
    # Extract parameters from the first row (like original code)
    min_tranche_size = df['Min tranche size'].iloc[0]  # Use iloc[0] to get first value
    hedge_year = int(df['Year'].iloc[0])
    hedge_fraction = df['Hedge fraction'].iloc[0] 
    ppa_hedge_fraction = df['PPA_fraction'].iloc[0]
    solar_ppa_price = df['Solar PPA price'].iloc[0]
    wind_ppa_price = df['Wind PPA price'].iloc[0]
    
    print(f"Processing with parameters:")
    print(f"  Hedge Year: {hedge_year}")
    print(f"  Hedge Fraction: {hedge_fraction:.2%}")
    print(f"  PPA Fraction: {ppa_hedge_fraction:.2%}")
    print(f"  Min Tranche Size: {min_tranche_size}")
    print(f"  Solar PPA Price: {solar_ppa_price}")
    print(f"  Wind PPA Price: {wind_ppa_price}")
    
    # Get PFC prices
    print('Fetching PFC data')
    import data_fetchers
    pfc = data_fetchers.get_latest_pfc(hedge_year)
    print('PFC data fetched successfully.')

    # Drop unnecessary columns (like original code)
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

    # Preprocess the data (merge with PFC)
    processed_df = preprocess_data(df, pfc)

    # Merge renewables data
    processed_df = merge_renewables(processed_df, hedge_year)

    # Add isPeak and Q columns (like original code)
    processed_df['isPeak'] = ((processed_df.index.hour >= 8) & 
                             (processed_df.index.hour < 20) & 
                             (processed_df.index.weekday < 5)).astype(int)
    processed_df['Q'] = processed_df.index.quarter.astype(str)

    # Archive the original file
    archive_path = archive_dir / f"{today}_{filename}"
    shutil.copy2(file_path, archive_path)
    print(f"Original file archived to: {archive_path}")
    
    # Save the processed file in the temp directory
    processed_df.to_csv(temp_dir / filename, index=True)

    return (processed_df, min_tranche_size, hedge_year, hedge_fraction, 
            ppa_hedge_fraction, solar_ppa_price, wind_ppa_price, total_volume, profile_cost)


def validate_input_data(df, filename):
    """
    Validate input data quality and structure.
    
    Args:
        df: Input DataFrame
        filename: Name of the file being processed
        
    Returns:
        dict: Validation results with issues found
    """
    issues = []
    
    # Check for required columns
    required_columns = ['Load']  # Add other required columns as needed
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        issues.append(f"Missing required columns: {missing_columns}")
    
    # Check for date index or Date column
    has_date = False
    if 'Date' in df.columns:
        has_date = True
    elif isinstance(df.index, pd.DatetimeIndex):
        has_date = True
    
    if not has_date:
        issues.append("No Date column or datetime index found")
    
    # Check for missing values
    if 'Load' in df.columns:
        missing_load = df['Load'].isna().sum()
        if missing_load > 0:
            issues.append(f"Missing load values: {missing_load}")
        
        # Check for negative loads
        negative_loads = (df['Load'] < 0).sum()
        if negative_loads > 0:
            issues.append(f"Negative load values: {negative_loads}")
        
        # Check for unrealistic loads (very high values)
        max_load = df['Load'].max()
        if max_load > 10000:  # Adjust threshold as needed
            issues.append(f"Unusually high load values detected (max: {max_load})")
    
    # Check data coverage
    if has_date:
        date_col = df['Date'] if 'Date' in df.columns else df.index
        date_range = pd.to_datetime(date_col).max() - pd.to_datetime(date_col).min()
        if date_range < pd.Timedelta(days=300):
            issues.append(f"Limited date coverage: only {date_range.days} days")
    
    return {
        'filename': filename,
        'issues': issues,
        'is_valid': len(issues) == 0,
        'total_records': len(df),
        'columns': list(df.columns)
    }


def create_data_summary(df, filename):
    """
    Create a summary of the input data.
    
    Args:
        df: Input DataFrame
        filename: Name of the file
        
    Returns:
        dict: Data summary statistics
    """
    summary = {
        'filename': filename,
        'total_records': len(df),
        'columns': list(df.columns),
        'date_range': None,
        'load_statistics': None
    }
    
    # Date range analysis
    if 'Date' in df.columns:
        dates = pd.to_datetime(df['Date'])
        summary['date_range'] = {
            'start': dates.min().strftime('%Y-%m-%d'),
            'end': dates.max().strftime('%Y-%m-%d'),
            'days': (dates.max() - dates.min()).days
        }
    elif isinstance(df.index, pd.DatetimeIndex):
        summary['date_range'] = {
            'start': df.index.min().strftime('%Y-%m-%d'),
            'end': df.index.max().strftime('%Y-%m-%d'),
            'days': (df.index.max() - df.index.min()).days
        }
    
    # Load statistics
    if 'Load' in df.columns:
        summary['load_statistics'] = {
            'mean': df['Load'].mean(),
            'median': df['Load'].median(),
            'min': df['Load'].min(),
            'max': df['Load'].max(),
            'std': df['Load'].std(),
            'total': df['Load'].sum()
        }
    
    return summary 