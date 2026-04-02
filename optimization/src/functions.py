import pandas as pd


def save_combined_results_to_csv(results_max_hedge, results_sell_limit, filename, profile_cost, futures, avg_price):
    """
    Saves the combined results to a CSV, including results_max_hedge, results_sell_limit, profile_cost,
    futures (Period, base, peak), and avg_price.

    Parameters:
    results_max_hedge (DataFrame): DataFrame with max hedge results
    results_sell_limit (DataFrame): DataFrame with sell limit results
    filename (str): Name of the CSV file to save the results
    profile_cost (float): Profile cost value to be added
    futures (DataFrame): DataFrame containing 'Period', 'base', and 'peak' columns
    avg_price (float): The average price value to be added
    """

    # Create dataframes with the section headers
    max_hedge_header = pd.DataFrame([['Max hedge results']], columns=['net_spot_volume'])
    spot_limit_header = pd.DataFrame([['Spot limit results']], columns=['net_spot_volume'])

    # Filter out any NaN column names
    max_hedge_columns = ['index'] + [col for col in results_max_hedge.columns.tolist() if pd.notna(col)]
    spot_limit_columns = ['index'] + [col for col in results_sell_limit.columns.tolist() if pd.notna(col)]

    # Pad the header dataframes
    max_hedge_header = max_hedge_header.reindex(columns=max_hedge_columns, fill_value='')
    spot_limit_header = spot_limit_header.reindex(columns=spot_limit_columns, fill_value='')

    # Reset index and add as a column
    results_max_hedge = results_max_hedge.reset_index()
    results_sell_limit = results_sell_limit.reset_index()

    # Add the profile_cost column
    results_max_hedge['profile_cost'] = ''
    results_sell_limit['profile_cost'] = ''

    def format_number(x):
        if pd.notnull(x) and isinstance(x, (int, float)):
            return f"{x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        return x

    # Format numeric columns in results_max_hedge
    for col in results_max_hedge.columns:
        if col != 'index' and any(prefix in col for prefix in ['mw_', 'mwh_', 'ppa_', 'net_', 'abs_', 'spot_', 'hedge_', 'avg_']):
            results_max_hedge[col] = results_max_hedge[col].apply(format_number)

    # Format numeric columns in results_sell_limit
    for col in results_sell_limit.columns:
        if col != 'index' and any(prefix in col for prefix in ['mw_', 'mwh_', 'net_', 'abs_', 'spot_', 'hedge_', 'avg_']):
            results_sell_limit[col] = results_sell_limit[col].apply(format_number)

    # Create empty rows for spot limit section spacing (to maintain original positioning)
    if results_sell_limit.empty:
      # Add 6 empty rows to maintain spacing where spot limit results used to be
      empty_rows = pd.DataFrame([[''] * len(max_hedge_columns)] * 6, columns=max_hedge_columns)
      combined_df = pd.concat([max_hedge_header, results_max_hedge, spot_limit_header, empty_rows],
                            ignore_index=True)
    else:
    # Original behavior when results_sell_limit has data
      combined_df = pd.concat([max_hedge_header, results_max_hedge, spot_limit_header, results_sell_limit],
                            ignore_index=True)

    # Add profile_cost to the first row
    combined_df.loc[0, 'profile_cost'] = format_number(profile_cost)

    # Create futures header
    futures_header = pd.DataFrame([['Futures']], columns=['Period'])
    
    # Format futures prices
    futures = futures.copy()
    for col in ['base', 'peak']:
        futures[col] = futures[col].apply(format_number)

    # Create avg_price row
    avg_price_row = pd.DataFrame([['Average spot price', format_number(avg_price), '']], 
                               columns=['Period', 'base', 'peak'])

    # Concatenate futures section
    futures_section = pd.concat([futures_header, futures, avg_price_row], ignore_index=True)

    # Create final DataFrame
    final_df = pd.concat([combined_df, futures_section], ignore_index=True)

    # Save to CSV
    final_df.to_csv(filename, index=False, quoting=1)



def calculate_futures_from_PFC(df, product=None):
    """
    Calculate futures prices from PFC data.
    """
    # Ensure we have the 'price' column and datetime index
    if 'Price (EUR/MWh)' in df.columns:
        df = df.rename(columns={'Price (EUR/MWh)': 'price'})
    
    # Ensure index is datetime
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    
    # Initialize result DataFrame with the correct index
    result = pd.DataFrame(index=['1', '2', '3', '4', 'Y'], columns=['base', 'peak'])
    result.index.name = 'Period'
    
    # Define quarters mapping
    quarters = {'1': (1, 3), '2': (4, 6), '3': (7, 9), '4': (10, 12), 'Y': (1, 12)}
    
    # If specific product requested, only calculate that one
    products_to_calc = [product] if product else quarters.keys()
    
    for prod in products_to_calc:
        start_month, end_month = quarters[prod]
        
        if prod == 'Y':
            # Full year calculation
            period_data = df
        else:
            # Quarter calculation
            period_data = df[(df.index.month >= start_month) & (df.index.month <= end_month)]
        
        # Calculate base (all hours) and peak (8-20 on weekdays) prices
        base_price = period_data['price'].mean()
        peak_mask = (period_data.index.hour >= 8) & (period_data.index.hour < 20) & (period_data.index.weekday < 5)
        peak_price = period_data.loc[peak_mask, 'price'].mean()
        
        # Add to results
        result.loc[prod, 'base'] = base_price
        result.loc[prod, 'peak'] = peak_price
    
    return result

def combine_dataframes(summary, mwh_volumes, futures_volumes, ppa_volumes=None):
    """
    Combine the summary DataFrame with the mwh_volumes, futures_volumes, and ppa_volumes DataFrames.

    Parameters:
    - summary: DataFrame containing summary statistics.
    - mwh_volumes: DataFrame with MWh volumes.
    - futures_volumes: DataFrame with MW volumes.
    - ppa_volumes: DataFrame with PPA volumes (optional).

    Returns:
    - combined_df: DataFrame combining summary and volumes information.
    """
    # Transpose mwh_volumes and futures_volumes for alignment
    mwh_volumes_transposed = mwh_volumes.T
    futures_volumes_transposed = futures_volumes.T

    # Prefix the columns to distinguish between MWh and MW volumes
    mwh_volumes_transposed.columns = ['mwh_' + col for col in mwh_volumes_transposed.columns]
    futures_volumes_transposed.columns = ['mw_' + col for col in futures_volumes_transposed.columns]

    # Reset index to join on the 'index' column
    mwh_volumes_transposed.reset_index(inplace=True)
    futures_volumes_transposed.reset_index(inplace=True)

    # Merge the transposed dataframes on the 'index' column
    combined_transposed = pd.merge(mwh_volumes_transposed, futures_volumes_transposed, on='index')

    # If ppa_volumes is provided, include it in the combined dataframe
    if ppa_volumes is not None:
        # Create a new row in combined_transposed for PPA volumes
        ppa_row = pd.DataFrame({'index': ['PPA']})
        # Add PPA columns with correct values
        ppa_row['ppa_W'] = ppa_volumes.loc['W', 'W']  # Get Wind PPA value
        ppa_row['ppa_S'] = ppa_volumes.loc['S', 'S']  # Get Solar PPA value
        
        # Add empty values for other columns that exist in combined_transposed
        for col in combined_transposed.columns:
            if col not in ppa_row.columns and col != 'index':
                ppa_row[col] = ''
        
        # Concatenate the PPA row with the existing dataframe
        combined_transposed = pd.concat([combined_transposed, ppa_row], ignore_index=True)

    # Set the index to the product names
    combined_transposed.set_index('index', inplace=True)
    combined_transposed.index.name = None

    # Prepare summary DataFrame with index 'Summary'
    summary_df = summary.copy()
    summary_df.index = ['Summary']

    # Concatenate summary_df and combined_transposed
    combined_df = pd.concat([summary_df, combined_transposed], axis=0)

    return combined_df

def calculate_mwh_volumes(products, quarters, is_peaks, future_volumes):
    """
    Calculate MWh volumes for each product and quarter.

    Parameters:
    - products: list of product identifiers (e.g., ['1', '2', '3', '4', 'Y'])
    - quarters: list of quarter identifiers for each hour (e.g., ['1', '1', '2', '2', ...])
    - is_peaks: list indicating if each hour is peak (1) or not (0)
    - future_volumes: DataFrame with MW volumes for each product and quarter

    Returns:
    - mwh_volumes_df: DataFrame with MWh volumes for each product and quarter
    """
    n_hours = len(quarters)

    # Initialize base and peak hours counters for each product
    base_hours = {product: 0 for product in products}
    peak_hours = {product: 0 for product in products}

    # Count the number of base and peak hours for each quarter
    for i in range(n_hours):
        q = str(quarters[i])
        base_hours[q] += 1
        if is_peaks[i]:
            peak_hours[q] += 1

    # For yearly product 'Y', total hours are the total hours counted
    base_hours['Y'] = n_hours
    peak_hours['Y'] = sum(is_peaks)

    # Initialize the result DataFrame with the same structure as future_volumes, but zeroed
    mwh_volumes_df = future_volumes.copy()
    mwh_volumes_df.iloc[:, :] = 0  # Set all values to 0

    # Calculate the MWh volume by multiplying MW volumes by the corresponding hours
    for product in future_volumes.index:
        for quarter in future_volumes.columns:
            mw_value = future_volumes.at[product, quarter]
            if mw_value != 0:
                if product == 'base':
                    hours = base_hours[quarter]
                elif product == 'peak':
                    hours = peak_hours[quarter]
                else:
                    hours = 0  # Should not happen, but just in case
                mwh_volume = hours * mw_value
                mwh_volumes_df.at[product, quarter] = mwh_volume

    return mwh_volumes_df

def adjust_min_tranche(values_dict, min_tranche_size):
    """
    Adjust the tranches in values_dict so that if after subtracting from quarterly tranches any of them is
    smaller than min_tranche_size and not zero, add min_tranche_size to all of them and subtract the total
    added amount from the yearly tranche.
    """
    products = ['1', '2', '3', '4']
    num_quarters = len(products)

    # Check if any value in values_dict is negative. If yes change its value to 0
    for p in values_dict:
        if values_dict[p] < 0:
            values_dict[p] = 0

    # Check if any quarterly tranche is less than min_tranche_size and greater than zero
    need_adjustment = any(0 < values_dict[p] < min_tranche_size for p in products)

    while need_adjustment and values_dict['Y'] > 0:


        if values_dict['Y'] >= min_tranche_size:
            # Enough in 'Y' to adjust
            for p in products:
                values_dict[p] += min_tranche_size
            values_dict['Y'] -= min_tranche_size
        else:
            # Not enough in 'Y', adjust as much as possible
            increment_per_quarter = values_dict['Y']
            for p in products:
                values_dict[p] += increment_per_quarter
            values_dict['Y'] = 0

        # Recheck if any quarterly tranche is less than min_tranche_size and greater than zero
        need_adjustment = any(0 < values_dict[p] < min_tranche_size for p in products)

    # if any value is still smaller than min_tranche_size, make it 0
    for p in products:
        if values_dict[p] < min_tranche_size:
            values_dict[p] = 0

    return values_dict