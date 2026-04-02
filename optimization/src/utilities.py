"""
Utilities Module

Contains helper functions and utilities:
- Result formatting and saving
- Data processing utilities
- Mathematical calculations
- File I/O operations
"""

import pandas as pd
from pathlib import Path

import config


# ==================== RESULT FORMATTING ====================

def save_combined_results_to_csv(results_max_hedge, results_sell_limit, filename, profile_cost, futures, avg_price):
    """
    Use the original save function from the reference implementation.
    This ensures exact compatibility with the original output format.
    """
    # Import the original functions module
    import sys
    sys.path.append('/Users/piotrek/PycharmProjects/WIP_hedger_notebook/src')
    import functions
    
    # Use the original function
    return functions.save_combined_results_to_csv(
        results_max_hedge, results_sell_limit, filename, profile_cost, futures, avg_price
    )


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

    # Count base and peak hours for each product
    for t in range(n_hours):
        quarter = quarters[t]
        is_peak = is_peaks[t]
        
        if quarter in products:
            base_hours[quarter] += 1
            if is_peak:
                peak_hours[quarter] += 1

    # Calculate MWh volumes
    mwh_data = {}
    for product in products:
        if product in future_volumes.index:
            base_mw = future_volumes.loc[product, 'base'] if 'base' in future_volumes.columns else 0
            peak_mw = future_volumes.loc[product, 'peak'] if 'peak' in future_volumes.columns else 0
            
            base_mwh = base_mw * base_hours[product]
            peak_mwh = peak_mw * peak_hours[product]
            
            mwh_data[product] = {
                'base': base_mwh,
                'peak': peak_mwh,
                'total': base_mwh + peak_mwh
            }

    # Create DataFrame
    mwh_volumes_df = pd.DataFrame(mwh_data).T
    mwh_volumes_df.index.name = 'Product'
    
    return mwh_volumes_df


def adjust_min_tranche(values_dict, min_tranche_size):
    """
    Adjust tranche values to meet minimum tranche size requirements.
    
    Parameters:
    - values_dict: Dictionary of tranche values
    - min_tranche_size: Minimum allowed tranche size
    
    Returns:
    - adjusted_dict: Dictionary with adjusted values
    """
    adjusted_dict = values_dict.copy()
    
    # Find values below minimum threshold
    small_tranches = {k: v for k, v in adjusted_dict.items() if 0 < v < min_tranche_size}
    
    if not small_tranches:
        return adjusted_dict
    
    # For small tranches, either set to 0 or increase to minimum
    for key, value in small_tranches.items():
        if value < min_tranche_size / 2:
            # If very small, set to 0
            adjusted_dict[key] = 0
        else:
            # If close to minimum, increase to minimum
            adjusted_dict[key] = min_tranche_size
    
    return adjusted_dict


# ==================== DATA VALIDATION ====================

def validate_optimization_inputs(df, futures_data, parameters):
    """
    Validate inputs for optimization models.
    
    Parameters:
    - df: Input DataFrame with load data
    - futures_data: Futures price data
    - parameters: Dictionary of optimization parameters
    
    Returns:
    - validation_results: Dictionary with validation status and issues
    """
    issues = []
    
    # Check DataFrame structure
    required_columns = ['Load']
    if 'Wind' in df.columns and 'Solar' in df.columns:
        required_columns.extend(['Wind', 'Solar'])
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        issues.append(f"Missing required columns: {missing_columns}")
    
    # Check for datetime index
    if not isinstance(df.index, pd.DatetimeIndex):
        issues.append("DataFrame index must be datetime")
    
    # Check data completeness
    if 'Load' in df.columns:
        if df['Load'].isna().sum() > 0:
            issues.append("Missing load values detected")
        if (df['Load'] < 0).sum() > 0:
            issues.append("Negative load values detected")
    
    # Check futures data
    if futures_data is None or futures_data.empty:
        issues.append("No futures data provided")
    else:
        required_futures_cols = ['base', 'peak']
        missing_futures_cols = [col for col in required_futures_cols if col not in futures_data.columns]
        if missing_futures_cols:
            issues.append(f"Missing futures columns: {missing_futures_cols}")
    
    # Check parameters
    required_params = ['hedge_fraction', 'min_tranche_size']
    missing_params = [param for param in required_params if param not in parameters]
    if missing_params:
        issues.append(f"Missing parameters: {missing_params}")
    
    # Check parameter ranges
    if 'hedge_fraction' in parameters:
        if not 0 <= parameters['hedge_fraction'] <= 1:
            issues.append("Hedge fraction must be between 0 and 1")
    
    if 'ppa_fraction' in parameters:
        if not 0 <= parameters['ppa_fraction'] <= 1:
            issues.append("PPA fraction must be between 0 and 1")
    
    return {
        'is_valid': len(issues) == 0,
        'issues': issues,
        'data_shape': df.shape,
        'date_range': (df.index.min(), df.index.max()) if isinstance(df.index, pd.DatetimeIndex) else None
    }


# ==================== FILE OPERATIONS ====================

def ensure_directory_exists(path):
    """Ensure a directory exists, create if it doesn't."""
    Path(path).mkdir(parents=True, exist_ok=True)


def get_file_list(directory, extension='.csv'):
    """Get list of files with specified extension in directory."""
    directory = Path(directory)
    if not directory.exists():
        return []
    
    return [f for f in directory.iterdir() if f.suffix.lower() == extension.lower()]


def archive_file(source_path, archive_dir, prefix=''):
    """Archive a file to the archive directory with optional prefix."""
    source_path = Path(source_path)
    archive_dir = Path(archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    archive_filename = f"{prefix}{source_path.name}" if prefix else source_path.name
    archive_path = archive_dir / archive_filename
    
    import shutil
    shutil.copy2(source_path, archive_path)
    return archive_path


# ==================== MATHEMATICAL UTILITIES ====================

def calculate_weighted_average(values, weights):
    """Calculate weighted average."""
    if len(values) != len(weights):
        raise ValueError("Values and weights must have the same length")
    
    total_weight = sum(weights)
    if total_weight == 0:
        return 0
    
    weighted_sum = sum(v * w for v, w in zip(values, weights))
    return weighted_sum / total_weight


def calculate_percentage_change(old_value, new_value):
    """Calculate percentage change between two values."""
    if old_value == 0:
        return float('inf') if new_value != 0 else 0
    return ((new_value - old_value) / old_value) * 100


# ==================== DISPLAY UTILITIES ====================

def format_currency(value, currency='EUR'):
    """Format value as currency."""
    if pd.isna(value):
        return 'N/A'
    return f"{value:,.2f} {currency}"


def format_percentage(value, decimal_places=2):
    """Format value as percentage."""
    if pd.isna(value):
        return 'N/A'
    return f"{value:.{decimal_places}%}"


def format_volume(value, unit='MWh'):
    """Format volume with appropriate units."""
    if pd.isna(value):
        return 'N/A'
    
    if abs(value) >= 1000:
        return f"{value/1000:.1f} G{unit}"
    elif abs(value) >= 1:
        return f"{value:.1f} {unit}"
    else:
        return f"{value*1000:.1f} k{unit}"


def create_summary_table(data_dict, title="Summary"):
    """Create a formatted summary table from dictionary."""
    df = pd.DataFrame(list(data_dict.items()), columns=['Metric', 'Value'])
    df.index = range(1, len(df) + 1)
    return df


# ==================== PROGRESS TRACKING ====================

def create_progress_tracker():
    """Create a simple progress tracker for notebook usage."""
    class ProgressTracker:
        def __init__(self):
            self.steps = []
            self.current_step = 0
            
        def add_step(self, name, description=""):
            self.steps.append({'name': name, 'description': description, 'completed': False})
            
        def complete_step(self, step_name):
            for step in self.steps:
                if step['name'] == step_name:
                    step['completed'] = True
                    break
            self.current_step += 1
            
        def get_progress(self):
            completed = sum(1 for step in self.steps if step['completed'])
            total = len(self.steps)
            return completed, total, f"{completed}/{total} ({completed/total*100:.1f}%)" if total > 0 else "0/0 (0%)"
            
        def display_status(self):
            completed, total, percentage = self.get_progress()
            print(f"Progress: {percentage}")
            for i, step in enumerate(self.steps):
                status = "✓" if step['completed'] else "○"
                print(f"  {status} {step['name']}")
                if step['description']:
                    print(f"    {step['description']}")
    
    return ProgressTracker() 