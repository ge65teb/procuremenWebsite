import pandas as pd
import numpy as np
from datetime import timedelta
from sklearn.metrics.pairwise import cosine_similarity

def read_stats_input(file_path):
    """
    Read the stats input file with Terminmarkt and PPA volumes.
    
    Parameters:
    file_path (str): Path to the CSV file with stats input data.
    
    Returns:
    dict: Dictionary containing the additional statistics.
    """
    # Load the stats input data
    print("Loading stats input data...")
    df = pd.read_csv(file_path, index_col=0, decimal=',')  # Use comma as decimal separator
    
    # Ensure all numeric columns are converted properly
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Extract Terminmarkt volumes
    terminmarkt_volumes = {
        "Terminmarkt Q1 Base": df.loc["Q1", "Base (MWh)"] if "Q1" in df.index else 0,
        "Terminmarkt Q1 Peak": df.loc["Q1", "Peak (MWh)"] if "Q1" in df.index else 0,
        "Terminmarkt Q2 Base": df.loc["Q2", "Base (MWh)"] if "Q2" in df.index else 0,
        "Terminmarkt Q2 Peak": df.loc["Q2", "Peak (MWh)"] if "Q2" in df.index else 0,
        "Terminmarkt Q3 Base": df.loc["Q3", "Base (MWh)"] if "Q3" in df.index else 0,
        "Terminmarkt Q3 Peak": df.loc["Q3", "Peak (MWh)"] if "Q3" in df.index else 0,
        "Terminmarkt Q4 Base": df.loc["Q4", "Base (MWh)"] if "Q4" in df.index else 0,
        "Terminmarkt Q4 Peak": df.loc["Q4", "Peak (MWh)"] if "Q4" in df.index else 0,
        "Terminmarkt Y Base": df.loc["Y", "Base (MWh)"] if "Y" in df.index else 0,
        "Terminmarkt Y Peak": df.loc["Y", "Peak (MWh)"] if "Y" in df.index else 0
    }
    
    # Calculate total Terminmarkt volume
    total_terminmarkt = 0
    for key, value in terminmarkt_volumes.items():
        if not pd.isna(value):
            total_terminmarkt += value
    
    # Extract PPA volumes
    ppa_volumes = {
        "PPA Total": df.loc["PPA", "Base (MWh)"] if "PPA" in df.index else 0,
        "PPA Solar": df.loc["Solar", "Base (MWh)"] if "Solar" in df.index else 0,
        "PPA Wind": df.loc["Wind", "Base (MWh)"] if "Wind" in df.index else 0
    }
    
    # Extract additional statistics that were previously calculated
    additional_metrics = {
        "Total load": df.loc["total_consumption", "Base (MWh)"] if "total_consumption" in df.index else 0,
        "Profile cost": df.loc["profile cost", "Base (MWh)"] if "profile cost" in df.index else 0,
        "Spot bought": df.loc["spot", "Base (MWh)"] if "spot" in df.index else 0,
        "Total Terminmarkt": total_terminmarkt
    }
    
    # Combine all stats
    additional_stats = {}
    additional_stats.update(terminmarkt_volumes)
    additional_stats.update(ppa_volumes)
    additional_stats.update(additional_metrics)
    
    return additional_stats

def calculate_statistics(file_path):
    """
    Calculate various statistics from the hourly results data.
    
    Parameters:
    file_path (str): Path to the CSV file with hourly data.
    
    Returns:
    dict: Dictionary containing the calculated statistics.
    """
    # Load the data
    print("Loading hourly data...")
    df = pd.read_csv(file_path, decimal=',')  # Use comma as decimal separator
    
    # Convert necessary columns to numeric to ensure they're not strings
    numeric_columns = ['Spot_Price', 'Load', 'Spot_Volume', 'hedge_volume', 'Solar_Volume', 'Wind_Volume', 'Solar_Dist', 'Wind_Dist', 'Is_Peak']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert timestamp to datetime
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    
    # Sort by timestamp to ensure correct calculations
    df = df.sort_values('Timestamp')
    
    # Calculate total load
    print("Calculating Total load...")
    total_load = df['Load'].sum()/1000
    
    # 1. Calculate Profile cost
    # (average spot price weighted by load / average spot price) - 1
    print("Calculating Profile cost...")
    weighted_avg_spot = (df['Spot_Price'] * df['Load']).sum() / df['Load'].sum()
    avg_spot = df['Spot_Price'].mean()
    profile_cost = (weighted_avg_spot / avg_spot) - 1
    
    # 2. Calculate load predictability
    # average of (current load - load 1 week ago) / current load
    print("Calculating Load predictability...")
    # Create a column with load from 1 week ago
    df['Load_week_ago'] = df['Load'].shift(168)  # 168 hours = 1 week
    # Calculate predictability only for valid rows (after first week)
    valid_rows = df.dropna(subset=['Load_week_ago'])

    # Ensure load values are at least 1 for the predictability calculation
    valid_rows['Load_for_calc'] = valid_rows['Load'].apply(lambda x: max(1.0, x))
    load_predictability = 1 - (abs(valid_rows['Load'] - valid_rows['Load_week_ago']) / valid_rows['Load_for_calc']).mean()
    
    # 3. Calculate Peak fraction
    # sum of load when isPeak = 1 / the sum of total load
    print("Calculating Peak fraction...")
    peak_load_sum = df.loc[df['Is_Peak'] == 1, 'Load'].sum()
    total_load_sum = df['Load'].sum()
    peak_fraction = peak_load_sum / total_load_sum
    
    # 4. Calculate Solar fit using cosine similarity
    print("Calculating Solar fit...")
    # Reshape vectors for cosine_similarity function
    load_vector = df['Load'].values.reshape(1, -1)
    solar_vector = df['Solar_Dist'].values.reshape(1, -1)
    # Calculate cosine similarity
    solar_fit = cosine_similarity(load_vector, solar_vector)[0][0]
    
    # 5. Calculate Wind fit using cosine similarity
    print("Calculating Wind fit...")
    # Reshape vectors for cosine_similarity function
    wind_vector = df['Wind_Dist'].values.reshape(1, -1)
    # Calculate cosine similarity
    wind_fit = cosine_similarity(load_vector, wind_vector)[0][0]
    
    # 6. Calculate Spot Energy bought and sold (absolute values)
    print("Calculating Spot Energy bought and sold...")
    df['Spot_Volume_2'] = df['Load'] - df['hedge_volume'] - df['Solar_Volume'] - df['Wind_Volume']
    # Calculate sum of positive spot volumes (in GWh)
    spot_bought = df['Spot_Volume_2'].apply(lambda x: max(0, x)).sum() / 1000
    # Calculate sum of negative spot volumes (in GWh)
    spot_sold = -df['Spot_Volume_2'].apply(lambda x: min(0, x)).sum() / 1000
    
    
    # Return results
    return {
        "Total load": total_load,
        "Profile cost": profile_cost,
        "Load predictability": load_predictability,
        "Peak fraction": peak_fraction,
        "Solar fit": solar_fit,
        "Wind fit": wind_fit,
        "Spot bought": spot_bought,
        "Spot sold": spot_sold
    }

if __name__ == "__main__":
    # Calculate statistics from hourly data
    hourly_file = "input/hourly_results.csv"
    results = calculate_statistics(hourly_file)
    
    # Read additional statistics from stats input file
    stats_input_file = "input/stats_input.csv"
    additional_stats = read_stats_input(stats_input_file)
    
    # Combine all results
    results.update(additional_stats)
    
    # Print all results
    print("\nResults:")
    print("\nMarket Profile Statistics:")
    print(f"Total load: {results['Total load']:.3f} GWh")
    print(f"Profile cost: {results['Profile cost']:.6f}")
    print(f"Load predictability: {results['Load predictability']:.6f}")
    print(f"Peak fraction: {results['Peak fraction']:.6f}")
    print(f"Solar fit: {results['Solar fit']:.6f}")
    print(f"Wind fit: {results['Wind fit']:.6f}")
    print(f"Spot bought: {results['Spot bought']:.3f} GWh")
    print(f"Spot sold: {results['Spot sold']:.3f} GWh")
    
    print("\nTerminmarkt Volumes:")
    print(f"Q1 Base: {results['Terminmarkt Q1 Base']:.3f} GWh")
    print(f"Q1 Peak: {results['Terminmarkt Q1 Peak']:.3f} GWh")
    print(f"Q2 Base: {results['Terminmarkt Q2 Base']:.3f} GWh")
    print(f"Q2 Peak: {results['Terminmarkt Q2 Peak']:.3f} GWh")
    print(f"Q3 Base: {results['Terminmarkt Q3 Base']:.3f} GWh")
    print(f"Q3 Peak: {results['Terminmarkt Q3 Peak']:.3f} GWh")
    print(f"Q4 Base: {results['Terminmarkt Q4 Base']:.3f} GWh")
    print(f"Q4 Peak: {results['Terminmarkt Q4 Peak']:.3f} GWh")
    print(f"Year Base: {results['Terminmarkt Y Base']:.3f} GWh")
    print(f"Year Peak: {results['Terminmarkt Y Peak']:.3f} GWh")
    
    print("\nPPA Volumes:")
    print(f"Total PPA: {results['PPA Total']:.3f} GWh")
    print(f"Solar PPA: {results['PPA Solar']:.3f} GWh")
    print(f"Wind PPA: {results['PPA Wind']:.3f} GWh")
