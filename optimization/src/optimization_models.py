"""
Optimization Models Module

Contains the actual optimization models:
- Max Hedge Model: Optimize electricity costs with PPA and futures hedging
- Sell Limit Model: Optimize with selling limits and futures hedging

These models use Pyomo for mathematical optimization and IPOPT solver.
"""

import pandas as pd
import numpy as np
from pyomo.environ import *
from pyomo.opt import SolverFactory, TerminationCondition
import os

# Import the original functions module
import sys
sys.path.append('/Users/piotrek/PycharmProjects/WIP_hedger_notebook/src')
import functions


# ==================== MAX HEDGE MODEL ====================

def electricity_cost_model(loads, spot_prices, is_peaks, quarters,
                           hedge_fraction, ppa_fraction, solar_dist, wind_dist, solar_ppa_price, wind_ppa_price, min_tranche_size, futures_price, output_filename, year):
    """
    Optimize electricity costs with PPA and futures hedging.
    This is the original max hedge model from the reference implementation.
    """
    print("Creating optimization model...")
    
    # Create model
    model = ConcreteModel()
    
    # Define sets
    n_hours = len(loads)
    model.hours = RangeSet(0, n_hours - 1)
    
    # Define products and quarters
    products = ['1', '2', '3', '4']
    model.products = Set(initialize=products)
    model.quarters = Set(initialize=products)
    
    # Define parameters  
    model.loads = Param(model.hours, initialize=dict(enumerate(loads)), within=NonNegativeReals)
    model.spot_price = Param(model.hours, initialize=dict(enumerate(spot_prices)), within=Reals)
    model.is_peak = Param(model.hours, initialize=dict(enumerate(is_peaks)), within=Binary)
    model.quarter = Param(model.hours, initialize=dict(enumerate(quarters)), within=Any)
    
    # PPA distributions
    model.solar_dist = Param(model.hours, initialize=dict(enumerate(solar_dist)), within=NonNegativeReals)
    model.wind_dist = Param(model.hours, initialize=dict(enumerate(wind_dist)), within=NonNegativeReals)
    
    # Futures prices
    model.futures_price = Param(model.quarters, ['base', 'peak'], initialize=dict(
        ((q, 'base'), futures_price.loc[q, 'base']) for q in products
    ) | dict(
        ((q, 'peak'), futures_price.loc[q, 'peak']) for q in products
    ))
    
    # Decision variables
    model.B = Var(model.products, domain=NonNegativeReals)
    model.P = Var(model.products, domain=NonNegativeReals)
    model.W = Var(domain=NonNegativeReals)
    model.S = Var(domain=NonNegativeReals)
    
    # Derived variables for each hour
    model.base_volume = Var(model.hours, domain=Reals)
    model.peak_volume = Var(model.hours, domain=Reals)
    model.hedge_volume = Var(model.hours, domain=Reals)
    model.wind_volume = Var(model.hours, domain=Reals)
    model.solar_volume = Var(model.hours, domain=Reals)
    model.spot_volume = Var(model.hours, domain=Reals)
    
    # Cost variables
    model.hedge_cost = Var(model.hours, domain=Reals)
    model.spot_cost = Var(model.hours, domain=Reals)
    model.wind_cost = Var(model.hours, domain=Reals)
    model.solar_cost = Var(model.hours, domain=Reals)
    
    # Total volumes
    model.total_wind_volume = Var(domain=NonNegativeReals)
    model.total_solar_volume = Var(domain=NonNegativeReals)
    
    # Constraints
    def volume_balance_rule(m, t):
        q = m.quarter[t]
        return (m.spot_volume[t] == m.loads[t] - m.hedge_volume[t] - 
                m.wind_volume[t] - m.solar_volume[t])
    model.volume_balance = Constraint(model.hours, rule=volume_balance_rule)
    
    def base_volume_rule(m, t):
        q = m.quarter[t]
        return m.base_volume[t] == m.B[q]
    model.base_volume_constraint = Constraint(model.hours, rule=base_volume_rule)
    
    def peak_volume_rule(m, t):
        q = m.quarter[t]
        return m.peak_volume[t] == m.P[q] * m.is_peak[t]
    model.peak_volume_constraint = Constraint(model.hours, rule=peak_volume_rule)
    
    def hedge_volume_rule(m, t):
        return m.hedge_volume[t] == m.base_volume[t] + m.peak_volume[t]
    model.hedge_volume_constraint = Constraint(model.hours, rule=hedge_volume_rule)
    
    def wind_volume_rule(m, t):
        return m.wind_volume[t] == m.W * m.wind_dist[t] * 1000
    model.wind_volume_constraint = Constraint(model.hours, rule=wind_volume_rule)
    
    def solar_volume_rule(m, t):
        return m.solar_volume[t] == m.S * m.solar_dist[t] * 1000
    model.solar_volume_constraint = Constraint(model.hours, rule=solar_volume_rule)
    
    def total_wind_volume_rule(m):
        return m.total_wind_volume == sum(m.wind_volume[t] for t in m.hours)
    model.total_wind_volume_constraint = Constraint(rule=total_wind_volume_rule)
    
    def total_solar_volume_rule(m):
        return m.total_solar_volume == sum(m.solar_volume[t] for t in m.hours)
    model.total_solar_volume_constraint = Constraint(rule=total_solar_volume_rule)
    
    # Cost constraints
    def hedge_cost_rule(m, t):
        q = m.quarter[t]
        return (m.hedge_cost[t] == m.base_volume[t] * m.futures_price[q, 'base'] + 
                m.peak_volume[t] * m.futures_price[q, 'peak'])
    model.hedge_cost_constraint = Constraint(model.hours, rule=hedge_cost_rule)
    
    def spot_cost_rule(m, t):
        return m.spot_cost[t] == m.spot_volume[t] * m.spot_price[t]
    model.spot_cost_constraint = Constraint(model.hours, rule=spot_cost_rule)
    
    def wind_cost_rule(m, t):
        return m.wind_cost[t] == m.wind_volume[t] * wind_ppa_price
    model.wind_cost_constraint = Constraint(model.hours, rule=wind_cost_rule)
    
    def solar_cost_rule(m, t):
        return m.solar_cost[t] == m.solar_volume[t] * solar_ppa_price
    model.solar_cost_constraint = Constraint(model.hours, rule=solar_cost_rule)
    
    # Hedge fraction constraint
    def ppa_to_futures_hedge_ratio_rule(m):
        return sum(m.loads[t] for t in m.hours) * (ppa_fraction + hedge_fraction) == (
            sum(m.hedge_volume[t] for t in m.hours) + m.total_wind_volume + m.total_solar_volume)
    model.ppa_to_futures_hedge_ratio = Constraint(rule=ppa_to_futures_hedge_ratio_rule)
    
    def hedged_volume_rule(m):
        return sum(m.hedge_volume[t] for t in m.hours) == sum(m.loads[t] for t in m.hours) * hedge_fraction
    model.hedged_volume = Constraint(rule=hedged_volume_rule)
    
    # Objective
    def objective_rule(m):
        return sum(m.hedge_cost[t] + m.spot_cost[t] + m.wind_cost[t] + m.solar_cost[t] for t in m.hours)
    model.objective = Objective(rule=objective_rule, sense=minimize)
    
    # Solve
    solver = SolverFactory('ipopt')
    solution = solver.solve(model, tee=True)
    
    # Need to handle case where ppa_fraction is 0
    actual_fraction_multiplier = ppa_fraction + hedge_fraction
    
    # Post-processing
    try:
        # Extract optimized quarterly B and P values
        B_values = {p: value(model.B[p]) * actual_fraction_multiplier for p in products}
        P_values = {p: value(model.P[p]) * actual_fraction_multiplier for p in products}

        # Calculate minimum values of B and P
        B_min = min(B_values.values())
        P_min = min(P_values.values())

        # Set yearly tranches 'Y' to B_min and P_min
        B_values['Y'] = B_min
        P_values['Y'] = P_min

        # Adjust quarterly B[p] and P[p] by subtracting B_min and P_min
        for p in products:
            B_values[p] -= B_min
            P_values[p] -= P_min

        # Adjust tranches to meet min_tranche_size
        B_values = functions.adjust_min_tranche(B_values, min_tranche_size)
        P_values = functions.adjust_min_tranche(P_values, min_tranche_size)

        # Get total volumes of wind and solar
        total_wind_volume = value(model.total_wind_volume)
        total_solar_volume = value(model.total_solar_volume)
        
        # Check if either is below threshold (600) and adjust if needed
        W_adjusted = value(model.W) * actual_fraction_multiplier
        S_adjusted = value(model.S) * actual_fraction_multiplier
        
        if total_wind_volume < 1000 or total_solar_volume < 1000:
            if total_wind_volume < total_solar_volume:
                # Wind is smaller, add it to solar and set to zero
                S_adjusted += W_adjusted
                W_adjusted = 0
            else:
                # Solar is smaller, add it to wind and set to zero
                W_adjusted += S_adjusted
                S_adjusted = 0

        # Update volumes and costs for each hour
        base_volume = []
        peak_volume = []
        hedge_volume = []
        spot_volume = []
        hedge_cost = []
        spot_cost = []
        loads_list = []

        for t in model.hours:
            q = value(model.quarter[t])
            load = value(model.loads[t])
            is_peak = value(model.is_peak[t])

            # Calculate volumes
            base_vol = B_values['Y'] + B_values[str(int(q))]
            peak_vol = (P_values['Y'] + P_values[str(int(q))]) * is_peak
            hedge_vol = base_vol + peak_vol
            spot_vol = load - hedge_vol - value(model.solar_dist[t]) * S_adjusted * 1000 - value(model.wind_dist[t]) * W_adjusted * 1000  

            # Get prices
            base_price = value(model.futures_price[str(int(q)), 'base'])
            peak_price = value(model.futures_price[str(int(q)), 'peak'])
            spot_price = value(model.spot_price[t])

            # Calculate costs
            base_c = base_vol * base_price
            peak_c = peak_vol * peak_price
            hedge_c = base_c + peak_c
            spot_c = spot_vol * spot_price

            # Append to lists
            base_volume.append(base_vol)
            peak_volume.append(peak_vol)
            hedge_volume.append(hedge_vol)
            spot_volume.append(spot_vol)
            hedge_cost.append(hedge_c)
            spot_cost.append(spot_c)
            loads_list.append(load)

        # Create hourly results DataFrame
        hourly_results = pd.DataFrame({
            'Timestamp': pd.date_range(start=f'{year}-01-01', periods=len(model.hours), freq='H'),
            'hedge_volume': hedge_volume,
            'Load': loads_list,
            'Spot_Volume': spot_volume,
            'Spot_Price': [value(model.spot_price[t]) for t in model.hours],
            'Wind_Volume': [value(model.wind_dist[t]*W_adjusted*1000) for t in model.hours],
            'Solar_Volume': [value(model.solar_dist[t]*S_adjusted*1000) for t in model.hours],
            'Wind_Cost': [value(model.wind_volume[t]) * wind_ppa_price for t in model.hours],
            'Solar_Cost': [value(model.solar_volume[t]) * solar_ppa_price for t in model.hours],
            'Base_Cost': [value(model.base_volume[t]) * value(model.futures_price[str(int(value(model.quarter[t]))), 'base']) for t in model.hours],
            'Peak_Cost': [value(model.peak_volume[t]) * value(model.futures_price[str(int(value(model.quarter[t]))), 'peak']) for t in model.hours],
            'Hedge_Cost': [value(model.hedge_cost[t]) for t in model.hours],
            'Spot_Cost': [value(model.spot_cost[t]) for t in model.hours],
            'Is_Peak': [value(model.is_peak[t]) for t in model.hours],
            'Solar_Dist': [value(model.solar_dist[t]*sum(loads_list)) for t in model.hours],
            'Wind_Dist': [value(model.wind_dist[t]*sum(loads_list)) for t in model.hours]
        })

        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
            hourly_results.to_csv(output_filename, index=False)
            print(f"Hourly results saved to {output_filename}")
        except Exception as e:
            print(f"Error saving hourly results: {e}")

        # Summary calculations
        net_spot_volume = sum(spot_volume)
        abs_spot_volume = sum(abs(vol) for vol in spot_volume)
        spot_buy_volume = sum(vol for vol in spot_volume if vol > 0)
        spot_sell_volume = -sum(vol for vol in spot_volume if vol < 0)
        hedge_volume_total = sum(hedge_volume)

        avg_buy_spot_price = (
            sum(cost for vol, cost in zip(spot_volume, spot_cost) if vol > 0) / spot_buy_volume
            if spot_buy_volume != 0 else 0)
        avg_sell_spot_price = (
            sum(cost for vol, cost in zip(spot_volume, spot_cost) if vol < 0) / -spot_sell_volume
            if spot_sell_volume != 0 else 0)
        avg_spot_price = sum(spot_cost) / net_spot_volume if net_spot_volume != 0 else 0
        avg_hedge_price = sum(hedge_cost) / hedge_volume_total if hedge_volume_total != 0 else 0

        summary = {
            'net_spot_volume': net_spot_volume,
            'abs_spot_volume': abs_spot_volume,
            'avg_spot_price': avg_spot_price,
            'spot_buy_volume': spot_buy_volume,
            'avg_buy_spot_price': avg_buy_spot_price,
            'spot_sell_volume': spot_sell_volume,
            'avg_sell_spot_price': avg_sell_spot_price,
            'hedge_volume': hedge_volume_total,
            'avg_hedge_price': avg_hedge_price
        }
        summary = pd.DataFrame([summary])

        # Prepare futures_volumes DataFrame
        base_futures_volumes = {p: B_values[p] for p in products + ['Y']}
        peak_futures_volumes = {p: P_values[p] for p in products + ['Y']}
        futures_volumes = pd.DataFrame(
            [base_futures_volumes, peak_futures_volumes], index=['base', 'peak'])
        
        # Print W and S values
        print(f"W: {value(model.W)}, S: {value(model.S)}")
        
        # Prepare PPA_volumes DataFrame
        ppa_volumes = pd.DataFrame({'W': [W_adjusted],
                                    'S': [S_adjusted]
                                    }, index=['W', 'S'])

        # Calculate MWh volumes 
        products_with_Y = products + ['Y']
        mwh_volumes = functions.calculate_mwh_volumes(
            products_with_Y, quarters, is_peaks, futures_volumes)

        # Combine results
        results = functions.combine_dataframes(summary, mwh_volumes, futures_volumes, ppa_volumes)

        if solution.solver.termination_condition == TerminationCondition.optimal:
            status = 1  # Success
        else:
            status = 0  # Infeasible or other non-optimal termination
    except Exception as e:
        print("Error during result processing:", e)
        status = 0
        results = None

    return status, results


def run_max_hedge_simulation(df, tranche_size, hedge_fraction, ppa_fraction, solar_ppa_price, wind_ppa_price, futures_price, output_filename, year):
    """Run the max hedge simulation."""
    loads = df['Load'].values
    prices = df['price'].values
    is_peaks = df['isPeak'].values
    quarters = df['Q'].values
    wind_dist = df['Wind'].values
    solar_dist = df['Solar'].values
    status = 0

    status, results = electricity_cost_model(loads, prices, is_peaks, quarters,
                                             hedge_fraction, ppa_fraction, solar_dist, wind_dist, solar_ppa_price, wind_ppa_price,
                                             tranche_size, futures_price, output_filename, year)

    print('Max hedge optimization, for hedge fraction of ', hedge_fraction, 'and ppa fraction of ', ppa_fraction, 'completed')
    return results


# ==================== COMPATIBILITY FUNCTIONS ====================

def run_max_hedge_model(df, min_tranche_size, hedge_fraction, ppa_hedge_fraction, 
                       solar_ppa_price, wind_ppa_price, futures, hourly_results_filename, hedge_year):
    """Compatibility function for original interface."""
    return run_max_hedge_simulation(df, min_tranche_size, hedge_fraction, ppa_hedge_fraction, 
                                   solar_ppa_price, wind_ppa_price, futures, hourly_results_filename, hedge_year) 