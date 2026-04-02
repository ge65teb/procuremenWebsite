import pandas as pd
from pyomo.environ import ConcreteModel, Var, NonNegativeReals, Objective, minimize, Constraint, SolverFactory, value, \
    RangeSet, Param, sqrt, TerminationCondition, Expression, Any
import pandas as pd
import functions
import os


def electricity_cost_model(loads, spot_prices, is_peaks, quarters,
                           hedge_fraction, ppa_fraction, solar_dist, wind_dist, solar_ppa_price, wind_ppa_price, min_tranche_size, futures_price, output_filename, year):
    """
    Optimize the electricity cost model.

    Parameters:
    - loads: array of load values per hour
    - spot_prices: array of spot prices per hour
    - is_peaks: array indicating if each hour is peak (1) or not (0)
    - quarters: array of quarter identifiers for each hour
    - hedge_fraction: desired fraction of load to hedge
    - ppa_fraction: desired fraction of load to hedge with PPA
    - solar_dist: array of solar distribution values per hour
    - wind_dist: array of wind distribution values per hour
    - solar_ppa_price: price of solar PPA
    - wind_ppa_price: price of wind PPA
    - min_tranche_size: minimum allowed tranche size
    - futures_price: DataFrame with futures prices for each product and type
    - output_filename: filename to save hourly results
    - year: year to use for the timestamp range

    Returns:
    - status: 1 if optimization succeeded, 0 otherwise
    - results: DataFrame with summary results
    - results_ts: DataFrame with time series results
    """


    model = ConcreteModel()

    # Constants
    T = len(loads)
    model.hours = RangeSet(0, T - 1)
    products = ['1', '2', '3', '4']  # Quarterly products only
    types = ['base', 'peak']
     

    # Build futures_price_dict only for quarterly products
    futures_price_dict = {}
    for _, row in futures_price.iterrows():
        if row['Period'] in products:
            futures_price_dict[(row['Period'], 'base')] = row['base']
            futures_price_dict[(row['Period'], 'peak')] = row['peak']

    # Parameters
    # Map inputs to Pyomo parameters
    model.is_peak = Param(model.hours, initialize={i: int(is_peaks[i]) for i in range(T)})
    model.quarter = Param(model.hours, initialize={i: str(quarters[i]) for i in range(T)}, within=Any)
    model.loads = Param(model.hours, initialize={i: loads[i] for i in range(T)})
    model.spot_price = Param(model.hours, initialize={i: spot_prices[i] for i in range(T)})
    model.futures_price = Param(products, types, initialize=futures_price_dict)
    model.solar_ppa_price = Param(initialize=solar_ppa_price)
    model.wind_ppa_price = Param(initialize=wind_ppa_price)
    model.wind_dist = Param(model.hours, initialize={i: wind_dist[i] for i in range(T)})
    model.solar_dist = Param(model.hours, initialize={i: solar_dist[i] for i in range(T)})
    model.hedge_fraction = Param(initialize=hedge_fraction, within=NonNegativeReals)
    model.ppa_fraction = Param(initialize=ppa_fraction, within=NonNegativeReals)
    # Calculate the sum of squared loads
    total_squared_loads = sum(loads[i]**2 for i in range(T))
    model.ref_open_volume_squared = Param(initialize=total_squared_loads)
    
    # total consumption
    model.total_consumption = Param(initialize=sum(loads[i] for i in range(T)))
    # Calculate the cost of unhedged electricity
    total_spot_cost = sum(spot_prices[i] * loads[i] for i in range(T))
    model.ref_spot_cost = Param(initialize=total_spot_cost)

    # Total volume of PPA

    # Variables
    # Base and peak MW volumes for each product (quarterly products only)
    model.B = Var(products, domain=NonNegativeReals, bounds=(0, 100), initialize=0)
    model.P = Var(products, domain=NonNegativeReals, bounds=(0, 100), initialize=0)
    model.W = Var(domain=NonNegativeReals, bounds=(0, 100), initialize=0)
    model.S = Var(domain=NonNegativeReals, bounds=(0, 100), initialize=0)

    # Expressions
    # Calculate base and peak volumes for each hour
    model.base_volume = Expression(model.hours, rule=lambda m, t: m.B[m.quarter[t]])
    model.peak_volume = Expression(model.hours, rule=lambda m, t: m.P[m.quarter[t]] * m.is_peak[t])
    model.hedge_volume = Expression(model.hours, rule=lambda m, t: m.base_volume[t] + m.peak_volume[t])
    model.wind_volume = Expression(model.hours, rule=lambda m, t: m.W * m.wind_dist[t] * 1000)  # (TWh to MWh)
    model.solar_volume = Expression(model.hours, rule=lambda m, t: m.S * m.solar_dist[t] * 1000)  # (TWh to MWh)
    model.spot_volume = Expression(model.hours, rule=lambda m, t: m.loads[t] - m.hedge_volume[t] - m.wind_volume[t] - m.solar_volume[t])

    # Define total volumes as expressions (not indexed)

    model.total_hedge_volume = Expression(rule=lambda m: sum(m.hedge_volume[t] for t in m.hours))
    model.total_solar_volume = Expression(rule=lambda m: sum(m.solar_volume[t] for t in m.hours))
    model.total_wind_volume = Expression(rule=lambda m: sum(m.wind_volume[t] for t in m.hours))
    model.total_ppa_volume = Expression(rule=lambda m: sum(m.wind_volume[t] + m.solar_volume[t] for t in m.hours))

    # Enforce PPA and futures ratio
    def ppa_to_futures_hedge_ratio_rule(m):
        return m.total_ppa_volume == m.total_consumption * ppa_fraction/(ppa_fraction + hedge_fraction)
    
    def hedged_volume_rule(m):
        return m.total_hedge_volume == m.total_consumption * hedge_fraction / (ppa_fraction + hedge_fraction)
    
    
    model.ppa_to_futures_hedge_ratio = Constraint(rule=ppa_to_futures_hedge_ratio_rule)
    model.hedged_volume = Constraint(rule=hedged_volume_rule)
    # Negative spot cost (value of electricity sold back to the spot market)
    model.spot_cost = Expression(
        model.hours, rule=lambda m, t: m.spot_price[t] * m.spot_volume[t])
    
    model.hedge_cost = Expression(
        model.hours, rule=lambda m, t: m.futures_price[m.quarter[t], 'base'] * m.base_volume[t] + m.futures_price[m.quarter[t], 'peak'] * m.peak_volume[t])

    model.ppa_cost = Expression(
        model.hours, rule=lambda m, t: m.solar_ppa_price * m.solar_volume[t] + m.wind_ppa_price * m.wind_volume[t])

    # Objective function: minimize total value of electricity sold back to the spot market
    def objective_rule(m):
        total_open_volume_squared = sum(m.spot_volume[t]**2 for t in m.hours)
        total_hedge_cost = sum(m.hedge_cost[t] for t in m.hours)
        total_spot_cost = sum(m.spot_cost[t] for t in m.hours)
        total_ppa_cost = sum(m.ppa_cost[t] for t in m.hours)
        total_cost = total_hedge_cost + total_spot_cost + total_ppa_cost
        objective = total_open_volume_squared/model.ref_open_volume_squared * total_cost/model.ref_spot_cost

        return objective

    model.objective = Objective(rule=objective_rule, sense=minimize)

    # Solver
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
            q = model.quarter[t]
            load = value(model.loads[t])
            is_peak = value(model.is_peak[t])

            # Calculate volumes
            base_vol = B_values['Y'] + B_values[q]
            peak_vol = (P_values['Y'] + P_values[q]) * is_peak
            hedge_vol = base_vol + peak_vol
            spot_vol = load - hedge_vol - model.solar_dist[t] * S_adjusted * 1000 - model.wind_dist[t] * W_adjusted * 1000  

            # Get prices
            base_price = value(model.futures_price[q, 'base'])
            peak_price = value(model.futures_price[q, 'peak'])
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
            'Base_Cost': [value(model.base_volume[t]) * value(model.futures_price[model.quarter[t], 'base']) for t in model.hours],
            'Peak_Cost': [value(model.peak_volume[t]) * value(model.futures_price[model.quarter[t], 'peak']) for t in model.hours],
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



def run_simulation(df, tranche_size, hedge_fraction, ppa_fraction, solar_ppa_price, wind_ppa_price, futures_price, output_filename, year):
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
    # create a list out of spot_costs
    return results
