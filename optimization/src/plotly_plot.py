import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import numpy as np
import stats  # Import the stats module
import base64
import glob
import shutil

def generate_plots(core_path="/content/drive/Shareddrives/Ecoplanet_main/02_Product/07_Procurement/tools/hedger"):
    """
    Generate interactive Plotly plots from hourly results and stats data.
    
    Args:
        core_path (str): Base path for the project directory. Defaults to "./"
    
    Returns:
        dict: Summary of processing results including number of files processed and any errors
    """
    
    # Define directory paths relative to core path
    HOURLY_INPUT_DIR = os.path.join(core_path, "data", "hourly_results")
    STATS_INPUT_DIR = os.path.join(core_path, "stats_input")
    PLOTS_OUTPUT_DIR = os.path.join(core_path, "plots")
    HOURLY_ARCHIVE_DIR = os.path.join(core_path, "archive", "hourly")
    STATS_ARCHIVE_DIR = os.path.join(core_path, "archive", "stats")
    LOGO_PATH = os.path.join(core_path, "data", "logo.png")

    # Create all necessary directories if they don't exist
    os.makedirs(PLOTS_OUTPUT_DIR, exist_ok=True)
    os.makedirs(HOURLY_ARCHIVE_DIR, exist_ok=True)
    os.makedirs(STATS_ARCHIVE_DIR, exist_ok=True)

    # Find all files in the hourly input directory that end with _hourly
    hourly_files = glob.glob(os.path.join(HOURLY_INPUT_DIR, '*_hourly.csv'))
    
    # Initialize result tracking
    result = {
        "files_processed": 0,
        "files_skipped": 0,
        "errors": [],
        "processed_files": [],
        "skipped_files": []
    }

    # Process each file
    for hourly_file_path in hourly_files:
        try:
            # Extract base filename (without _hourly.csv)
            base_filename = os.path.basename(hourly_file_path).replace('_hourly.csv', '')
            print("Processing",base_filename)
            
            # For filenames with multiple parts, extract the company name
            filename_parts = base_filename.split('_')
            
            # Try to intelligently extract company name based on common patterns
            if len(filename_parts) >= 3 and filename_parts[2].isdigit():  # Pattern: DATE_COMPANY_YEAR
                company_name = filename_parts[1]
            elif len(filename_parts) >= 2:  # Pattern: DATE_COMPANY or COMPANY_YEAR
                company_name = filename_parts[1]
            else:  # Only one part, use it as company name
                company_name = base_filename
            
            # Look for stats files with various patterns in the stats input directory
            # First try new pattern: "[ANY]_Load profile analysis_[COMPANY] - stats.csv"
            stats_files = []
            
            # Try pattern with exact company name
            pattern1 = os.path.join(STATS_INPUT_DIR, f"*_Load profile analysis_{company_name} - stats.csv")
            stats_files.extend(glob.glob(pattern1))
            
            # Try pattern with company name anywhere in filename
            pattern2 = os.path.join(STATS_INPUT_DIR, f"*{company_name}*stats.csv")
            stats_files.extend(glob.glob(pattern2))
            
            # Try old pattern
            pattern3 = os.path.join(STATS_INPUT_DIR, f"stats_{company_name}.csv")
            if os.path.exists(pattern3):
                stats_files.append(pattern3)
            
            # If still no matches, try with base filename
            if not stats_files:
                pattern4 = os.path.join(STATS_INPUT_DIR, f"*{base_filename}*stats.csv")
                stats_files.extend(glob.glob(pattern4))
            
            # Use the first valid stats file found
            stats_file_path = None
            for file_path in stats_files:
                if os.path.exists(file_path):
                    stats_file_path = file_path
                    break
            
            # If no stats file found, skip this hourly file
            if not stats_file_path:
                warning_msg = f"Warning: No stats file found for {hourly_file_path}"
                print(warning_msg)
                result["files_skipped"] += 1
                result["skipped_files"].append(hourly_file_path)
                continue
            
            print(f"Processing {base_filename}...")
            print(f"Using stats file: {stats_file_path}")
            
            # Load the hourly results data
            df = pd.read_csv(hourly_file_path, decimal=',')  # Use comma as decimal separator
            
            # Ensure Timestamp is datetime
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            
            # Convert necessary columns to numeric to ensure they're not strings
            numeric_columns = ['Spot_Volume', 'Load', 'hedge_volume', 'Solar_Volume', 'Wind_Volume']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Add 1 hour to all timestamps
            df['Timestamp'] = df['Timestamp'] + pd.Timedelta(hours=1)
            
            # Calculate Spot Volume as max(0, load - hedge - solar - wind)
            df_processed = df.copy()
            df_processed['Spot_Volume_Positive'] = df_processed.apply(
                lambda row: max(0, row['Spot_Volume']),
                axis=1
            )
            
            # For the second plot, also calculate negative spot volumes
            df_processed['Spot_Volume_Negative'] = df_processed.apply(
                lambda row: min(0, row['Spot_Volume']),
                axis=1
            )
            
            # Calculate total volume (including spot)
            df_processed['Total_Volume'] = (df_processed['hedge_volume'] + 
                                           df_processed['Wind_Volume'] + 
                                           df_processed['Solar_Volume'] +
                                           df_processed['Spot_Volume_Positive'])
            
            # Identify rows where total exceeds load
            df_processed['Excess'] = np.maximum(0, df_processed['Total_Volume'] - df_processed['Load'])
            
            # Apply the adjustments in the specified order
            for i, row in df_processed.iterrows():
                excess = row['Excess']
                if excess > 0:
                    # 1. First reduce spot volume
                    spot_reduction = min(excess, df_processed.at[i, 'Spot_Volume_Positive'])
                    df_processed.at[i, 'Spot_Volume_Positive'] -= spot_reduction
                    excess -= spot_reduction
                    
                    # 2. Then reduce wind volume
                    if excess > 0:
                        wind_reduction = min(excess, df_processed.at[i, 'Wind_Volume'])
                        df_processed.at[i, 'Wind_Volume'] -= wind_reduction
                        excess -= wind_reduction
                        
                        # 3. Then reduce solar volume
                        if excess > 0:
                            solar_reduction = min(excess, df_processed.at[i, 'Solar_Volume'])
                            df_processed.at[i, 'Solar_Volume'] -= solar_reduction
                            excess -= solar_reduction
                            
                            # 4. Finally reduce hedge volume
                            if excess > 0:
                                hedge_reduction = min(excess, df_processed.at[i, 'hedge_volume'])
                                df_processed.at[i, 'hedge_volume'] -= hedge_reduction
            
            # Verify the adjustment worked
            df_processed['New_Total'] = (df_processed['hedge_volume'] + 
                                        df_processed['Wind_Volume'] + 
                                        df_processed['Solar_Volume'] +
                                        df_processed['Spot_Volume_Positive'])
            
            # Create figure for volumes
            volume_fig = go.Figure()
            
            # Add positive volume traces as stacked bars - using processed data
            volume_fig.add_trace(go.Bar(
                x=df_processed['Timestamp'],
                y=df_processed['hedge_volume'],
                name='Terminmarktvolumen',
                width=3600000,  # Width in milliseconds (1 hour)
                marker=dict(
                    color='#2280BF',  # Hedge volume color
                    line=dict(width=0)  # No outline
                ),
                opacity=0.9,
                hovertemplate='<b>Terminmarktvolumen</b>: %{y:.1f} MWh<extra></extra>'
            ))
            volume_fig.add_trace(go.Bar(
                x=df_processed['Timestamp'],
                y=df_processed['Wind_Volume'],
                name='Windvolumen',
                width=3600000,
                marker=dict(
                    color='#206F6D',  # Wind volume color
                    line=dict(width=0)  # No outline
                ),
                opacity=0.9,
                hovertemplate='<b>Windvolumen</b>: %{y:.1f} MWh<extra></extra>'
            ))
            volume_fig.add_trace(go.Bar(
                x=df_processed['Timestamp'],
                y=df_processed['Solar_Volume'],
                name='Solarvolumen',
                width=3600000,
                marker=dict(
                    color='#76C7A6',  # Solar volume color
                    line=dict(width=0)  # No outline
                ),
                opacity=0.9,
                hovertemplate='<b>Solarvolumen</b>: %{y:.1f} MWh<extra></extra>'
            ))
            volume_fig.add_trace(go.Bar(
                x=df_processed['Timestamp'],
                y=df_processed['Spot_Volume_Positive'],
                name='Spotvolumen',
                width=3600000,
                marker=dict(
                    color='#A1C9E1',  # Spot volume color
                    line=dict(width=0)  # No outline
                ),
                opacity=0.9,
                hovertemplate='<b>Spotvolumen</b>: %{y:.1f} MWh<extra></extra>'
            ))
            
            # Add load as a line
            volume_fig.add_trace(go.Scatter(
                x=df_processed['Timestamp'],
                y=df_processed['Load'],
                mode='lines',
                name='Verbrauch',
                line=dict(color='black', width=2),
                hovertemplate='<b>Verbrauch</b>: %{y:.1f} MWh<extra></extra>'
            ))
            
            # Set up x-axis with numeric date format
            # Get min and max dates for proper tick spacing
            start_date = df_processed['Timestamp'].min()
            end_date = df_processed['Timestamp'].max()
            
            # Create date range for month starts (for tick marks)
            date_range = pd.date_range(start=start_date, end=end_date, freq='MS')  # Monthly start
            tick_vals = date_range.tolist()
            tick_text = [f"{d.month:02d}.{d.year}" for d in date_range]  # Format as MM.YYYY
            
            # Update layout for volume figure
            volume_fig.update_layout(
                title=f'Beschaffungsstrategie - {company_name}',
                xaxis_title='Datum',
                yaxis_title='Volumen (MWh)',
                barmode='stack',
                hovermode='x unified',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                plot_bgcolor='rgba(240,240,240,0.8)',
                # Numeric date formatting for x-axis
                xaxis=dict(
                    tickmode='array',
                    tickvals=tick_vals,
                    ticktext=tick_text,
                    tickangle=-45,
                    # Format hover labels to show dates as DD.MM.YYYY HH:MM
                    hoverformat="%d.%m.%Y %H:%M"
                ),
                # Configure hover label appearance
                hoverlabel=dict(
                    bgcolor="white", 
                    font_size=12
                )
            )
            
            # Create a subplot figure with 1 row and 2 columns
            fig = make_subplots(
                rows=1, cols=2,
                column_widths=[0.8, 0.2],  # 80% for plot, 20% for stats
                specs=[[{"type": "xy"}, {"type": "table"}]],  # xy plot on left, table on right
                horizontal_spacing=0.03  # Space between columns
            )
            
            # Add all traces from volume_fig to the first subplot
            for trace in volume_fig.data:
                fig.add_trace(trace, row=1, col=1)
            
            # Copy layout settings from original plot to the subplot
            for key in volume_fig.layout:
                if key not in ['width', 'height', 'title', 'xaxis', 'yaxis']:
                    fig.layout[key] = volume_fig.layout[key]
            
            # Update subplot axes
            fig.update_xaxes(
                title=volume_fig.layout.xaxis.title,
                tickmode=volume_fig.layout.xaxis.tickmode,
                tickvals=volume_fig.layout.xaxis.tickvals,
                ticktext=volume_fig.layout.xaxis.ticktext,
                tickangle=volume_fig.layout.xaxis.tickangle,
                hoverformat=volume_fig.layout.xaxis.hoverformat,
                row=1, col=1
            )
            
            fig.update_yaxes(
                title=volume_fig.layout.yaxis.title,
                row=1, col=1
            )
            
            # Get statistics from stats module
            hourly_stats = stats.calculate_statistics(hourly_file_path)
            additional_stats = stats.read_stats_input(stats_file_path)
            
            # Use values from stats file if available, otherwise use calculated values
            total_load = additional_stats.get("Total load", hourly_stats.get("Total load", 0))
            profile_cost = additional_stats.get("Profile cost", hourly_stats.get("Profile cost", 0))
            spot_bought = additional_stats.get("Spot bought", hourly_stats.get("Spot bought", 0))
            
            # Calculate sum of hedge_volume and PPA volumes from the original data (not processed)
            total_hedge = additional_stats.get("Total Terminmarkt", df['hedge_volume'].sum() / 1000)  # Use value from stats file if available
            total_solar_ppa = df['Solar_Volume'].sum() / 1000  # Convert to GWh
            total_wind_ppa = df['Wind_Volume'].sum() / 1000  # Convert to GWh
            total_ppa = additional_stats.get("PPA Total", total_solar_ppa + total_wind_ppa)  # Use value from stats file if available
            total_spot = spot_bought  # Use the value from stats file
            
            # Fix for -0.00 issue: ensure zeros are displayed as 0.00, not -0.00
            if abs(total_ppa) < 0.005:  # Threshold for considering a value as zero
                total_ppa = 0.0
            if abs(total_spot) < 0.005:
                total_spot = 0.0
            if abs(total_hedge) < 0.005:
                total_hedge = 0.0
            if abs(total_solar_ppa) < 0.005:
                total_solar_ppa = 0.0
            if abs(total_wind_ppa) < 0.005:
                total_wind_ppa = 0.0
            
            # Create values for the stats table
            categories = [
                "<b>Verbrauchsprofil</b>", 
                "Gesamtverbrauch", 
                "Spitzenlast",
                "Profilkosten", 
                "Vorhersagbarkeit", 
                "Peak-Anteil", 
                "Solar-Fit", 
                "Wind-Fit",
                "<b>Portfolio</b>",
                "<b>Spotmarkt</b>",
                "<b>Terminmarkt</b>",
                " Q1 Base/Peak",
                " Q2 Base/Peak",
                " Q3 Base/Peak", 
                " Q4 Base/Peak",
                " Jahr Base/Peak",
                "<b>PPA</b>",
                " Solar",
                " Wind"
            ]
            
            values = [
                "",  # Empty cell for the header
                f"{total_load:.1f} GWh",  # Add 0.1 GWh to total consumption
                f"{df['Load'].max():.1f} MW",
                f"{profile_cost*100 if profile_cost < 1 else profile_cost:.1f}%",  # If profile_cost is already in percent form, don't multiply by 100
                # Fix for Load predictability - check if it's -inf and replace with "n/a"
                "n/a" if np.isinf(hourly_stats['Load predictability']) else f"{hourly_stats['Load predictability']*100:.1f}%",
                f"{hourly_stats['Peak fraction']*100:.1f}%",
                f"{hourly_stats['Solar fit']*100:.1f}%",
                f"{hourly_stats['Wind fit']*100:.1f}%",
                "",  # Empty cell for the header
                f"<b>{total_spot:.2f} GWh</b>",  # Total spot volume
                f"<b>{total_hedge:.2f} GWh</b>",  # Total hedge volume
                f"{additional_stats['Terminmarkt Q1 Base']:.2f}/{additional_stats['Terminmarkt Q1 Peak']:.2f} GWh",
                f"{additional_stats['Terminmarkt Q2 Base']:.2f}/{additional_stats['Terminmarkt Q2 Peak']:.2f} GWh",
                f"{additional_stats['Terminmarkt Q3 Base']:.2f}/{additional_stats['Terminmarkt Q3 Peak']:.2f} GWh",
                f"{additional_stats['Terminmarkt Q4 Base']:.2f}/{additional_stats['Terminmarkt Q4 Peak']:.2f} GWh",
                f"{additional_stats['Terminmarkt Y Base']:.2f}/{additional_stats['Terminmarkt Y Peak']:.2f} GWh",
                f"<b>{total_ppa:.2f} GWh</b>",  # Total PPA volume calculated from the data
                f"{total_solar_ppa:.2f} GWh",  # Use calculated value instead of from stats
                f"{total_wind_ppa:.2f} GWh"   # Use calculated value instead of from stats
            ]
            
            # Define the background colors for each row
            background_colors = [
                "#f0f0f0",  # Header - Verbrauchsprofil - highlighted
                "white",    # Gesamtverbrauch
                "white",    # Spitzenlast
                "white",    # Profilkosten
                "white",    # Vorhersagbarkeit
                "white",    # Peak-Anteil
                "white",    # Solar-Fit
                "white",    # Wind-Fit - not highlighted
                "#f0f0f0",  # Header - Portfolio - highlighted
                "white",    # Spotmarkt - bold but not highlighted
                "white",    # Terminmarkt - bold but not highlighted
                "white",    # Q1 Base/Peak
                "white",    # Q2 Base/Peak
                "white",    # Q3 Base/Peak
                "white",    # Q4 Base/Peak
                "white",    # Jahr Base/Peak
                "white",    # PPA - bold but not highlighted
                "white",    # Solar
                "white"     # Wind
            ]
            
            # Add the stats table to the second subplot
            fig.add_trace(
                go.Table(
                    header=dict(
                        values=["<b>Statistik</b>"],
                        font=dict(size=14, color="white"),
                        fill_color="#2f4f4f",
                        align="left",
                        height=40
                    ),
                    cells=dict(
                        values=[categories, values],
                        font=dict(size=12),
                        fill_color=[background_colors],
                        align="left",
                        height=25
                    )
                ),
                row=1, col=2
            )
            
            # Update the overall layout
            fig.update_layout(
                title=f'Beschaffungsstrategie - {company_name}',
                height=800,  # Taller to fit all stats
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=0.8),
                hoverlabel=dict(
                    bgcolor="white", 
                    font_size=12
                ),
                plot_bgcolor='rgba(240,240,240,0.8)'
            )
            
            # Add logo to the upper right corner
            if os.path.exists(LOGO_PATH):
                # Encode the image file to base64
                with open(LOGO_PATH, "rb") as image_file:
                    encoded_image = base64.b64encode(image_file.read()).decode('ascii')
                
                # Add the image to the layout
                fig.add_layout_image(
                    dict(
                        source=f'data:image/png;base64,{encoded_image}',
                        xref="paper", yref="paper",
                        x=1.0, y=1.0,  # Position to align with the right edge of the stats subplot
                        sizex=0.15, sizey=0.15,  # Keep original size
                        xanchor="right", yanchor="bottom",  # Anchor bottom-right of logo
                        layer="above"
                    )
                )
            
            # Add German instructions below the plot
            instructions_html = """
            <div style="margin-top: 10px; background-color: #f8f9fa; padding: 15px; border-radius: 5px; font-family: Arial; color: #333; max-width: 100%; font-size: 14px; line-height: 1.5; border-left: 4px solid #2f4f4f;">
                <h4 style="margin-top: 0; color: #2f4f4f; font-size: 16px;">Hinweise zur interaktiven Nutzung:</h4>
                <p><b>Hineinzoomen:</b> Um einen bestimmten Zeitraum oder Bereich genauer zu betrachten, markieren Sie einfach mit der Maus den gewÃ¼nschten Bereich direkt im Diagramm.</p>
                <p><b>Herauszoomen:</b> Ein Doppelklick auf das Diagramm setzt die Ansicht wieder auf die ursprÃ¼ngliche Gesamtansicht zurÃ¼ck.</p>
                <p><b>Datenreihen ein- und ausblenden:</b> Klicken Sie in der Legende auf einzelne Elemente (z. B. "Spotvolumen" oder "Windvolumen"), um diese in der Darstellung ein- oder auszublenden. So kÃ¶nnen Sie gezielt einzelne Bestandteile betrachten.</p>
                <p><b>Details anzeigen:</b> Wenn Sie mit der Maus Ã¼ber die Balken oder Linien fahren, erscheinen hilfreiche Zusatzinformationen wie Volumenwerte und Zeitstempel.</p>
                <p><b>Darstellungsmodus Ã¤ndern:</b> Ãœber die Symbolleiste oben rechts im Diagramm kÃ¶nnen Sie zwischen verschiedenen Ansichtsmodi wechseln, z. B. Zoom oder ZurÃ¼cksetzen der Ansicht.</p>
            </div>
            """
            
            # Save the figure as HTML file for interactive viewing with instructions
            output_file = os.path.join(PLOTS_OUTPUT_DIR, f'{base_filename}_plot.html')
            
            # Generate the plot HTML
            plot_html = fig.to_html(include_plotlyjs='cdn', full_html=True)
            
            # Insert the instructions before the closing body tag
            modified_html = plot_html.replace('</body>', f'{instructions_html}</body>')
            
            # Write the modified HTML to file
            with open(output_file, 'w') as f:
                f.write(modified_html)
            
            # Now create a second plot with negative spot values shown in red, extending downward
            # Create a copy of the original figure
            fig2 = make_subplots(
                rows=1, cols=2,
                column_widths=[0.8, 0.2],  # 80% for plot, 20% for stats
                specs=[[{"type": "xy"}, {"type": "table"}]],  # xy plot on left, table on right
                horizontal_spacing=0.03  # Space between columns
            )
            
            # Add all traces from volume_fig to the first subplot, except the spot volume
            for trace in volume_fig.data:
                if trace.name != 'Spotvolumen':
                    fig2.add_trace(trace, row=1, col=1)
            
            # Add positive spot volume
            fig2.add_trace(
                go.Bar(
                    x=df_processed['Timestamp'],
                    y=df_processed['Spot_Volume_Positive'],
                    name='Spotvolumen (Kauf)',
                    width=3600000,
                    marker=dict(
                        color='#A1C9E1',  # Spot volume color (blue)
                        line=dict(width=0)  # No outline
                    ),
                    opacity=0.9,
                    hovertemplate='<b>Spotvolumen (Kauf)</b>: %{y:.1f} MWh<extra></extra>'
                ), 
                row=1, col=1
            )
            
            # Add negative spot volume (shown as negative values)
            fig2.add_trace(
                go.Bar(
                    x=df_processed['Timestamp'],
                    y=df_processed['Spot_Volume_Negative'],  # Already negative values
                    name='Spotvolumen (Verkauf)',
                    width=3600000,
                    marker=dict(
                        color='#FF6961',  # Red color for negative values
                        line=dict(width=0)  # No outline
                    ),
                    opacity=0.9,
                    hovertemplate='<b>Spotvolumen (Verkauf)</b>: %{y:.1f} MWh<extra></extra>'
                ),
                row=1, col=1
            )
            
            # Copy layout settings
            for key in volume_fig.layout:
                if key not in ['width', 'height', 'title', 'xaxis', 'yaxis']:
                    fig2.layout[key] = volume_fig.layout[key]
            
            # Update subplot axes
            fig2.update_xaxes(
                title=volume_fig.layout.xaxis.title,
                tickmode=volume_fig.layout.xaxis.tickmode,
                tickvals=volume_fig.layout.xaxis.tickvals,
                ticktext=volume_fig.layout.xaxis.ticktext,
                tickangle=volume_fig.layout.xaxis.tickangle,
                hoverformat=volume_fig.layout.xaxis.hoverformat,
                row=1, col=1
            )
            
            fig2.update_yaxes(
                title=volume_fig.layout.yaxis.title,
                row=1, col=1
            )
            
            # Add the stats table to the second subplot
            fig2.add_trace(
                go.Table(
                    header=dict(
                        values=["<b>Statistik</b>"],
                        font=dict(size=14, color="white"),
                        fill_color="#2f4f4f",
                        align="left",
                        height=40
                    ),
                    cells=dict(
                        values=[categories, values],
                        font=dict(size=12),
                        fill_color=[background_colors],
                        align="left",
                        height=25
                    )
                ),
                row=1, col=2
            )
            
            # Update the overall layout for the second plot
            fig2.update_layout(
                title=f'Beschaffungsstrategie (mit Spot-Verkauf) - {company_name}',
                height=800,  # Taller to fit all stats
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=0.8),
                hoverlabel=dict(
                    bgcolor="white", 
                    font_size=12
                ),
                plot_bgcolor='rgba(240,240,240,0.8)',
                barmode='relative'  # Use relative mode to allow negative values
            )
            
            # Add logo to the second plot
            if os.path.exists(LOGO_PATH):
                fig2.add_layout_image(
                    dict(
                        source=f'data:image/png;base64,{encoded_image}',
                        xref="paper", yref="paper",
                        x=1.0, y=1.0,  # Position to align with the right edge of the stats subplot
                        sizex=0.15, sizey=0.15,  # Keep original size
                        xanchor="right", yanchor="bottom",  # Anchor bottom-right of logo
                        layer="above"
                    )
                )
            
            # Save the second figure as HTML file
            output_file2 = os.path.join(PLOTS_OUTPUT_DIR, f'{base_filename}_plot_with_negative_spot.html')
            
            # Generate the second plot HTML
            plot_html2 = fig2.to_html(include_plotlyjs='cdn', full_html=True)
            
            # Insert the instructions before the closing body tag
            modified_html2 = plot_html2.replace('</body>', f'{instructions_html}</body>')
            
            # Write the modified HTML to file
            with open(output_file2, 'w') as f:
                f.write(modified_html2)
            
            # Move processed files to archive directories
            try:
                # Move hourly file to archive
                hourly_archive_file = os.path.join(HOURLY_ARCHIVE_DIR, os.path.basename(hourly_file_path))
                shutil.move(hourly_file_path, hourly_archive_file)
                print(f"Moved hourly file to: {hourly_archive_file}")
                
                # Move stats file to archive
                stats_archive_file = os.path.join(STATS_ARCHIVE_DIR, os.path.basename(stats_file_path))
                shutil.move(stats_file_path, stats_archive_file)
                print(f"Moved stats file to: {stats_archive_file}")
                
            except Exception as e:
                error_msg = f"Warning: Could not move files to archive: {e}"
                print(error_msg)
                result["errors"].append(error_msg)
            
            # Print stats to check if we have valid data and adjustments
            print(f"Data points: {len(df)}")
            print(f"Adjusted volume total - Min: {df_processed['New_Total'].min()}, Max: {df_processed['New_Total'].max()}")
            print(f"Load - Min: {df['Load'].min()}, Max: {df['Load'].max()}")
            print(f"Adjusted = Load check: {np.allclose(df_processed['New_Total'], df_processed['Load'], rtol=1e-05, atol=1e-08)}")
            print(f"Plots saved to {output_file} and {output_file2}")
            print("-" * 50)
            
            # Track successful processing
            result["files_processed"] += 1
            result["processed_files"].append({
                "hourly_file": hourly_file_path,
                "stats_file": stats_file_path,
                "company_name": company_name,
                "output_files": [output_file, output_file2]
            })
            
        except Exception as e:
            error_msg = f"Error processing {hourly_file_path}: {str(e)}"
            print(error_msg)
            result["errors"].append(error_msg)
            result["files_skipped"] += 1
            result["skipped_files"].append(hourly_file_path)

    print(f"Processing complete! {result['files_processed']} files processed, {result['files_skipped']} files skipped.")
    if result["errors"]:
        print(f"Encountered {len(result['errors'])} errors during processing.")
    print("All plots have been generated in the 'plots' directory")
    
    return result


if __name__ == "__main__":
    # Run the function when script is executed directly
    generate_plots()