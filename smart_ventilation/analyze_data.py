#!/usr/bin/env python3
"""
Script to analyze the actual data ranges in the CSV files
"""

import pandas as pd
import numpy as np
import os

def analyze_csv_data():
    """Analyze the actual data ranges in the CSV files"""
    
    print("=== Analyzing CSV Data Ranges ===\n")
    
    # Temperature data
    try:
        temp_file = "datasets/10c_temp_last_30_days.csv"
        if os.path.exists(temp_file):
            print(f"Analyzing: {temp_file}")
            
            # Read CSV with proper columns
            df_temp = pd.read_csv(temp_file)
            if 'temperature' in df_temp.columns:
                temp_values = df_temp['temperature'].dropna()
                if len(temp_values) > 0:
                    print(f"Temperature: Min={temp_values.min():.1f}°C, Max={temp_values.max():.1f}°C, Mean={temp_values.mean():.1f}°C")
                    print(f"Temperature Range: {temp_values.max() - temp_values.min():.1f}°C")
                    print(f"Data points: {len(temp_values)}")
                else:
                    print("No temperature values found")
            else:
                print("Temperature column not found")
                
    except Exception as e:
        print(f"Error analyzing temperature data: {e}")
    
    print()
    
    # CO2 data
    try:
        co2_file = "datasets/10c_co2_last_30_days.csv"
        if os.path.exists(co2_file):
            print(f"Analyzing: {co2_file}")
            
            # Read CSV with proper columns
            df_co2 = pd.read_csv(co2_file)
            if 'co2' in df_co2.columns:
                co2_values = df_co2['co2'].dropna()
                if len(co2_values) > 0:
                    print(f"CO2: Min={co2_values.min():.0f}ppm, Max={co2_values.max():.0f}ppm, Mean={co2_values.mean():.0f}ppm")
                    print(f"CO2 Range: {co2_values.max() - co2_values.min():.0f}ppm")
                    print(f"Data points: {len(co2_values)}")
                else:
                    print("No CO2 values found")
            else:
                print("CO2 column not found")
                
    except Exception as e:
        print(f"Error analyzing CO2 data: {e}")
    
    print()
    
    # Humidity data (simulated based on temperature)
    try:
        print("Analyzing: Humidity (simulated)")
        # Generate realistic humidity values based on typical indoor ranges
        humidity_values = np.random.uniform(30, 70, 100)  # 30-70% typical indoor range
        print(f"Humidity: Min={humidity_values.min():.1f}%, Max={humidity_values.max():.1f}%, Mean={humidity_values.mean():.1f}%")
        print(f"Humidity Range: {humidity_values.max() - humidity_values.min():.1f}%")
                
    except Exception as e:
        print(f"Error analyzing humidity data: {e}")
    
    print("\n=== Recommended Slider Ranges ===")
    print("Based on the actual data, here are the recommended ranges:")
    print()
    print("Energy Consumption:")
    print("- Current: 2.0 kWh = 0%, 4.0 kWh = 100%")
    print("- Recommended: Keep as is (realistic for school environment)")
    print()
    print("Air Quality (CO2-based):")
    print("- Current: 65 = 0%, 95 = 100%")
    print("- Recommended: Use actual CO2 ranges from data")
    print()
    print("Temperature Deviation:")
    print("- Current: 0°C = 0%, 3°C = 100%")
    print("- Recommended: Use actual temperature ranges from data")

if __name__ == "__main__":
    analyze_csv_data()
