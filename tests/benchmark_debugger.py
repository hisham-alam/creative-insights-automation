#!/usr/bin/env python3
"""
Benchmark Debugger - Account Level

Calculates benchmark metrics at the account level over a specified time period.
Uses account-level insights API for efficient data retrieval.
"""

import sys
import os
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import the Meta API client
from src.meta_api_client import MetaApiClient

def get_account_benchmarks(region: str, days: int = 30) -> Dict[str, Any]:
    """
    Get account-level benchmark metrics for a specific region and time period
    
    Args:
        region: Region code (GBR, ASI, EUR, etc.)
        days: Number of days to analyze
        
    Returns:
        Dict: Account benchmark metrics
    """
    print(f"Calculating account benchmarks for {region} over the past {days} days...\n")
    
    # Initialize the client for the specified region
    client = MetaApiClient(region=region)
    
    # Test connection
    print("Testing API connection...")
    if not client.test_connection():
        print("❌ Failed to connect to Meta API")
        return {}
    
    # Get account info
    print("Getting account information...")
    account_info = client.get_account_info()
    if not account_info:
        print("❌ Failed to retrieve account information")
        return {}
    
    print(f"Account: {account_info.get('name')}")
    print(f"Account ID: {client.ad_account_id}")
    print(f"Currency: {account_info.get('currency')}")
    
    # Get account-level insights
    print(f"\nRetrieving account metrics for the past {days} days...")
    try:
        account_metrics = client.get_account_insights(days=days)
        if not account_metrics:
            print("❌ No metrics data available for this time period")
            return {}
            
        print("✅ Successfully retrieved account metrics\n")
        return account_metrics
        
    except Exception as e:
        print(f"❌ Error retrieving account metrics: {str(e)}")
        return {}

def format_metric(name: str, value: Any) -> str:
    """Format a metric value with appropriate units"""
    if name in ["spend", "cpc", "cpm", "cpp", "cpr", "cost_per_conversion"]:
        return f"£{float(value):.2f}"
    elif name in ["ctr", "ctr_destination", "hook_rate", "viewthrough_rate"]:
        return f"{float(value):.2f}%"
    elif name in ["impressions", "clicks", "reach", "conversions", "outbound_clicks", 
                  "video_3_sec_views", "video_p100_watched"]:
        return f"{int(float(value)):,}"
    elif name == "frequency":
        return f"{float(value):.2f}"
    else:
        return f"{value}"

def print_benchmark_results(metrics: Dict[str, Any], region: str) -> None:
    """
    Print formatted benchmark results and save to file
    
    Args:
        metrics: Dictionary of account metrics
        region: Region code
    """
    if not metrics:
        print("\nNo benchmark data available")
        return
        
    print("\n=== ACCOUNT BENCHMARK METRICS ===\n")
    
    # Define the specific metrics to include (as requested)
    required_metrics = {
        "cpm": "CPM (Cost Per 1,000 Impressions)",
        "hook_rate": "Hook Rate",
        "viewthrough_rate": "Viewthrough Rate",
        "ctr_destination": "CTR Destination",
        "cpc": "CPC (Cost Per Click)",
        "cpr": "CPR (Cost Per Registration)"
    }
    
    # Print only the requested metrics in defined order
    for metric, display_name in required_metrics.items():
        if metric in metrics:
            value = metrics[metric]
            formatted_value = format_metric(metric, value)
            print(f"{display_name}: {formatted_value}")
        else:
            print(f"{display_name}: Not available")
    
    # Save only the requested metrics to CSV file in config folder
    config_dir = os.path.join(Path(__file__).parent.parent, 'config')
    filename = f"benchmark_{region}_{datetime.now().strftime('%Y%m%d')}.csv"
    filepath = os.path.join(config_dir, filename)
    with open(filepath, "w") as f:
        f.write("Metric,Value,Formatted Value\n")
        for metric in required_metrics.keys():
            if metric in metrics:
                value = metrics[metric]
                formatted_value = format_metric(metric, value)
                f.write(f"{metric},{value},{formatted_value}\n")
            else:
                f.write(f"{metric},0,Not available\n")
    
    print(f"\nResults saved to {filepath}")

if __name__ == "__main__":
    # Set default values
    days = 30
    region = "GBR"  # Default region
    
    # Valid regions
    valid_regions = ["GBR", "ASI", "EUR", "LAT", "NAM"]
    
    # Check if command line arguments were provided
    # This allows for both interactive and non-interactive usage
    if len(sys.argv) > 1:
        # Non-interactive mode with command line arguments
        region = sys.argv[1].upper()
        if region not in valid_regions:
            print(f"❌ Invalid region code. Please use one of: {', '.join(valid_regions)}")
            sys.exit(1)
            
        # Get optional days parameter
        if len(sys.argv) > 2:
            try:
                days = int(sys.argv[2])
                if days <= 0:
                    print("Days must be a positive number")
                    sys.exit(1)
            except ValueError:
                print(f"Invalid days value: {sys.argv[2]}. Using default: 30 days")
    else:
        # Interactive mode with prompts
        try:
            print("=== META ACCOUNT BENCHMARK TOOL ===\n")
            print(f"Available regions: {', '.join(valid_regions)}\n")
            
            # Get region from user input
            while True:
                region = input("Enter region code (e.g., GBR): ").strip().upper()
                if region in valid_regions:
                    break
                else:
                    print(f"❌ Invalid region code. Please use one of: {', '.join(valid_regions)}\n")
            
            # Get days from user input
            while True:
                days_input = input("\nEnter days to analyze (default: 30): ").strip()
                if not days_input:  # Empty input, use default
                    days = 30
                    break
                try:
                    days = int(days_input)
                    if days <= 0:
                        print("\n❌ Days must be a positive number")
                        continue
                    if days > 90:
                        print("\n⚠️ Warning: Analyzing more than 90 days may exceed API limits")
                        confirm = input("Continue anyway? (y/n): ").strip().lower()
                        if confirm != "y":
                            continue
                    break
                except ValueError:
                    print("\n❌ Please enter a valid number of days")
                    continue
                    
            # Show selected parameters
            print(f"\nSelected parameters:\n- Region: {region}\n- Days: {days}")
            confirm = input("\nConfirm and run benchmark? (y/n): ").strip().lower()
            if confirm != "y":
                print("\nBenchmark cancelled")
                sys.exit(0)
                
        except (EOFError, KeyboardInterrupt):
            # Handle running in non-interactive environments
            print("\nRunning with default parameters: GBR region, 30 days")
            region = "GBR"
            days = 30
    
    # Display parameters being used (both for interactive and non-interactive modes)
    print(f"\nRunning benchmark analysis with:\n- Region: {region}\n- Days: {days}\n")
    
    # Start the benchmark calculation
    print(f"Starting benchmark analysis for {region} region over past {days} days\n")
    start_time = time.time()
    
    # Get and print the benchmark metrics
    metrics = get_account_benchmarks(region, days)
    print_benchmark_results(metrics, region)
    
    print(f"\nBenchmark calculation completed in {time.time() - start_time:.1f} seconds")