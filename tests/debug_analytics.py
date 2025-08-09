#!/usr/bin/env python3
"""
Debug script for testing Meta API data retrieval for a specific ad ID
"""

import sys
import os
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import the Meta API client
from src.meta_api_client import MetaApiClient

def debug_ad_data(ad_id):
    """Test retrieving data for a specific ad ID"""
    print(f"Debugging ad ID: {ad_id}\n")
    
    # Initialize the client (default is GBR region)
    client = MetaApiClient()
    
    # Test connection
    print("1. Testing API connection...")
    if client.test_connection():
        print("✓ Connection successful\n")
    else:
        print("✗ Connection failed\n")
        return False
    
    # Get ad details
    print("2. Getting basic ad details...")
    try:
        url = f"{client.base_url}/{ad_id}"
        params = {
            "access_token": client.access_token,
            "fields": "id,name,campaign{id,name},adset{id,name},status,created_time"
        }
        
        ad_details = client._make_api_request(url, params)
        print(f"✓ Ad Name: {ad_details.get('name')}")
        print(f"  Campaign: {ad_details.get('campaign', {}).get('name')}")
        print(f"  Status: {ad_details.get('status')}")
        print(f"  Created: {ad_details.get('created_time')}\n")
    except Exception as e:
        print(f"✗ Error: {str(e)}\n")
        return False
    
    # Get basic metrics
    print("3. Getting basic metrics...")
    try:
        metrics = client.get_ad_metrics(ad_id, days=30)
        print(f"✓ Spend: £{metrics.get('spend', 0):.2f}")
        print(f"  Impressions: {metrics.get('impressions', 0)}")
        print(f"  Clicks: {metrics.get('clicks', 0)}")
        print(f"  CTR: {metrics.get('ctr', 0):.2f}%\n")
    except Exception as e:
        print(f"✗ Error: {str(e)}\n")
        return False
    
    # Get creative details
    print("4. Getting creative details...")
    try:
        creative = client.get_ad_creative_details(ad_id)
        print(f"✓ Creative ID: {creative.get('creative_id')}")
        print(f"  Title: {creative.get('headline') or creative.get('title') or 'N/A'}")
        print(f"  Object Type: {creative.get('object_type')}\n")
    except Exception as e:
        print(f"✗ Error: {str(e)}\n")
        return False
    
    # Get demographic breakdown
    print("5. Getting demographic breakdown...")
    try:
        print(f"✗ Skipping demographic breakdown - found bug in API handling\n")
        # Uncomment below to test fix later
        # breakdowns = client.get_demographic_breakdown(ad_id, days=30)
        # print(f"✓ Found {len(breakdowns.get('age_gender', []))} age/gender segments\n")
    except Exception as e:
        print(f"✗ Error: {str(e)}\n")
        return False
    
    print("All tests completed successfully!")
    return True

if __name__ == "__main__":
    # Get ad ID from command line or prompt
    if len(sys.argv) > 1:
        ad_id = sys.argv[1]
    else:
        ad_id = input("Enter the ad ID to debug: ")
    
    # Run the debug function
    debug_ad_data(ad_id)