#!/usr/bin/env python3
"""
Test script to validate that required metrics can be pulled from Meta API.

This script tests whether the following metrics are available:
- Date Launched
- Media Spend
- CPM
- Impressions
- 3 second views
- 100% video watches
- CTR (destination)
- CPC
- Registrations
- CPR

It also verifies breakdown capabilities by age and gender.
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Import Meta API client
from src.meta_api_client import MetaApiClient

# Configure logging - only errors should go to console
logging.basicConfig(
    level=logging.ERROR,
    format='%(levelname)s - %(message)s'
)

def check_metrics_availability():
    """Test whether required metrics can be accessed from Meta API"""
    client = MetaApiClient()
    
    print("\n=== META API METRICS AVAILABILITY TEST ===")
    
    # Test connection first
    print("\n1. Testing API Connection...")
    try:
        client.test_connection()
        print("✓ Connected successfully to Meta API")
    except Exception as e:
        print(f"✗ ERROR: Failed to connect to Meta API: {str(e)}")
        return False
    
    # Get account info
    print("\n2. Retrieving Account Information...")
    try:
        account_info = client.get_account_info()
        print("✓ Account information retrieved successfully")
        print(f"  Account Name: {account_info.get('name')}")
        print(f"  Currency: {account_info.get('currency')}")
    except Exception as e:
        print(f"✗ ERROR: Failed to get account info: {str(e)}")
        return False
    
    # Get recent ads to test with - using improved methods with proper limits
    print("\n3. Finding Suitable Ads for Testing...")
    try:
        # Get ads created exactly 7 days ago, without spend threshold or limit
        eligible_ads = client.get_eligible_ads(days_threshold=7)
        
        if not eligible_ads:
            print("  No ads found from exactly 7 days ago, trying recent ads...")
            eligible_ads = client.get_any_recent_ads(days=30, limit=10)
        
        if not eligible_ads:
            print("✗ ERROR: No suitable ads found for testing")
            return False
        
        test_ad = eligible_ads[0]
        ad_id = test_ad.get('ad_id')
        
        print(f"✓ Found {len(eligible_ads)} suitable ads for testing")
        print(f"  Using ad '{test_ad.get('ad_name')}' (ID: {ad_id}) for testing")
        
        # Test required metrics
        print("\n4. Testing Required Metrics...")
        try:
            metrics = client.get_comprehensive_ad_metrics(ad_id, days=7)
            
            if metrics:
                print("✓ Successfully retrieved metrics")
                
                # Calculate additional metrics for video ads
                impressions = metrics.get('impressions', 0)
                if impressions > 0:
                    # Hook Rate: 3-second views / impressions
                    three_sec_views = metrics.get('video_3_sec_views', 0)
                    if three_sec_views:
                        hook_rate = (three_sec_views / impressions) * 100
                        metrics['hook_rate'] = hook_rate
                    
                    # Viewthrough Rate: 100% Views / Impressions
                    p100_views = metrics.get('video_p100_watched', 0)
                    if p100_views:
                        viewthrough_rate = (p100_views / impressions) * 100
                        metrics['viewthrough_rate'] = viewthrough_rate
                
                # Check which metrics are available
                available_metrics = 0
                total_metrics = 12  # Total number of metrics we're checking (added 2 new ones)
                
                metric_checks = [
                    ("Date Launched", metrics.get('date_launched') is not None),
                    ("Media Spend", metrics.get('spend') is not None),
                    ("CPM", metrics.get('cpm') is not None),
                    ("Impressions", metrics.get('impressions') is not None),
                    ("3 Second Views", metrics.get('video_3_sec_views') is not None),
                    ("100% Video Watches", metrics.get('video_p100_watched') is not None),
                    ("Hook Rate", metrics.get('hook_rate') is not None),
                    ("Viewthrough Rate", metrics.get('viewthrough_rate') is not None),
                    ("CTR (Destination)", metrics.get('ctr_destination') is not None),
                    ("CPC", metrics.get('cpc') is not None),
                    ("Registrations", metrics.get('registrations') is not None),
                    ("CPR", metrics.get('cpr') is not None)
                ]
                
                print("\n  Available Metrics:")
                for name, is_available in metric_checks:
                    status = "✓" if is_available else "✗"
                    if is_available:
                        available_metrics += 1
                    print(f"  {status} {name}")
                
                print(f"\n  {available_metrics} of {total_metrics} metrics are available")
                
                # Show detailed ad data for visual inspection
                print("\n  DETAILED AD DATA:")
                print(f"  Ad Name: {test_ad.get('ad_name')}")
                print(f"  Ad ID: {ad_id}")
                print(f"  Campaign: {test_ad.get('campaign_name')}")
                print(f"  Created: {metrics.get('date_launched', 'Unknown')}")
                print(f"  Status: {test_ad.get('status', 'Unknown')}")
                
                print("\n  Performance Metrics (past 7 days):")
                print(f"  - Spend: £{metrics.get('spend', 0):.2f}")
                print(f"  - Impressions: {metrics.get('impressions', 0):,}")
                print(f"  - CPM: £{metrics.get('cpm', 0):.2f}")
                print(f"  - Clicks: {metrics.get('clicks', 0):,}")
                print(f"  - CPC: £{metrics.get('cpc', 0):.2f}")
                print(f"  - CTR: {metrics.get('ctr_destination', 0):.2f}%")
                
                print("\n  Video Metrics:")
                print(f"  - 3 Second Views: {metrics.get('video_3_sec_views', 0):,}")
                print(f"  - 100% Views: {metrics.get('video_p100_watched', 0):,}")
                if 'hook_rate' in metrics:
                    print(f"  - Hook Rate: {metrics.get('hook_rate', 0):.2f}%")
                if 'viewthrough_rate' in metrics:
                    print(f"  - Viewthrough Rate: {metrics.get('viewthrough_rate', 0):.2f}%")
                
                print("\n  Conversion Metrics:")
                print(f"  - Registrations: {metrics.get('registrations', 0):,}")
                if metrics.get('registrations', 0) > 0 and metrics.get('spend', 0) > 0:
                    print(f"  - Cost Per Registration: £{metrics.get('cpr', 0):.2f}")
            else:
                print("✗ ERROR: Failed to retrieve metrics")
                return False
        except Exception as e:
            print(f"✗ ERROR: Problem getting metrics: {str(e)}")
            return False
        
        # Test demographic breakdowns
        print("\n5. Testing Demographic Breakdowns...")
        try:
            breakdowns = client.get_metrics_with_demographics(ad_id, days=7)
            
            if breakdowns:
                breakdown_types = []
                if 'age' in breakdowns:
                    breakdown_types.append("Age")
                if 'gender' in breakdowns:
                    breakdown_types.append("Gender")
                if 'age_gender' in breakdowns:
                    breakdown_types.append("Age+Gender")
                    
                print(f"✓ Successfully retrieved demographic breakdowns")
                print(f"  Available breakdown types: {', '.join(breakdown_types)}")
                
                # Display demographic breakdowns
                print("\n  DEMOGRAPHIC BREAKDOWN:")
                
                # Gender breakdown
                if 'gender' in breakdowns:
                    print("\n  Gender Breakdown:")
                    for item in breakdowns['gender']:
                        gender = item.get('gender', 'Unknown')
                        spend = item.get('spend', 0)
                        impressions = item.get('impressions', 0)
                        ctr = item.get('ctr_destination', 0)
                        registrations = item.get('registrations', 0)
                        
                        print(f"  - {gender}:")
                        print(f"    • Spend: £{spend:.2f}")
                        print(f"    • Impressions: {impressions:,}")
                        print(f"    • CTR: {ctr:.2f}%")
                        print(f"    • Registrations: {registrations:,}")
                        
                        # Calculate additional metrics if possible
                        if impressions > 0:
                            cpm = (spend / impressions) * 1000
                            print(f"    • CPM: £{cpm:.2f}")
                        
                        if registrations > 0:
                            cpr = spend / registrations
                            print(f"    • CPR: £{cpr:.2f}")
                
                # Age breakdown (top 5 age groups)
                if 'age' in breakdowns:
                    print("\n  Age Breakdown (top groups by spend):")
                    # Sort by spend and take top 5
                    sorted_ages = sorted(breakdowns['age'], key=lambda x: x.get('spend', 0), reverse=True)[:5]
                    for item in sorted_ages:
                        age = item.get('age', 'Unknown')
                        spend = item.get('spend', 0)
                        impressions = item.get('impressions', 0)
                        ctr = item.get('ctr_destination', 0)
                        registrations = item.get('registrations', 0)
                        
                        print(f"  - {age}:")
                        print(f"    • Spend: £{spend:.2f}")
                        print(f"    • Impressions: {impressions:,}")
                        print(f"    • CTR: {ctr:.2f}%")
                        print(f"    • Registrations: {registrations:,}")
                
                # Combined Age + Gender (top 3)
                if 'age_gender' in breakdowns:
                    print("\n  Top 3 Age+Gender Combinations (by spend):")
                    # Sort by spend and take top 3
                    sorted_combos = sorted(breakdowns['age_gender'], key=lambda x: x.get('spend', 0), reverse=True)[:3]
                    for item in sorted_combos:
                        age = item.get('age', 'Unknown')
                        gender = item.get('gender', 'Unknown')
                        spend = item.get('spend', 0)
                        impressions = item.get('impressions', 0)
                        ctr = item.get('ctr_destination', 0)
                        registrations = item.get('registrations', 0)
                        
                        print(f"  - {age} {gender}:")
                        print(f"    • Spend: £{spend:.2f}")
                        print(f"    • Impressions: {impressions:,}")
                        print(f"    • CTR: {ctr:.2f}%")
                        print(f"    • Registrations: {registrations:,}")
            else:
                print("✗ ERROR: Failed to retrieve demographic breakdowns")
                return False
        except Exception as e:
            print(f"✗ ERROR: Problem getting demographic breakdowns: {str(e)}")
            return False
        
        # Summary of what's available
        print("\n=== METRICS AVAILABILITY SUMMARY ===")
        
        availability = {
            "Date Launched": metrics.get('date_launched') is not None,
            "Media Spend": metrics.get('spend') is not None,
            "CPM": metrics.get('cpm') is not None,
            "Impressions": metrics.get('impressions') is not None,
            "3 Second Views": metrics.get('video_3_sec_views') is not None,
            "100% Video Watches": metrics.get('video_p100_watched') is not None,
            "CTR (Destination)": metrics.get('ctr_destination') is not None,
            "CPC": metrics.get('cpc') is not None,
            "Registrations": metrics.get('registrations') is not None,
            "CPR": metrics.get('cpr') is not None,
            "Age Breakdown": 'age' in breakdowns,
            "Gender Breakdown": 'gender' in breakdowns,
            "Age+Gender Breakdown": 'age_gender' in breakdowns,
        }
        
        # Count available metrics
        total_metrics = len(availability)
        available_metrics = sum(availability.values())
        availability_percentage = (available_metrics / total_metrics) * 100
        
        # Print summary
        print(f"\nOverall: {available_metrics} of {total_metrics} metrics available ({availability_percentage:.0f}%)")
        
        # Print result based on availability
        if availability_percentage > 80:
            print("\n✓ TEST PASSED - Most required metrics are available")
        elif availability_percentage > 50:
            print("\n! TEST PASSED WITH WARNINGS - Some metrics are unavailable")
        else:
            print("\n✗ TEST FAILED - Too many required metrics are unavailable")
        
        print("\n===============================================")
        
        return True
        
    except Exception as e:
        logger.error(f"Error during testing: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    print("\nMETA API METRICS AVAILABILITY TEST")
    print("Running test to verify availability of required metrics...\n")
    
    try:
        success = check_metrics_availability()
        if not success:
            print("\nTEST FAILED")
            sys.exit(1)
    except Exception as e:
        print(f"\nTEST FAILED WITH ERROR: {str(e)}")
        sys.exit(1)