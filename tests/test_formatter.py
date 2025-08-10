#!/usr/bin/env python3
"""
Test Sheets Formatter

This script tests the SheetsFormatter with mock data to ensure it correctly
formats and exports data to CSV.
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import the formatter
from src.sheets_formatter import SheetsFormatter

def create_mock_data():
    """Create mock ad data for testing"""
    
    # Sample mock ad data
    mock_ads = [
        {
            "ad_data": {
                "ad_id": "123456789",
                "ad_name": "Test Ad 1 - Video Ad",
                "campaign_name": "Test Campaign 1",
                "created_time": "2025-08-05T12:00:00",
                "metrics": {
                    "spend": 150.75,
                    "impressions": 10000,
                    "clicks": 500,
                    "ctr": 5.0,
                    "cpr": 15.08,
                    "hook_rate": 8.5,
                    "viewthrough_rate": 4.2
                },
                "creative": {
                    "video_url": "https://example.com/videos/test1.mp4",
                    "headline": "Amazing Offer",
                    "primary_text": "Check out our amazing offer today!"
                },
                "breakdowns": {
                    "age_gender": [
                        {
                            "age": "18-24",
                            "gender": "male",
                            "spend": 50.25,
                            "impressions": 3500,
                            "ctr": 4.8,
                            "conversions": 4,
                            "cpr": 12.56
                        },
                        {
                            "age": "25-34",
                            "gender": "female",
                            "spend": 100.50,
                            "impressions": 6500,
                            "ctr": 5.2,
                            "conversions": 7,
                            "cpr": 14.36
                        }
                    ]
                }
            },
            "analysis_result": {
                "benchmark_comparison": {
                    "overall_performance_score": 25.5,
                    "performance_rating": "Above Average"
                },
                "insights": {
                    "summary": [
                        "Direct comparison of products resonates strongly with audience.",
                        "Simple animation helps explain complex concepts quickly."
                    ]
                }
            }
        },
        {
            "ad_data": {
                "ad_id": "987654321",
                "ad_name": "Test Ad 2 - Image Ad",
                "campaign_name": "Test Campaign 2",
                "created_time": "2025-08-03T10:30:00",
                "metrics": {
                    "spend": 85.25,
                    "impressions": 5000,
                    "clicks": 150,
                    "ctr": 3.0,
                    "cpr": 28.42,
                    "hook_rate": 5.1,
                    "viewthrough_rate": 1.8
                },
                "creative": {
                    "image_url": "https://example.com/images/test2.jpg",
                    "headline": "Limited Time Offer",
                    "primary_text": "Don't miss this limited time offer!"
                },
                "breakdowns": {
                    "age_gender": [
                        {
                            "age": "35-44",
                            "gender": "male",
                            "spend": 35.25,
                            "impressions": 2000,
                            "ctr": 2.8,
                            "conversions": 1,
                            "cpr": 35.25
                        },
                        {
                            "age": "45-54",
                            "gender": "female",
                            "spend": 50.00,
                            "impressions": 3000,
                            "ctr": 3.2,
                            "conversions": 2,
                            "cpr": 25.00
                        }
                    ]
                }
            },
            "analysis_result": {
                "benchmark_comparison": {
                    "overall_performance_score": -15.2,
                    "performance_rating": "Below Average"
                },
                "insights": {
                    "summary": [
                        "Image quality is good but messaging is unclear.",
                        "Call to action could be more compelling."
                    ]
                }
            }
        }
    ]
    
    return mock_ads

def test_formatter():
    """Test the SheetsFormatter with mock data"""
    
    print("Starting SheetsFormatter test with mock data...")
    
    # Create output directory
    output_dir = project_root / "tests" / "output"
    output_dir.mkdir(exist_ok=True)
    
    # Initialize formatter
    formatter = SheetsFormatter(output_dir=str(output_dir))
    
    # Create mock data
    mock_ads = create_mock_data()
    
    print(f"Created {len(mock_ads)} mock ad entries for testing")
    
    # Format data for sheets
    formatted_ads = formatter.format_ad_data_for_sheets(mock_ads)
    print(f"Successfully formatted {len(formatted_ads)} ads for Google Sheets")
    
    # Create Google Sheets formulas
    sheets_ready_ads = formatter.create_sheets_formulas(formatted_ads)
    print(f"Created Google Sheets formulas for {len(sheets_ready_ads)} ads")
    
    # Export to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = formatter.export_to_csv(formatted_ads, f"test_ad_analysis_{timestamp}.csv")
    
    print(f"CSV export successful: {csv_path}")
    
    # Format for Google Sheets API
    sheets_rows = formatter.format_for_sheets_api(sheets_ready_ads)
    print(f"Created {len(sheets_rows)} rows for Google Sheets API")
    
    # Print header row and first data row to check content
    if len(sheets_rows) > 1:
        print("\nGoogle Sheets header row:")
        print(", ".join(sheets_rows[0]))
        
        print("\nFirst data row:")
        print(", ".join(str(value) for value in sheets_rows[1]))
    
    # Also save the mock data as JSON for reference
    json_path = output_dir / f"test_ad_analysis_{timestamp}.json"
    with open(json_path, 'w') as f:
        json.dump(mock_ads, f, indent=2, default=str)
        
    print(f"JSON data saved to: {json_path}")
    print(f"Test completed successfully!")
    
    return csv_path, json_path

if __name__ == "__main__":
    csv_path, json_path = test_formatter()
    
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)
    print(f"CSV exported to: {csv_path}")
    print(f"JSON saved to: {json_path}")
    print("=" * 50)