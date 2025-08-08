#!/usr/bin/env python3
"""
Setup Google Sheets Script

This script creates and initializes a Google Sheet for the Creative Analysis Tool.
It sets up the required tabs and formatting for performance reporting.
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Import Sheets manager and settings
from src.sheets_manager import SheetsManager
from config.settings import SHEETS_SPREADSHEET_ID

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_sheets(spreadsheet_id: str = None, title: str = "Creative Performance Analysis") -> tuple:
    """
    Set up Google Sheets for the Creative Analysis Tool
    
    Args:
        spreadsheet_id: Optional existing spreadsheet ID
        title: Title for new spreadsheet if creating
        
    Returns:
        tuple: (success, spreadsheet_id, spreadsheet_url)
    """
    logger.info("Setting up Google Sheets for Creative Analysis Tool")
    
    try:
        # Initialize Sheets manager
        sheets_manager = SheetsManager(spreadsheet_id=spreadsheet_id)
        
        # Get spreadsheet URL
        spreadsheet_url = sheets_manager.get_spreadsheet_url()
        
        # Get the ID (in case a new one was created)
        spreadsheet_id = sheets_manager.spreadsheet_id
        
        logger.info(f"Google Sheets setup successful with ID: {spreadsheet_id}")
        
        # Initialize dashboard with sample data
        try:
            # Create sample summary data
            summary_data = {
                "date": "2023-01-01",
                "ads_analyzed": 0,
                "avg_performance_score": 0,
                "top_performers": [
                    {"ad_id": "example_1", "ad_name": "Example Top Ad", "score": 25.5}
                ],
                "bottom_performers": [
                    {"ad_id": "example_2", "ad_name": "Example Bottom Ad", "score": -10.2}
                ]
            }
            
            # Update dashboard with sample data
            sheets_manager.update_dashboard(summary_data)
            logger.info("Dashboard initialized with sample data")
            
        except Exception as e:
            logger.warning(f"Warning: Could not initialize dashboard with sample data: {str(e)}")
        
        return True, spreadsheet_id, spreadsheet_url
        
    except Exception as e:
        logger.exception(f"Error setting up Google Sheets: {str(e)}")
        return False, None, None

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Set up Google Sheets for the Creative Analysis Tool'
    )
    parser.add_argument(
        '--id',
        help='Existing Google Sheets ID (if omitted, a new spreadsheet will be created)',
        default=SHEETS_SPREADSHEET_ID
    )
    parser.add_argument(
        '--title',
        help='Title for new spreadsheet (if creating)',
        default="Creative Performance Analysis"
    )
    
    return parser.parse_args()

def update_env_file(spreadsheet_id: str) -> bool:
    """
    Update the .env file with the spreadsheet ID
    
    Args:
        spreadsheet_id: Google Sheets spreadsheet ID
        
    Returns:
        bool: True if successful
    """
    try:
        env_path = os.path.join(project_root, '.env')
        
        # Read existing .env file if it exists
        env_content = []
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                env_content = f.readlines()
        
        # Check if SHEETS_SPREADSHEET_ID already exists
        sheet_id_exists = False
        for i, line in enumerate(env_content):
            if line.startswith('SHEETS_SPREADSHEET_ID='):
                env_content[i] = f'SHEETS_SPREADSHEET_ID={spreadsheet_id}\n'
                sheet_id_exists = True
                break
        
        # Add SHEETS_SPREADSHEET_ID if it doesn't exist
        if not sheet_id_exists:
            env_content.append(f'SHEETS_SPREADSHEET_ID={spreadsheet_id}\n')
        
        # Write updated .env file
        with open(env_path, 'w') as f:
            f.writelines(env_content)
        
        return True
    except Exception as e:
        logger.error(f"Could not update .env file: {str(e)}")
        return False

if __name__ == "__main__":
    args = parse_arguments()
    
    # Run setup
    print(f"Setting up Google Sheets for Creative Analysis Tool...")
    success, spreadsheet_id, spreadsheet_url = setup_sheets(args.id, args.title)
    
    if success:
        print("\nGoogle Sheets setup completed successfully!")
        print(f"\nSpreadsheet ID: {spreadsheet_id}")
        print(f"Spreadsheet URL: {spreadsheet_url}")
        print("\nTabs created:")
        print("  - Dashboard: Summary metrics and performance trends")
        print("  - Ad Details: Detailed metrics for each ad")
        print("  - Segments: Performance data for demographic segments")
        
        # Update .env file with spreadsheet ID if needed
        if spreadsheet_id and not args.id:
            if update_env_file(spreadsheet_id):
                print(f"\n.env file updated with SHEETS_SPREADSHEET_ID={spreadsheet_id}")
            else:
                print("\nNote: Could not update .env file. Please add the following line manually:")
                print(f"SHEETS_SPREADSHEET_ID={spreadsheet_id}")
    else:
        print("\nGoogle Sheets setup failed. Check the logs for details.")
        sys.exit(1)