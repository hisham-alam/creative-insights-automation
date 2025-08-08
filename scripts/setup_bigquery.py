#!/usr/bin/env python3
"""
Setup BigQuery Script

This script creates the required BigQuery tables for the Creative Analysis Tool.
It should be run once during initial setup or when the schema needs to be updated.
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Import BigQuery handler and settings
from src.bigquery_handler import BigQueryHandler
from config.settings import GCP_PROJECT_ID, BIGQUERY_DATASET, BENCHMARKS_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_bigquery(project_id: str = GCP_PROJECT_ID, dataset_name: str = BIGQUERY_DATASET) -> bool:
    """
    Set up BigQuery tables for the Creative Analysis Tool
    
    Args:
        project_id: GCP Project ID
        dataset_name: BigQuery dataset name
        
    Returns:
        bool: True if setup was successful
    """
    logger.info(f"Setting up BigQuery tables in project {project_id}, dataset {dataset_name}")
    
    try:
        # Initialize BigQuery handler
        bq_handler = BigQueryHandler(project_id=project_id, dataset_name=dataset_name)
        
        # Create dataset and tables
        bq_handler.ensure_tables_exist()
        logger.info("BigQuery tables created successfully")
        
        # Insert initial benchmarks
        logger.info("Loading initial benchmarks...")
        if os.path.exists(BENCHMARKS_PATH):
            with open(BENCHMARKS_PATH, 'r') as f:
                benchmark_data = json.load(f)
                
            # Add validity dates
            benchmark_data['valid_from'] = '2023-01-01'  # Starting date
            benchmark_data['valid_to'] = '2025-12-31'    # Far future date
                
            # Insert benchmarks
            result = bq_handler.insert_benchmarks(benchmark_data)
            if result:
                logger.info("Benchmarks loaded successfully")
            else:
                logger.warning("Failed to load benchmarks")
        else:
            logger.warning(f"Benchmarks file not found at {BENCHMARKS_PATH}")
        
        return True
        
    except Exception as e:
        logger.exception(f"Error setting up BigQuery: {str(e)}")
        return False

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Set up BigQuery tables for the Creative Analysis Tool'
    )
    parser.add_argument(
        '--project',
        help='GCP Project ID (defaults to value from settings)',
        default=GCP_PROJECT_ID
    )
    parser.add_argument(
        '--dataset',
        help='BigQuery dataset name (defaults to value from settings)',
        default=BIGQUERY_DATASET
    )
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    
    # Validate required values
    if not args.project:
        logger.error("GCP Project ID not provided. Set GCP_PROJECT_ID in .env or pass --project")
        sys.exit(1)
    
    # Run setup
    print(f"Setting up BigQuery tables in project {args.project}, dataset {args.dataset}...")
    success = setup_bigquery(args.project, args.dataset)
    
    if success:
        print("\nBigQuery setup completed successfully!")
        print(f"\nCreated tables in dataset {args.dataset}:")
        print("  - ad_performance: Stores ad performance metrics and analysis")
        print("  - benchmarks: Stores benchmark values for comparison")
        print("  - analysis_log: Logs pipeline runs and status")
        print("\nLoaded initial benchmarks from configuration")
    else:
        print("\nBigQuery setup failed. Check the logs for details.")
        sys.exit(1)