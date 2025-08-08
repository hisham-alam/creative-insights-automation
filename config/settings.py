#!/usr/bin/env python3
"""
Configuration settings for the Creative Analysis Tool.
Contains environment variables and constants.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Meta API Settings
META_AD_ACCOUNT_ID = os.getenv('META_AD_ACCOUNT_ID', '935036179899914')
META_ACCESS_TOKEN = os.getenv('META_ACCESS_TOKEN', '')
META_API_VERSION = os.getenv('META_API_VERSION', 'v23.0')
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"

# Analysis Thresholds
SPEND_THRESHOLD = float(os.getenv('SPEND_THRESHOLD', 70.0))  # Minimum spend in £
DAYS_THRESHOLD = int(os.getenv('DAYS_THRESHOLD', 7))  # Number of days to analyze

# GCP Settings
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', '')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'creative_analysis')

# Google Sheets
SHEETS_SPREADSHEET_ID = os.getenv('SHEETS_SPREADSHEET_ID', '')

# Paths
CONFIG_DIR = Path(__file__).parent
PROJECT_ROOT = CONFIG_DIR.parent
BENCHMARKS_PATH = CONFIG_DIR / 'benchmarks.json'

# Run Mode
DEBUG = os.getenv('DEBUG', 'False').lower() in ('true', '1', 't')