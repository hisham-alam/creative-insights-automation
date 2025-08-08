# Creative Analysis Tool

An automated system for analyzing Meta ad performance, comparing to benchmarks, generating insights, and updating Google Sheets.

## Project Overview

This tool analyzes Meta ad creative performance for ads launched exactly 7 days ago, comparing metrics to benchmarks, generating actionable insights, and automatically updating Google Sheets dashboards.

## How It Works

1. **Data Collection**: The system connects to the Meta Marketing API and retrieves ads created exactly 7 days ago
2. **Performance Analysis**: Ad metrics are compared against industry benchmarks to identify strengths and weaknesses
3. **Demographic Analysis**: Performance is broken down by demographic segments (age and gender)
4. **Insight Generation**: The system generates actionable insights and recommendations based on performance data
5. **Data Storage**: Results are stored in BigQuery for historical analysis
6. **Reporting**: A Google Sheets dashboard is automatically updated with the latest performance data

## Key Metrics Tracked

- Standard metrics: Spend, Impressions, CPM, CTR, CPC, Conversions, CPR
- Video metrics: 3-second views, 100% views, Hook Rate, Viewthrough Rate
- Demographic breakdowns: Performance by age groups and gender

## Core Components

- **Meta API Client**: Handles all interactions with Meta Marketing API
- **Performance Analyzer**: Compares ad metrics to benchmarks and calculates performance scores
- **Insight Generator**: Creates actionable recommendations based on performance data
- **Pipeline Manager**: Orchestrates the entire workflow from data retrieval to reporting
- **BigQuery Handler**: Manages data storage in Google BigQuery
- **Sheets Manager**: Updates Google Sheets dashboards with analysis results

## Testing

Run the metrics availability test to verify API access:
```
python tests/check_ad_data.py
```

This script tests whether all required metrics can be accessed from the Meta API and displays detailed metrics for a sample ad.

## Setup

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Configure the `.env` file with your API keys and settings:
   ```
   META_AD_ACCOUNT_ID=your_ad_account_id
   META_ACCESS_TOKEN=your_access_token
   GOOGLE_SHEETS_ID=your_google_sheets_id
   ```
4. Initialize BigQuery tables:
   ```
   python scripts/setup_bigquery.py
   ```
5. Initialize Google Sheets:
   ```
   python scripts/setup_sheets.py
   ```

## Usage

Run the main pipeline with:
```
python src/pipeline_manager.py
```

## Structure

- `config/`: Configuration files and benchmarks
- `src/`: Main source code
  - `meta_api_client.py`: Handles Meta Marketing API interactions
  - `performance_analyzer.py`: Analyzes ad performance against benchmarks
  - `insight_generator_simple.py`: Generates insights based on performance data
  - `pipeline_manager.py`: Orchestrates the entire analysis process
- `tests/`: Test scripts
  - `check_ad_data.py`: Validates data availability from Meta API
- `scripts/`: Setup and utility scripts
- `cloud_function/`: Google Cloud Function deployment files
- `docs/`: Documentation