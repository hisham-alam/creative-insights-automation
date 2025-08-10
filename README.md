# Ad Analysis Tool

A robust system for analyzing Meta ad performance, comparing metrics to benchmarks, and generating detailed performance reports.

## Project Overview

This tool analyzes Meta ad performance data, comparing key metrics against account benchmarks to identify high and low-performing ads. It focuses on ads with sufficient spend over the past 7 days, with fallback mechanisms to ensure consistent analysis.

## How It Works

1. **Benchmark Collection**: The system dynamically calculates benchmarks based on account-level performance metrics
2. **Ad Data Retrieval**: Finds ads with sufficient spend (default: £70) over the past 7 days, with fallback options
3. **Performance Analysis**: Compares ad metrics against benchmarks to calculate performance scores
4. **Demographic Analysis**: Analyzes performance across demographic segments (age and gender)
5. **Data Export**: Exports results to JSON for further processing

## Key Metrics Analyzed

- **Standard Metrics**: Spend, Impressions, CPM, CTR, CPC, Conversions, CPR
- **Video Metrics**: 3-second views, 100% views, Hook Rate, Viewthrough Rate
- **Performance Scores**: Overall performance rating compared to benchmarks
- **Demographic Breakdowns**: Performance by age groups and gender

## Core Components

- **Meta API Client**: Handles all interactions with Meta Marketing API with robust error handling and rate limiting
- **Performance Analyzer**: Compares ad metrics to benchmarks and calculates performance scores
- **Data Validator**: Ensures data quality and completeness before analysis
- **Pipeline Manager**: Orchestrates the entire workflow from data retrieval to reporting

## Testing & Debugging

The repository includes several testing utilities:

- **debug_mvp.py**: Tests each stage of the pipeline with interactive configuration
  ```
  python tests/debug_mvp.py
  ```
  
- **check_ad_data.py**: Validates Meta API data availability and format
  ```
  python tests/check_ad_data.py
  ```
  
- **analyze_ad.py**: Analyzes a specific ad by ID for quick testing
  ```
  python tests/analyze_ad.py [ad_id]
  ```

- **benchmark_debugger.py**: Calculates account-level benchmarks for specific regions
  ```
  python tests/benchmark_debugger.py [region] [days]
  ```

## Setup

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Configure the `.env` file in the config folder with your API credentials:
   ```
   META_ACCESS_TOKEN=your_access_token
   META_AD_ACCOUNT_ID_GBR=your_ad_account_id
   ```

## Usage

Run the main pipeline with:
```
python src/pipeline_manager.py [--region REGION] [--max-ads N]
```

For debugging individual pipeline stages:
```
python tests/debug_mvp.py
```
This will prompt for configuration parameters in the terminal.

## Project Structure

- **config/**: Configuration files and environment variables
- **src/**: Main source code
  - `meta_api_client.py`: Robust Meta Marketing API client with fallbacks
  - `performance_analyzer.py`: Analyzes ad performance against benchmarks
  - `data_validator.py`: Validates ad data quality and completeness
  - `pipeline_manager.py`: Orchestrates the analysis workflow
- **tests/**: Test scripts and debugging utilities
  - `debug_mvp.py`: Tests each stage of the pipeline
  - `check_ad_data.py`: Validates data availability from Meta API
  - `analyze_ad.py`: Analyzes individual ads
  - `benchmark_debugger.py`: Generates account benchmarks
- **tests/output/**: Output JSON files from test runs

## Extending the Tool

The current implementation provides a solid foundation for ad performance analysis. Future enhancements planned:

- AI-powered insight generation
- Additional visualization capabilities
- Multi-account analysis