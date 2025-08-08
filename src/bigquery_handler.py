#!/usr/bin/env python3
"""
BigQuery Handler

This module handles interactions with Google BigQuery for storing
ad performance data, benchmarks, and analysis results.
"""

import logging
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict

# Import settings from config
import sys
import os

# Add project root to path if running as script
if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, project_root)

from config.settings import GCP_PROJECT_ID, BIGQUERY_DATASET

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BigQueryHandler:
    """Handles interactions with Google BigQuery"""
    
    def __init__(
        self,
        project_id: str = GCP_PROJECT_ID,
        dataset_name: str = BIGQUERY_DATASET
    ):
        """
        Initialize the BigQuery handler
        
        Args:
            project_id: GCP Project ID
            dataset_name: BigQuery dataset name
        """
        self.project_id = project_id
        self.dataset_name = dataset_name
        
        # Initialize BigQuery client
        self.client = bigquery.Client(project=project_id)
        
        # Validate credentials
        if not self.project_id:
            logger.error("Missing GCP Project ID")
            raise ValueError("GCP_PROJECT_ID must be provided")
        
        logger.info(f"BigQuery handler initialized for project {project_id}, dataset {dataset_name}")
        
        # Ensure dataset exists
        self._ensure_dataset_exists()
    
    def _ensure_dataset_exists(self) -> None:
        """
        Ensure the dataset exists, create if it doesn't
        """
        dataset_ref = self.client.dataset(self.dataset_name)
        
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_name} already exists")
        except NotFound:
            logger.info(f"Dataset {self.dataset_name} not found, creating...")
            
            # Create dataset
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Set dataset location
            dataset.description = "Creative Analysis Data"
            
            try:
                dataset = self.client.create_dataset(dataset)
                logger.info(f"Dataset {self.dataset_name} created successfully")
            except Exception as e:
                logger.error(f"Error creating dataset: {str(e)}")
                raise
    
    def create_ad_performance_table(self) -> None:
        """
        Create the ad_performance table if it doesn't exist
        
        Table schema matches the schema defined in the build plan
        """
        table_id = f"{self.project_id}.{self.dataset_name}.ad_performance"
        
        # Define schema based on build plan
        schema = [
            bigquery.SchemaField("ad_id", "STRING", description="Ad ID from Meta"),
            bigquery.SchemaField("analysis_date", "DATE", description="Date of analysis"),
            bigquery.SchemaField("metrics", "RECORD", mode="NULLABLE", description="Performance metrics", fields=[
                bigquery.SchemaField("spend", "FLOAT64", description="Ad spend"),
                bigquery.SchemaField("impressions", "INTEGER", description="Impressions count"),
                bigquery.SchemaField("clicks", "INTEGER", description="Clicks count"),
                bigquery.SchemaField("conversions", "INTEGER", description="Conversions count"),
                bigquery.SchemaField("ctr", "FLOAT64", description="Click-through rate"),
                bigquery.SchemaField("cpa", "FLOAT64", description="Cost per acquisition"),
                bigquery.SchemaField("roas", "FLOAT64", description="Return on ad spend")
            ]),
            bigquery.SchemaField("performance_vs_benchmark", "FLOAT64", description="Percentage difference from benchmark"),
            bigquery.SchemaField("best_segment", "STRING", description="Best performing segment"),
            bigquery.SchemaField("insights", "STRING", description="Generated insights")
        ]
        
        # Create table with time partitioning on analysis_date
        table = bigquery.Table(table_id, schema=schema)
        table.description = "Ad performance data and analysis"
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="analysis_date"
        )
        
        try:
            self.client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")
        except NotFound:
            logger.info(f"Table {table_id} not found, creating...")
            try:
                table = self.client.create_table(table)
                logger.info(f"Table {table_id} created successfully")
            except Exception as e:
                logger.error(f"Error creating table: {str(e)}")
                raise
    
    def create_benchmarks_table(self) -> None:
        """
        Create the benchmarks table if it doesn't exist
        """
        table_id = f"{self.project_id}.{self.dataset_name}.benchmarks"
        
        # Define schema for benchmarks table
        schema = [
            bigquery.SchemaField("market", "STRING", description="Market name (e.g., UK)"),
            bigquery.SchemaField("valid_from", "DATE", description="Date from which these benchmarks are valid"),
            bigquery.SchemaField("valid_to", "DATE", description="Date until which these benchmarks are valid"),
            bigquery.SchemaField("metrics", "RECORD", mode="NULLABLE", description="Benchmark metrics", fields=[
                bigquery.SchemaField("ctr", "FLOAT64", description="Benchmark CTR"),
                bigquery.SchemaField("cpa", "FLOAT64", description="Benchmark CPA"),
                bigquery.SchemaField("roas", "FLOAT64", description="Benchmark ROAS"),
                bigquery.SchemaField("cpm", "FLOAT64", description="Benchmark CPM"),
                bigquery.SchemaField("conversion_rate", "FLOAT64", description="Benchmark conversion rate")
            ]),
            bigquery.SchemaField("segments", "STRING", description="JSON string of segment benchmarks")
        ]
        
        # Create table
        table = bigquery.Table(table_id, schema=schema)
        table.description = "Performance benchmarks for comparison"
        
        try:
            self.client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")
        except NotFound:
            logger.info(f"Table {table_id} not found, creating...")
            try:
                table = self.client.create_table(table)
                logger.info(f"Table {table_id} created successfully")
            except Exception as e:
                logger.error(f"Error creating table: {str(e)}")
                raise
    
    def create_analysis_log_table(self) -> None:
        """
        Create the analysis_log table if it doesn't exist
        """
        table_id = f"{self.project_id}.{self.dataset_name}.analysis_log"
        
        # Define schema for analysis_log table
        schema = [
            bigquery.SchemaField("run_id", "STRING", description="Unique run identifier"),
            bigquery.SchemaField("run_time", "TIMESTAMP", description="Time of analysis run"),
            bigquery.SchemaField("ad_count", "INTEGER", description="Number of ads analyzed"),
            bigquery.SchemaField("success_count", "INTEGER", description="Number of successfully analyzed ads"),
            bigquery.SchemaField("error_count", "INTEGER", description="Number of errors"),
            bigquery.SchemaField("run_status", "STRING", description="Run status (success/failure)"),
            bigquery.SchemaField("error_details", "STRING", description="Error details if any"),
            bigquery.SchemaField("run_duration_seconds", "FLOAT64", description="Run duration in seconds")
        ]
        
        # Create table
        table = bigquery.Table(table_id, schema=schema)
        table.description = "Log of analysis runs"
        
        try:
            self.client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")
        except NotFound:
            logger.info(f"Table {table_id} not found, creating...")
            try:
                table = self.client.create_table(table)
                logger.info(f"Table {table_id} created successfully")
            except Exception as e:
                logger.error(f"Error creating table: {str(e)}")
                raise
    
    def ensure_tables_exist(self) -> None:
        """
        Ensure all required tables exist, creating them if they don't
        """
        logger.info("Ensuring all required tables exist...")
        
        # Create all tables
        self.create_ad_performance_table()
        self.create_benchmarks_table()
        self.create_analysis_log_table()
        
        logger.info("All tables verified/created successfully")
    
    def insert_ad_performance(self, ad_data: Dict[str, Any], analysis_result: Dict[str, Any]) -> bool:
        """
        Insert ad performance data and analysis result into BigQuery
        
        Args:
            ad_data: Ad performance data from Meta API
            analysis_result: Results of performance analysis
            
        Returns:
            bool: True if insertion was successful
        """
        logger.info(f"Inserting ad performance data for ad {ad_data.get('ad_id', 'unknown')}")
        
        table_id = f"{self.project_id}.{self.dataset_name}.ad_performance"
        
        # Format data for insertion
        metrics = ad_data.get('metrics', {})
        
        # Extract relevant analysis data
        performance_vs_benchmark = analysis_result.get('benchmark_comparison', {}).get('overall_performance_score', 0)
        best_segment = analysis_result.get('segment_analysis', {}).get('best_segments', ['unknown'])[0]
        
        # Get insights as formatted text
        insights_text = analysis_result.get('formatted_text', '')
        if len(insights_text) > 65000:  # BigQuery STRING limit
            insights_text = insights_text[:65000]  # Truncate if too long
        
        # Create row for insertion
        row = {
            "ad_id": ad_data.get('ad_id', 'unknown'),
            "analysis_date": datetime.now().strftime('%Y-%m-%d'),
            "metrics": {
                "spend": metrics.get('spend', 0),
                "impressions": metrics.get('impressions', 0),
                "clicks": metrics.get('clicks', 0),
                "conversions": metrics.get('conversions', 0),
                "ctr": metrics.get('ctr', 0),
                "cpa": metrics.get('cpa', 0),
                "roas": metrics.get('roas', 0)
            },
            "performance_vs_benchmark": performance_vs_benchmark,
            "best_segment": best_segment,
            "insights": insights_text
        }
        
        # Insert row into BigQuery
        try:
            errors = self.client.insert_rows_json(table_id, [row])
            if errors == []:
                logger.info(f"Ad performance data inserted successfully for ad {ad_data.get('ad_id', 'unknown')}")
                return True
            else:
                logger.error(f"Errors inserting ad performance data: {errors}")
                return False
        except Exception as e:
            logger.exception(f"Error inserting ad performance data: {str(e)}")
            return False
    
    def insert_benchmarks(self, benchmark_data: Dict[str, Any]) -> bool:
        """
        Insert or update benchmark data in BigQuery
        
        Args:
            benchmark_data: Benchmark data to insert
            
        Returns:
            bool: True if insertion was successful
        """
        logger.info(f"Inserting benchmark data for market {benchmark_data.get('market', 'unknown')}")
        
        table_id = f"{self.project_id}.{self.dataset_name}.benchmarks"
        
        # Format data for insertion
        market = benchmark_data.get('market', 'unknown')
        metrics = benchmark_data.get('benchmarks', {})
        segments = benchmark_data.get('segments', {})
        
        # Convert segments to JSON string for storage
        segments_json = json.dumps(segments)
        
        # Set validity dates (default to current date if not provided)
        valid_from = benchmark_data.get('valid_from', datetime.now().strftime('%Y-%m-%d'))
        valid_to = benchmark_data.get('valid_to', '2099-12-31')  # Far future date if not specified
        
        # Create row for insertion
        row = {
            "market": market,
            "valid_from": valid_from,
            "valid_to": valid_to,
            "metrics": {
                "ctr": metrics.get('ctr', 0),
                "cpa": metrics.get('cpa', 0),
                "roas": metrics.get('roas', 0),
                "cpm": metrics.get('cpm', 0),
                "conversion_rate": metrics.get('conversion_rate', 0)
            },
            "segments": segments_json
        }
        
        # Insert row into BigQuery
        try:
            errors = self.client.insert_rows_json(table_id, [row])
            if errors == []:
                logger.info(f"Benchmark data inserted successfully for market {market}")
                return True
            else:
                logger.error(f"Errors inserting benchmark data: {errors}")
                return False
        except Exception as e:
            logger.exception(f"Error inserting benchmark data: {str(e)}")
            return False
    
    def log_analysis_run(self, run_data: Dict[str, Any]) -> bool:
        """
        Log an analysis run in BigQuery
        
        Args:
            run_data: Analysis run data
            
        Returns:
            bool: True if logging was successful
        """
        logger.info("Logging analysis run")
        
        table_id = f"{self.project_id}.{self.dataset_name}.analysis_log"
        
        # Generate run_id if not provided
        if 'run_id' not in run_data:
            run_data['run_id'] = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Set run_time if not provided
        if 'run_time' not in run_data:
            run_data['run_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Create row for insertion
        row = {
            "run_id": run_data.get('run_id'),
            "run_time": run_data.get('run_time'),
            "ad_count": run_data.get('ad_count', 0),
            "success_count": run_data.get('success_count', 0),
            "error_count": run_data.get('error_count', 0),
            "run_status": run_data.get('run_status', 'unknown'),
            "error_details": run_data.get('error_details', ''),
            "run_duration_seconds": run_data.get('run_duration_seconds', 0)
        }
        
        # Insert row into BigQuery
        try:
            errors = self.client.insert_rows_json(table_id, [row])
            if errors == []:
                logger.info(f"Analysis run logged successfully with ID {run_data.get('run_id')}")
                return True
            else:
                logger.error(f"Errors logging analysis run: {errors}")
                return False
        except Exception as e:
            logger.exception(f"Error logging analysis run: {str(e)}")
            return False
    
    def batch_insert_ad_performance(self, items: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        Batch insert multiple ad performance records
        
        Args:
            items: List of dictionaries containing ad_data and analysis_result pairs
            
        Returns:
            Tuple[int, int]: (success_count, error_count)
        """
        logger.info(f"Batch inserting {len(items)} ad performance records")
        
        table_id = f"{self.project_id}.{self.dataset_name}.ad_performance"
        rows = []
        
        # Prepare rows for insertion
        for item in items:
            ad_data = item.get('ad_data', {})
            analysis_result = item.get('analysis_result', {})
            
            # Skip if missing required data
            if not ad_data or not analysis_result:
                continue
            
            # Format data (similar to insert_ad_performance method)
            metrics = ad_data.get('metrics', {})
            performance_vs_benchmark = analysis_result.get('benchmark_comparison', {}).get('overall_performance_score', 0)
            best_segment = analysis_result.get('segment_analysis', {}).get('best_segments', ['unknown'])[0]
            insights_text = analysis_result.get('formatted_text', '')[:65000]  # Truncate if too long
            
            row = {
                "ad_id": ad_data.get('ad_id', 'unknown'),
                "analysis_date": datetime.now().strftime('%Y-%m-%d'),
                "metrics": {
                    "spend": metrics.get('spend', 0),
                    "impressions": metrics.get('impressions', 0),
                    "clicks": metrics.get('clicks', 0),
                    "conversions": metrics.get('conversions', 0),
                    "ctr": metrics.get('ctr', 0),
                    "cpa": metrics.get('cpa', 0),
                    "roas": metrics.get('roas', 0)
                },
                "performance_vs_benchmark": performance_vs_benchmark,
                "best_segment": best_segment,
                "insights": insights_text
            }
            
            rows.append(row)
        
        # If no valid rows, return
        if not rows:
            logger.warning("No valid rows to insert")
            return 0, len(items)
        
        # Insert rows into BigQuery
        try:
            errors = self.client.insert_rows_json(table_id, rows)
            if errors == []:
                logger.info(f"Successfully inserted {len(rows)} ad performance records")
                return len(rows), 0
            else:
                logger.error(f"Errors inserting batch ad performance data: {errors}")
                return 0, len(rows)
        except Exception as e:
            logger.exception(f"Error batch inserting ad performance data: {str(e)}")
            return 0, len(rows)
    
    def get_latest_benchmarks(self, market: str = "UK") -> Dict[str, Any]:
        """
        Get the latest benchmarks for a specific market
        
        Args:
            market: Market name (e.g., "UK")
            
        Returns:
            Dict: Benchmark data
        """
        logger.info(f"Getting latest benchmarks for market {market}")
        
        query = f"""
        SELECT * FROM `{self.project_id}.{self.dataset_name}.benchmarks`
        WHERE market = @market
          AND valid_from <= CURRENT_DATE()
          AND valid_to >= CURRENT_DATE()
        ORDER BY valid_from DESC
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("market", "STRING", market),
            ]
        )
        
        try:
            query_job = self.client.query(query, job_config=job_config)
            results = list(query_job)
            
            if results:
                row = results[0]
                metrics = row.get('metrics', {})
                segments_json = row.get('segments', '{}')
                
                # Parse segments from JSON string
                try:
                    segments = json.loads(segments_json)
                except json.JSONDecodeError:
                    segments = {}
                
                # Format result
                return {
                    "market": row.market,
                    "valid_from": row.valid_from.strftime('%Y-%m-%d') if row.valid_from else None,
                    "valid_to": row.valid_to.strftime('%Y-%m-%d') if row.valid_to else None,
                    "benchmarks": {
                        "ctr": metrics.ctr if hasattr(metrics, 'ctr') else 0,
                        "cpa": metrics.cpa if hasattr(metrics, 'cpa') else 0,
                        "roas": metrics.roas if hasattr(metrics, 'roas') else 0,
                        "cpm": metrics.cpm if hasattr(metrics, 'cpm') else 0,
                        "conversion_rate": metrics.conversion_rate if hasattr(metrics, 'conversion_rate') else 0
                    },
                    "segments": segments
                }
            else:
                # Return default benchmarks from config if none found in BigQuery
                logger.warning(f"No benchmarks found for market {market}, using defaults")
                from config.settings import BENCHMARKS_PATH
                
                try:
                    with open(BENCHMARKS_PATH, 'r') as file:
                        return json.load(file)
                except (FileNotFoundError, json.JSONDecodeError):
                    # Return minimal defaults if config file not found
                    return {
                        "market": market,
                        "benchmarks": {
                            "ctr": 1.5,
                            "cpa": 25.0,
                            "roas": 3.0,
                            "cpm": 15.0,
                            "conversion_rate": 2.0
                        },
                        "segments": {}
                    }
                
        except Exception as e:
            logger.exception(f"Error getting benchmarks: {str(e)}")
            # Return empty defaults on error
            return {
                "market": market,
                "benchmarks": {},
                "segments": {}
            }
    
    def get_ad_performance_history(self, ad_id: str, days: int = 30) -> List[Dict[str, Any]]:
        """
        Get performance history for a specific ad
        
        Args:
            ad_id: Ad ID
            days: Number of days to look back
            
        Returns:
            List[Dict]: List of performance records
        """
        logger.info(f"Getting performance history for ad {ad_id} over {days} days")
        
        query = f"""
        SELECT ad_id, analysis_date, metrics, performance_vs_benchmark, best_segment
        FROM `{self.project_id}.{self.dataset_name}.ad_performance`
        WHERE ad_id = @ad_id
          AND analysis_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
        ORDER BY analysis_date
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ad_id", "STRING", ad_id),
                bigquery.ScalarQueryParameter("days", "INT64", days)
            ]
        )
        
        try:
            query_job = self.client.query(query, job_config=job_config)
            results = []
            
            for row in query_job:
                metrics = row.get('metrics', {})
                
                # Format row
                result = {
                    "ad_id": row.ad_id,
                    "analysis_date": row.analysis_date.strftime('%Y-%m-%d') if row.analysis_date else None,
                    "metrics": {
                        "spend": metrics.spend if hasattr(metrics, 'spend') else 0,
                        "impressions": metrics.impressions if hasattr(metrics, 'impressions') else 0,
                        "clicks": metrics.clicks if hasattr(metrics, 'clicks') else 0,
                        "conversions": metrics.conversions if hasattr(metrics, 'conversions') else 0,
                        "ctr": metrics.ctr if hasattr(metrics, 'ctr') else 0,
                        "cpa": metrics.cpa if hasattr(metrics, 'cpa') else 0,
                        "roas": metrics.roas if hasattr(metrics, 'roas') else 0
                    },
                    "performance_vs_benchmark": row.performance_vs_benchmark,
                    "best_segment": row.best_segment
                }
                
                results.append(result)
            
            logger.info(f"Found {len(results)} performance records for ad {ad_id}")
            return results
            
        except Exception as e:
            logger.exception(f"Error getting ad performance history: {str(e)}")
            return []
    
    def get_top_performing_ads(self, limit: int = 10, days: int = 7) -> List[Dict[str, Any]]:
        """
        Get top performing ads based on performance_vs_benchmark
        
        Args:
            limit: Maximum number of ads to return
            days: Number of days to consider
            
        Returns:
            List[Dict]: List of top performing ads
        """
        logger.info(f"Getting top {limit} performing ads over the last {days} days")
        
        query = f"""
        SELECT ad_id, analysis_date, metrics, performance_vs_benchmark, best_segment
        FROM `{self.project_id}.{self.dataset_name}.ad_performance`
        WHERE analysis_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
        ORDER BY performance_vs_benchmark DESC
        LIMIT @limit
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("days", "INT64", days),
                bigquery.ScalarQueryParameter("limit", "INT64", limit)
            ]
        )
        
        try:
            query_job = self.client.query(query, job_config=job_config)
            results = []
            
            for row in query_job:
                metrics = row.get('metrics', {})
                
                # Format row
                result = {
                    "ad_id": row.ad_id,
                    "analysis_date": row.analysis_date.strftime('%Y-%m-%d') if row.analysis_date else None,
                    "metrics": {
                        "spend": metrics.spend if hasattr(metrics, 'spend') else 0,
                        "impressions": metrics.impressions if hasattr(metrics, 'impressions') else 0,
                        "clicks": metrics.clicks if hasattr(metrics, 'clicks') else 0,
                        "conversions": metrics.conversions if hasattr(metrics, 'conversions') else 0,
                        "ctr": metrics.ctr if hasattr(metrics, 'ctr') else 0,
                        "cpa": metrics.cpa if hasattr(metrics, 'cpa') else 0,
                        "roas": metrics.roas if hasattr(metrics, 'roas') else 0
                    },
                    "performance_vs_benchmark": row.performance_vs_benchmark,
                    "best_segment": row.best_segment
                }
                
                results.append(result)
            
            logger.info(f"Found {len(results)} top performing ads")
            return results
            
        except Exception as e:
            logger.exception(f"Error getting top performing ads: {str(e)}")
            return []
    
    def get_run_stats(self, days: int = 30) -> Dict[str, Any]:
        """
        Get statistics on analysis runs
        
        Args:
            days: Number of days to look back
            
        Returns:
            Dict: Run statistics
        """
        logger.info(f"Getting run statistics for the last {days} days")
        
        query = f"""
        SELECT 
          COUNT(*) as total_runs,
          SUM(CASE WHEN run_status = 'success' THEN 1 ELSE 0 END) as successful_runs,
          SUM(CASE WHEN run_status != 'success' THEN 1 ELSE 0 END) as failed_runs,
          SUM(ad_count) as total_ads_processed,
          SUM(success_count) as successful_ads,
          SUM(error_count) as error_count,
          AVG(run_duration_seconds) as avg_duration_seconds,
          MAX(run_duration_seconds) as max_duration_seconds,
          MIN(run_duration_seconds) as min_duration_seconds
        FROM `{self.project_id}.{self.dataset_name}.analysis_log`
        WHERE TIMESTAMP(run_time) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("days", "INT64", days)
            ]
        )
        
        try:
            query_job = self.client.query(query, job_config=job_config)
            results = list(query_job)
            
            if results:
                row = results[0]
                return {
                    "total_runs": row.total_runs,
                    "successful_runs": row.successful_runs,
                    "failed_runs": row.failed_runs,
                    "total_ads_processed": row.total_ads_processed,
                    "successful_ads": row.successful_ads,
                    "error_count": row.error_count,
                    "avg_duration_seconds": row.avg_duration_seconds,
                    "max_duration_seconds": row.max_duration_seconds,
                    "min_duration_seconds": row.min_duration_seconds
                }
            else:
                return {
                    "total_runs": 0,
                    "successful_runs": 0,
                    "failed_runs": 0,
                    "total_ads_processed": 0,
                    "successful_ads": 0,
                    "error_count": 0,
                    "avg_duration_seconds": 0,
                    "max_duration_seconds": 0,
                    "min_duration_seconds": 0
                }
                
        except Exception as e:
            logger.exception(f"Error getting run statistics: {str(e)}")
            return {"error": str(e)}


# Example usage
if __name__ == "__main__":
    # Initialize the BigQuery handler
    bq_handler = BigQueryHandler()
    
    # Ensure tables exist
    try:
        bq_handler.ensure_tables_exist()
        print("\nTables created/verified successfully")
    except Exception as e:
        print(f"\nError creating tables: {str(e)}")
    
    # Insert test benchmark data
    try:
        from config.settings import BENCHMARKS_PATH
        with open(BENCHMARKS_PATH, 'r') as file:
            benchmark_data = json.load(file)
        
        # Add validity dates
        benchmark_data['valid_from'] = '2023-01-01'
        benchmark_data['valid_to'] = '2025-12-31'
        
        result = bq_handler.insert_benchmarks(benchmark_data)
        print(f"\nBenchmark data inserted: {result}")
    except Exception as e:
        print(f"\nError inserting benchmark data: {str(e)}")
    
    # Sample ad performance data
    sample_ad = {
        "ad_id": "test_123",
        "ad_name": "Test Ad",
        "campaign_name": "Test Campaign",
        "metrics": {
            "spend": 75.5,
            "impressions": 15000,
            "clicks": 225,
            "conversions": 12,
            "ctr": 1.5,
            "cpm": 5.03,
            "cpa": 6.29,
            "roas": 4.2
        }
    }
    
    # Sample analysis result
    sample_analysis = {
        "ad_id": "test_123",
        "benchmark_comparison": {
            "overall_performance_score": 22.5,
            "performance_rating": "Above Average"
        },
        "segment_analysis": {
            "best_segments": ["age_25_34_female"]
        },
        "formatted_text": "Sample insights text for testing"
    }
    
    # Insert sample ad performance
    try:
        result = bq_handler.insert_ad_performance(sample_ad, sample_analysis)
        print(f"\nAd performance data inserted: {result}")
    except Exception as e:
        print(f"\nError inserting ad performance data: {str(e)}")
    
    # Test querying
    try:
        benchmarks = bq_handler.get_latest_benchmarks()
        print(f"\nLatest benchmarks for {benchmarks['market']}:")
        print(f"CTR: {benchmarks['benchmarks']['ctr']}%")
        print(f"CPA: £{benchmarks['benchmarks']['cpa']}")
        print(f"ROAS: {benchmarks['benchmarks']['roas']}x")
    except Exception as e:
        print(f"\nError retrieving benchmarks: {str(e)}")
    
    # Test getting performance history
    try:
        history = bq_handler.get_ad_performance_history("test_123")
        print(f"\nFound {len(history)} performance records for ad test_123")
    except Exception as e:
        print(f"\nError retrieving performance history: {str(e)}")
    
    # Test getting top performers
    try:
        top_ads = bq_handler.get_top_performing_ads(limit=5)
        print(f"\nFound {len(top_ads)} top performing ads")
    except Exception as e:
        print(f"\nError retrieving top performers: {str(e)}")
    
    # Test getting run stats
    try:
        stats = bq_handler.get_run_stats(days=7)
        print(f"\nRun statistics for the last 7 days:")
        print(f"Total runs: {stats['total_runs']}")
        print(f"Success rate: {stats['successful_runs']}/{stats['total_runs'] if stats['total_runs'] > 0 else 1}")
    except Exception as e:
        print(f"\nError retrieving run stats: {str(e)}")