#!/usr/bin/env python3
"""
Pipeline Manager

This module orchestrates the data flow of the Creative Analysis Tool,
connecting all components together from data retrieval to reporting.
"""

import logging
import time
import uuid
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional
from pathlib import Path

# Add project root to path if running as script
if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, project_root)

# Import components
from config.settings import DEBUG
from src.meta_api_client import MetaApiClient
from src.data_validator import DataValidator
from src.performance_analyzer import PerformanceAnalyzer
from src.insight_generator_simple import InsightGeneratorSimple
from src.bigquery_handler import BigQueryHandler
from src.sheets_manager import SheetsManager

# Configure logging
logging.basicConfig(
    level=logging.INFO if not DEBUG else logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PipelineManager:
    """Orchestrates the Creative Analysis Tool pipeline"""
    
    def __init__(self):
        """Initialize the pipeline manager with all components"""
        logger.info("Initializing Pipeline Manager")
        
        try:
            # Initialize Meta API client
            self.meta_client = MetaApiClient()
            
            # Initialize data validator
            self.validator = DataValidator()
            
            # Initialize performance analyzer
            self.analyzer = PerformanceAnalyzer()
            
            # Initialize insight generator
            self.insight_generator = InsightGeneratorSimple()
            
            # Initialize BigQuery handler
            self.bq_handler = BigQueryHandler()
            
            # Initialize Sheets manager
            self.sheets_manager = SheetsManager()
            
            logger.info("All components initialized successfully")
            
        except Exception as e:
            logger.exception(f"Error initializing components: {str(e)}")
            raise
    
    def run_pipeline(self) -> Dict[str, Any]:
        """
        Run the complete analysis pipeline
        
        Returns:
            Dict: Pipeline run results
        """
        logger.info("Starting pipeline execution")
        
        # Generate unique run ID
        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        start_time = time.time()
        
        # Initialize run stats
        run_stats = {
            "run_id": run_id,
            "run_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "ad_count": 0,
            "success_count": 0,
            "error_count": 0,
            "run_status": "in_progress"
        }
        
        try:
            # Step 1: Test connection to Meta API
            logger.info("Testing Meta API connection...")
            if not self.meta_client.test_connection():
                error_msg = "Failed to connect to Meta API"
                logger.error(error_msg)
                run_stats["run_status"] = "failed"
                run_stats["error_details"] = error_msg
                return self._complete_run(run_stats, start_time)
            
            # Step 2: Ensure BigQuery tables exist
            logger.info("Ensuring BigQuery tables exist...")
            self.bq_handler.ensure_tables_exist()
            
            # Step 3: Query eligible ads from Meta (ads created exactly 7 days ago)
            logger.info("Querying ads created exactly 7 days ago...")
            eligible_ads = self.meta_client.get_eligible_ads(days_threshold=7)
            
            if not eligible_ads:
                logger.info("No eligible ads found for analysis")
                run_stats["run_status"] = "success"
                run_stats["ad_count"] = 0
                return self._complete_run(run_stats, start_time)
            
            run_stats["ad_count"] = len(eligible_ads)
            logger.info(f"Found {len(eligible_ads)} eligible ads for analysis")
            
            # Step 4: Validate ad data
            logger.info("Validating ad data...")
            validation_results = self.validator.validate_multiple_ads(eligible_ads)
            valid_ads = validation_results["valid_ads"]
            
            if not valid_ads:
                logger.warning("No valid ads after validation")
                run_stats["run_status"] = "success"  # Still count as success, just no valid ads
                run_stats["error_count"] = validation_results["invalid_count"]
                return self._complete_run(run_stats, start_time)
            
            # Step 5: Analyze ads and generate insights
            analyzed_ads = []
            
            for ad_data in valid_ads:
                try:
                    # Get detailed performance data
                    if 'ad_id' in ad_data:
                        ad_id = ad_data['ad_id']
                        
                        # Get detailed breakdowns if not already present
                        if 'breakdowns' not in ad_data or not ad_data['breakdowns']:
                            demographic_data = self.meta_client.get_demographic_breakdown(ad_id)
                            ad_data['breakdowns'] = demographic_data
                        
                        # Get creative details if not already present
                        if 'creative' not in ad_data or not ad_data['creative']:
                            creative_data = self.meta_client.get_ad_creative_details(ad_id)
                            ad_data['creative'] = creative_data
                    
                    # Analyze performance
                    analysis_result = self.analyzer.analyze_performance(ad_data)
                    
                    # Generate insights
                    insights = self.insight_generator.generate_all_insights(analysis_result)
                    
                    # Add insights to analysis result
                    analysis_result['insights'] = insights
                    analysis_result['formatted_text'] = insights.get('formatted_text', '')
                    
                    # Add to analyzed ads list
                    analyzed_ads.append({
                        "ad_data": ad_data,
                        "analysis_result": analysis_result
                    })
                    
                    run_stats["success_count"] += 1
                    
                except Exception as e:
                    logger.error(f"Error analyzing ad {ad_data.get('ad_id', 'unknown')}: {str(e)}")
                    run_stats["error_count"] += 1
            
            # Step 6: Store results in BigQuery
            logger.info(f"Storing {len(analyzed_ads)} analysis results in BigQuery...")
            bq_results = self.bq_handler.batch_insert_ad_performance(analyzed_ads)
            
            # Step 7: Update Google Sheets
            logger.info(f"Updating Google Sheets with analysis results...")
            
            # Update individual ad details
            sheets_results = self.sheets_manager.update_ad_details_batch(analyzed_ads)
            
            # Prepare summary data for dashboard
            summary_data = self._prepare_dashboard_summary(analyzed_ads)
            self.sheets_manager.update_dashboard(summary_data)
            
            # Complete run
            run_stats["run_status"] = "success"
            return self._complete_run(run_stats, start_time)
            
        except Exception as e:
            error_msg = f"Pipeline error: {str(e)}"
            logger.exception(error_msg)
            run_stats["run_status"] = "failed"
            run_stats["error_details"] = error_msg
            return self._complete_run(run_stats, start_time)
    
    def _complete_run(self, run_stats: Dict[str, Any], start_time: float) -> Dict[str, Any]:
        """
        Complete a pipeline run and log results
        
        Args:
            run_stats: Current run statistics
            start_time: Pipeline start time
            
        Returns:
            Dict: Final run statistics
        """
        # Calculate run duration
        end_time = time.time()
        duration = end_time - start_time
        run_stats["run_duration_seconds"] = round(duration, 2)
        
        # Log to BigQuery if available
        try:
            self.bq_handler.log_analysis_run(run_stats)
        except Exception as e:
            logger.error(f"Error logging run stats to BigQuery: {str(e)}")
        
        # Log to console
        status = run_stats["run_status"]
        if status == "success":
            logger.info(f"Pipeline completed successfully in {duration:.2f} seconds")
            logger.info(f"Processed {run_stats['ad_count']} ads: {run_stats['success_count']} successful, {run_stats['error_count']} failed")
        else:
            logger.error(f"Pipeline failed after {duration:.2f} seconds: {run_stats.get('error_details', 'Unknown error')}")
        
        return run_stats
    
    def _prepare_dashboard_summary(self, analyzed_ads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Prepare summary data for dashboard
        
        Args:
            analyzed_ads: List of analyzed ad data and results
            
        Returns:
            Dict: Summary data for dashboard
        """
        # Current date for analysis
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Count of analyzed ads
        ads_analyzed = len(analyzed_ads)
        
        # Calculate average performance score
        performance_scores = [
            ad["analysis_result"].get("benchmark_comparison", {}).get("overall_performance_score", 0)
            for ad in analyzed_ads if "analysis_result" in ad
        ]
        
        avg_performance_score = sum(performance_scores) / len(performance_scores) if performance_scores else 0
        
        # Find top performers
        top_performers = sorted(
            [
                {
                    "ad_id": ad["ad_data"].get("ad_id", "unknown"),
                    "ad_name": ad["ad_data"].get("ad_name", "Unknown Ad"),
                    "score": ad["analysis_result"].get("benchmark_comparison", {}).get("overall_performance_score", 0)
                }
                for ad in analyzed_ads if "ad_data" in ad and "analysis_result" in ad
            ],
            key=lambda x: x["score"],
            reverse=True
        )
        
        # Find bottom performers
        bottom_performers = sorted(
            [
                {
                    "ad_id": ad["ad_data"].get("ad_id", "unknown"),
                    "ad_name": ad["ad_data"].get("ad_name", "Unknown Ad"),
                    "score": ad["analysis_result"].get("benchmark_comparison", {}).get("overall_performance_score", 0)
                }
                for ad in analyzed_ads if "ad_data" in ad and "analysis_result" in ad
            ],
            key=lambda x: x["score"]
        )
        
        # Create summary data
        summary_data = {
            "date": today,
            "ads_analyzed": ads_analyzed,
            "avg_performance_score": avg_performance_score,
            "top_performers": top_performers[:5],  # Top 5 performers
            "bottom_performers": bottom_performers[:5]  # Bottom 5 performers
        }
        
        return summary_data


# Example usage
if __name__ == "__main__":
    try:
        # Initialize and run pipeline
        pipeline = PipelineManager()
        results = pipeline.run_pipeline()
        
        # Print completion message
        status = results["run_status"]
        if status == "success":
            print("\nPipeline completed successfully!")
            print(f"Processed {results['ad_count']} ads: {results['success_count']} successful, {results['error_count']} failed")
            print(f"Run duration: {results['run_duration_seconds']:.2f} seconds")
            
            # If sheets manager is initialized, print URL
            try:
                url = pipeline.sheets_manager.get_spreadsheet_url()
                print(f"\nView results at: {url}")
            except:
                pass
        else:
            print(f"\nPipeline failed: {results.get('error_details', 'Unknown error')}")
        
    except Exception as e:
        print(f"\nError running pipeline: {str(e)}")
        sys.exit(1)