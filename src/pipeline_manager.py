#!/usr/bin/env python3
"""
Pipeline Manager

This module orchestrates the data flow of the Creative Analysis Tool,
connecting all components together from data retrieval to reporting.
"""

import logging
import time
import os
import sys
import json
import requests
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional
from pathlib import Path

# Add project root to path if running as script
if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, project_root)

# Import components
from config.settings import DEBUG, SPEND_THRESHOLD, DAYS_THRESHOLD, CONFIG_DIR
from src.meta_api_client import MetaApiClient
from src.data_validator import DataValidator
from src.performance_analyzer import PerformanceAnalyzer
from src.insight_generator_simple import InsightGeneratorSimple
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
            # Initialize Meta API client with default region (GBR)
            self.meta_client = MetaApiClient(region="GBR")
            
            # Initialize data validator
            self.validator = DataValidator()
            
            # Initialize performance analyzer with dynamic benchmarks
            # Load benchmarks from JSON file
            benchmarks_path = os.path.join(CONFIG_DIR, 'benchmarks.json')
            try:
                with open(benchmarks_path, 'r') as f:
                    benchmarks = json.load(f)
                logger.info(f"Loaded benchmarks from {benchmarks_path}")
                self.analyzer = PerformanceAnalyzer(benchmarks=benchmarks.get("GBR", {}))
            except Exception as e:
                logger.warning(f"Could not load benchmarks from {benchmarks_path}: {str(e)}")
                logger.warning("Using default benchmarks")
                self.analyzer = PerformanceAnalyzer()
            
            # Initialize insight generator
            self.insight_generator = InsightGeneratorSimple()
            
            # Initialize Sheets manager with default region (GBR)
            self.sheets_manager = SheetsManager(region="GBR")
            
            logger.info("All components initialized successfully")
            
        except Exception as e:
            logger.exception(f"Error initializing components: {str(e)}")
            raise
    
    def run_pipeline(self, region: str = "GBR", max_ads: int = 20) -> Dict[str, Any]:
        """
        Run the complete analysis pipeline
        
        Args:
            region: Region code (ASI, EUR, LAT, PAC, GBR, NAM)
            
        Returns:
            Dict: Pipeline run results
        """
        logger.info(f"Starting pipeline execution for region {region}")
        
        # Initialize Meta API client and Sheets Manager for the specified region
        self.meta_client = MetaApiClient(region=region)
        self.sheets_manager = SheetsManager(region=region)
        
        # Update analyzer with region-specific benchmarks
        benchmarks_path = os.path.join(CONFIG_DIR, 'benchmarks.json')
        try:
            with open(benchmarks_path, 'r') as f:
                benchmarks = json.load(f)
            # Get benchmarks for the specified region
            region_benchmarks = benchmarks.get(region, {})
            self.analyzer = PerformanceAnalyzer(benchmarks=region_benchmarks)
            logger.info(f"Loaded benchmarks for region: {region}")
        except Exception as e:
            logger.warning(f"Could not load benchmarks for region {region}: {str(e)}")
            # Continue with default benchmarks
        
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
            
            # Step 2: Find ads with sufficient spend over the past 7 days (excluding today)
            logger.info(f"Finding ads with minimum spend of £{SPEND_THRESHOLD} over the past {DAYS_THRESHOLD} days (excluding today)...")
            eligible_ads = self.meta_client.find_ads_with_spend(
                days=DAYS_THRESHOLD,
                min_spend=SPEND_THRESHOLD,
                limit=max_ads
            )
            
            # Fallback mechanism if no ads found
            if not eligible_ads:
                logger.warning("No ads found with sufficient spend, trying alternative methods...")
                # Try looking for ads created exactly 7 days ago
                logger.info("Trying to find ads created exactly 7 days ago...")
                eligible_ads = self.meta_client.get_eligible_ads(days_threshold=7)
                
                # If still no results, try any recent ads
                if not eligible_ads:
                    logger.warning("No ads created exactly 7 days ago, trying any recent ads...")
                    eligible_ads = self.meta_client.get_any_recent_ads(days=30, limit=max_ads)
            
            if not eligible_ads:
                logger.info("No eligible ads found for analysis")
                run_stats["run_status"] = "success"
                run_stats["ad_count"] = 0
                return self._complete_run(run_stats, start_time)
            
            run_stats["ad_count"] = len(eligible_ads)
            logger.info(f"Found {len(eligible_ads)} eligible ads for analysis")
            
            # Step 3: Process each ad individually with proper validation and rate limiting
            logger.info("Processing ads individually with proper validation...")
            
            # We'll process the ads one by one to ensure proper rate limiting
            valid_ads = []
            processed_count = 0
            
            for ad in eligible_ads:
                ad_id = ad.get('ad_id')
                ad_name = ad.get('ad_name', 'Unknown Ad')
                logger.info(f"Processing ad {ad_id}: {ad_name}")
                processed_count += 1
                
                # Get complete ad data with metrics, creative, and demographics
                try:
                    # Get complete ad data
                    ad_data = self.meta_client.get_complete_ad_data(ad_id, days=DAYS_THRESHOLD)
                    
                    if not ad_data:
                        logger.warning(f"No data returned for ad {ad_id}")
                        run_stats["error_count"] += 1
                        continue
                        
                    # Validate the ad data
                    validation_result = self.validator.validate_ad(ad_data)
                    if not validation_result['valid']:
                        logger.warning(f"Ad {ad_id} failed validation: {validation_result.get('reason', 'Unknown reason')}")
                        run_stats["error_count"] += 1
                        continue
                    
                    # Add to valid ads list
                    valid_ads.append(ad_data)
                    logger.info(f"Successfully validated ad {ad_id}")
                    
                    # Add a short delay between processing ads to avoid rate limiting
                    if processed_count < len(eligible_ads):
                        time.sleep(1)  # 1 second delay between ads
                        
                except Exception as e:
                    logger.error(f"Error processing ad {ad_id}: {str(e)}")
                    run_stats["error_count"] += 1
                    continue
            
            if not valid_ads:
                logger.warning("No valid ads after validation")
                run_stats["run_status"] = "success"  # Still count as success, just no valid ads
                run_stats["error_count"] = validation_results["invalid_count"]
                return self._complete_run(run_stats, start_time)
            
            # Step 4: Analyze ads and generate insights
            analyzed_ads = []
            
            # The ads are already fully loaded with complete data from the previous step
            # Now we just need to analyze them and generate insights
            logger.info(f"Analyzing {len(valid_ads)} valid ads...")
            
            for ad_data in valid_ads:
                try:
                    ad_id = ad_data.get('ad_id', 'unknown')
                    
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
                    logger.info(f"Successfully analyzed ad {ad_id}")
                    
                except Exception as e:
                    logger.error(f"Error analyzing ad {ad_data.get('ad_id', 'unknown')}: {str(e)}")
                    run_stats["error_count"] += 1
            
            # Step 5: Save results to files and update Google Sheets
            logger.info(f"Saving analysis results...")
            
            # Save results to JSON file in config folder
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            json_file_path = os.path.join(CONFIG_DIR, f'ad_analysis_{region}_{timestamp}.json')
            try:
                with open(json_file_path, 'w') as f:
                    json.dump(analyzed_ads, f, indent=2, default=str)
                logger.info(f"Analysis results saved to: {json_file_path}")
            except Exception as e:
                logger.error(f"Error saving results to JSON: {str(e)}")
            
            # Save results to CSV file in config folder
            csv_file_path = os.path.join(CONFIG_DIR, f'ad_analysis_{region}_{timestamp}.csv')
            try:
                import csv
                with open(csv_file_path, 'w', newline='') as csvfile:
                    # Define CSV headers
                    fieldnames = ["Ad ID", "Ad Name", "Campaign", "Analysis Date", "Spend (£)", "Impressions", 
                                "CTR (%)", "Hook Rate (%)", "Viewthrough Rate (%)", "CPR (£)", "Performance Score", "Rating"]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    # Write each ad's data
                    for ad in analyzed_ads:
                        ad_data = ad.get("ad_data", {})
                        analysis = ad.get("analysis_result", {})
                        metrics = ad_data.get("metrics", {})
                        
                        writer.writerow({
                            "Ad ID": ad_data.get("ad_id", ""),
                            "Ad Name": ad_data.get("ad_name", ""),
                            "Campaign": ad_data.get("campaign_name", ""),
                            "Analysis Date": analysis.get("analysis_date", ""),
                            "Spend (£)": metrics.get("spend", 0),
                            "Impressions": metrics.get("impressions", 0),
                            "CTR (%)": metrics.get("ctr_destination", 0),
                            "Hook Rate (%)": metrics.get("hook_rate", 0),
                            "Viewthrough Rate (%)": metrics.get("viewthrough_rate", 0),
                            "CPR (£)": metrics.get("cpr", 0),
                            "Performance Score": analysis.get("performance_score", 0),
                            "Rating": analysis.get("performance_rating", "")
                        })
                        
                logger.info(f"Analysis results saved to: {csv_file_path}")
            except Exception as e:
                logger.error(f"Error saving results to CSV: {str(e)}")
            
            # Update Google Sheets
            logger.info(f"Updating Google Sheets with analysis results...")
            
            try:
                # Update individual ad details
                sheets_results = self.sheets_manager.update_ad_details_batch(analyzed_ads)
                
                # Prepare summary data for dashboard
                summary_data = self._prepare_dashboard_summary(analyzed_ads)
                self.sheets_manager.update_dashboard(summary_data)
                
                logger.info(f"Google Sheets updated successfully")
            except Exception as e:
                logger.error(f"Error updating Google Sheets: {str(e)}")  # Non-fatal error
            
            # Complete run
            run_stats["run_status"] = "success"
            return self._complete_run(run_stats, start_time)
            
        except requests.exceptions.RequestException as e:
            # Handle API connection errors
            error_msg = f"API connection error: {str(e)}"
            logger.exception(error_msg)
            run_stats["run_status"] = "failed"
            run_stats["error_details"] = error_msg
            return self._complete_run(run_stats, start_time)
            
        except ValueError as e:
            # Handle data validation errors
            error_msg = f"Data validation error: {str(e)}"
            logger.exception(error_msg)
            run_stats["run_status"] = "failed"
            run_stats["error_details"] = error_msg
            return self._complete_run(run_stats, start_time)
            
        except Exception as e:
            # Handle unexpected errors
            error_msg = f"Pipeline error: {str(e)}"
            logger.exception(error_msg)
            import traceback
            logger.error(traceback.format_exc())
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
        # Parse command line arguments
        import argparse
        parser = argparse.ArgumentParser(description="Creative Analysis Tool Pipeline")
        parser.add_argument("--region", type=str, default="GBR", help="Region code (GBR, EUR, NAM, etc.)")
        parser.add_argument("--max-ads", type=int, default=20, help="Maximum number of ads to process")
        args = parser.parse_args()
        
        # Initialize and run pipeline
        pipeline = PipelineManager()
        results = pipeline.run_pipeline(region=args.region, max_ads=args.max_ads)
        
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