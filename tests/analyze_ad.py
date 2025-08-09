#!/usr/bin/env python3
"""
Test script to analyze a specific Meta ad
"""

import sys
import os
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import required modules
from src.meta_api_client import MetaApiClient
from src.performance_analyzer import PerformanceAnalyzer
from src.insight_generator_simple import InsightGeneratorSimple
from config.settings import SPEND_THRESHOLD

def analyze_ad(ad_id):
    """Analyze a specific ad and print the results"""
    print(f"Analyzing ad ID: {ad_id}\n")
    
    # Initialize clients and analyzers
    meta_client = MetaApiClient()
    performance_analyzer = PerformanceAnalyzer()
    insight_generator = InsightGeneratorSimple()
    
    # Test connection
    if not meta_client.test_connection():
        print("❌ Failed to connect to Meta API")
        return
    
    print("Fetching ad details...")
    try:
        # Get detailed ad data
        ad_data = meta_client.get_complete_ad_data(ad_id)
        if not ad_data:
            print("❌ Failed to retrieve ad data")
            return
        
        # Print basic ad details
        print(f"\nAd: {ad_data.get('ad_name')}")
        print(f"Campaign: {ad_data.get('campaign_name')}")
        print(f"Created: {ad_data.get('created_time')}")
        print(f"Status: {ad_data.get('status')}")
        
        # Print key metrics
        metrics = ad_data.get('metrics', {})
        print(f"\nKey Metrics:")
        print(f"  Spend: £{metrics.get('spend', 0):.2f}")
        print(f"  Impressions: {metrics.get('impressions', 0)}")
        print(f"  Clicks: {metrics.get('clicks', 0)}")
        print(f"  CTR: {metrics.get('ctr', 0):.2f}%")
        
        if 'video' in metrics:
            video_metrics = metrics.get('video', {})
            print(f"\nVideo Metrics:")
            print(f"  Views: {video_metrics.get('views', 0)}")
            print(f"  25% completion: {video_metrics.get('p25', 0)}")
            print(f"  50% completion: {video_metrics.get('p50', 0)}")
            print(f"  75% completion: {video_metrics.get('p75', 0)}")
            print(f"  100% completion: {video_metrics.get('p100', 0)}")
        
        # Analyze performance against benchmarks
        print("\nAnalyzing performance against benchmarks...")
        performance_results = performance_analyzer.analyze_performance(ad_data)
        
        if performance_results:
            print(f"\nPerformance Results:")
            benchmark_comparison = performance_results.get('benchmark_comparison', {})
            rating = benchmark_comparison.get('performance_rating', 'Average')
            score = benchmark_comparison.get('overall_performance_score', 0)
            
            print(f"  Overall Rating: {rating} (Score: {score:.1f})")
            
            metrics_vs_benchmark = benchmark_comparison.get('metrics_vs_benchmark', {})
            if metrics_vs_benchmark:
                print("\n  Key Metrics vs Benchmarks:")
                for metric, value in metrics_vs_benchmark.items():
                    direction = "better" if value > 0 else "worse"
                    print(f"    {metric}: {abs(value):.1f}% {direction} than benchmark")
        
        # Generate insights
        print("\nGenerating insights...")
        insights_data = insight_generator.generate_all_insights(performance_results)
        insights = insights_data.get('summary', [])
        
        if insights:
            print(f"\nInsights:")
            for insight in insights:
                print(f"  • {insight}")
        else:
            print("  No significant insights found")
        
    except Exception as e:
        print(f"❌ Error analyzing ad: {str(e)}")

if __name__ == "__main__":
    # Get ad ID from command line or prompt
    if len(sys.argv) > 1:
        ad_id = sys.argv[1]
    else:
        ad_id = input("Enter the ad ID to analyze: ")
    
    # Run the analysis
    analyze_ad(ad_id)