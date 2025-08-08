#!/usr/bin/env python3
"""
Insight Generator Simple

This module generates simple text insights based on ad performance data
without using AI. It uses rule-based logic to create insights about
performance compared to benchmarks, top performing segments, and
general recommendations.
"""

import logging
from typing import Dict, List, Any, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class InsightGeneratorSimple:
    """Generates simple text insights based on ad performance data"""
    
    def __init__(self):
        """Initialize the insight generator"""
        logger.info("Simple insight generator initialized")
    
    def generate_performance_insights(self, analysis_result: Dict[str, Any]) -> List[str]:
        """
        Generate insights about overall performance
        
        Args:
            analysis_result: Performance analysis result
            
        Returns:
            List[str]: List of performance insights
        """
        logger.info(f"Generating performance insights for ad {analysis_result.get('ad_id', 'unknown')}")
        
        insights = []
        
        # Get benchmark comparison data
        benchmark_comparison = analysis_result.get('benchmark_comparison', {})
        performance_rating = benchmark_comparison.get('performance_rating', 'Average')
        performance_score = benchmark_comparison.get('overall_performance_score', 0)
        metrics_comparison = benchmark_comparison.get('metrics_vs_benchmark', {})
        
        # Overall performance insight
        insights.append(f"This ad performed {performance_rating.lower()} compared to benchmarks "
                       f"with an overall score of {performance_score:.1f}%.")
        
        # Individual metric insights
        metrics_insights = []
        
        # CTR insight
        ctr_vs_benchmark = metrics_comparison.get('ctr_vs_benchmark', 0)
        if abs(ctr_vs_benchmark) >= 15:  # Only mention if significant difference
            if ctr_vs_benchmark > 0:
                metrics_insights.append(f"CTR is {ctr_vs_benchmark:.1f}% higher than benchmark, "
                                      f"indicating strong ad creative engagement.")
            else:
                metrics_insights.append(f"CTR is {abs(ctr_vs_benchmark):.1f}% lower than benchmark, "
                                      f"suggesting potential creative or targeting issues.")
        
        # CPA insight
        cpa_vs_benchmark = metrics_comparison.get('cpa_vs_benchmark', 0)
        if abs(cpa_vs_benchmark) >= 15:  # Only mention if significant difference
            if cpa_vs_benchmark > 0:
                metrics_insights.append(f"CPA is {cpa_vs_benchmark:.1f}% better than benchmark, "
                                      f"showing good conversion efficiency.")
            else:
                metrics_insights.append(f"CPA is {abs(cpa_vs_benchmark):.1f}% worse than benchmark, "
                                      f"indicating conversion optimization opportunities.")
        
        # ROAS insight
        roas_vs_benchmark = metrics_comparison.get('roas_vs_benchmark', 0)
        if abs(roas_vs_benchmark) >= 15:  # Only mention if significant difference
            if roas_vs_benchmark > 0:
                metrics_insights.append(f"ROAS is {roas_vs_benchmark:.1f}% higher than benchmark, "
                                      f"showing strong return on investment.")
            else:
                metrics_insights.append(f"ROAS is {abs(roas_vs_benchmark):.1f}% lower than benchmark, "
                                      f"suggesting opportunity to improve conversion value.")
        
        # CPM insight
        cpm_vs_benchmark = metrics_comparison.get('cpm_vs_benchmark', 0)
        if abs(cpm_vs_benchmark) >= 15:  # Only mention if significant difference
            if cpm_vs_benchmark > 0:
                metrics_insights.append(f"CPM is {cpm_vs_benchmark:.1f}% better than benchmark, "
                                      f"indicating efficient media buying.")
            else:
                metrics_insights.append(f"CPM is {abs(cpm_vs_benchmark):.1f}% worse than benchmark, "
                                      f"suggesting audience or placement optimization opportunities.")
                
        # Hook Rate insight
        hook_rate_vs_benchmark = metrics_comparison.get('hook_rate_vs_benchmark', 0)
        if abs(hook_rate_vs_benchmark) >= 15:  # Only mention if significant difference
            if hook_rate_vs_benchmark > 0:
                metrics_insights.append(f"Hook Rate is {hook_rate_vs_benchmark:.1f}% higher than benchmark, "
                                      f"indicating strong initial video engagement.")
            else:
                metrics_insights.append(f"Hook Rate is {abs(hook_rate_vs_benchmark):.1f}% lower than benchmark, "
                                      f"suggesting the video opening needs improvement.")
        
        # Viewthrough Rate insight
        viewthrough_rate_vs_benchmark = metrics_comparison.get('viewthrough_rate_vs_benchmark', 0)
        if abs(viewthrough_rate_vs_benchmark) >= 15:  # Only mention if significant difference
            if viewthrough_rate_vs_benchmark > 0:
                metrics_insights.append(f"Viewthrough Rate is {viewthrough_rate_vs_benchmark:.1f}% higher than benchmark, "
                                      f"indicating excellent video retention.")
            else:
                metrics_insights.append(f"Viewthrough Rate is {abs(viewthrough_rate_vs_benchmark):.1f}% lower than benchmark, "
                                      f"suggesting the video content needs improvement to maintain viewer interest.")
        
        # Add up to 4 metric insights (most significant ones)
        metrics_insights.sort(key=lambda x: abs(float(x.split('%')[0].split()[-1])), reverse=True)
        insights.extend(metrics_insights[:4])
        
        # Stability insight
        stability_score = benchmark_comparison.get('stability_score', 0)
        if stability_score < 50:
            insights.append(f"Performance data has low stability ({stability_score:.0f}/100), "
                          f"insights should be treated with caution.")
        
        return insights
    
    def generate_recommendation_insights(self, analysis_result: Dict[str, Any]) -> List[str]:
        """
        Generate actionable recommendations based on performance
        
        Args:
            analysis_result: Performance analysis result
            
        Returns:
            List[str]: List of recommendation insights
        """
        logger.info(f"Generating recommendations for ad {analysis_result.get('ad_id', 'unknown')}")
        
        insights = []
        
        # Get benchmark comparison data
        benchmark_comparison = analysis_result.get('benchmark_comparison', {})
        performance_score = benchmark_comparison.get('overall_performance_score', 0)
        
        # Get segment analysis
        segment_analysis = analysis_result.get('segment_analysis', {})
        best_segments = segment_analysis.get('best_segments', [])
        segment_performance = segment_analysis.get('segment_performance', {})
        
        # Budget recommendation based on performance
        if performance_score >= 20:
            insights.append("Recommendation: Scale budget for this ad to capitalize on strong performance.")
        elif performance_score <= -20:
            insights.append("Recommendation: Pause this ad and iterate on creative or targeting to improve performance.")
        else:
            insights.append("Recommendation: Maintain current budget while monitoring performance closely.")
        
        # Targeting recommendation based on segments
        if best_segments and segment_performance:
            best_segment = best_segments[0]
            if best_segment in segment_performance:
                best_data = segment_performance[best_segment]
                display_name = best_data.get('display_name', best_segment)
                segment_score = best_data.get('segment_score', 0)
                
                if segment_score > 15:
                    insights.append(f"Recommendation: Focus more budget on {display_name} segment with targeted creative.")
        
        # Optimization recommendations based on metrics
        metrics_comparison = benchmark_comparison.get('metrics_vs_benchmark', {})
        
        # CTR recommendation
        ctr_vs_benchmark = metrics_comparison.get('ctr_vs_benchmark', 0)
        if ctr_vs_benchmark < -15:
            insights.append("Recommendation: Test new ad creative to improve click-through rate.")
        
        # CPA recommendation
        cpa_vs_benchmark = metrics_comparison.get('cpa_vs_benchmark', 0)
        if cpa_vs_benchmark < -15:
            insights.append("Recommendation: Optimize landing page or audience targeting to improve conversion efficiency.")
        
        # ROAS recommendation
        roas_vs_benchmark = metrics_comparison.get('roas_vs_benchmark', 0)
        if roas_vs_benchmark < -15:
            insights.append("Recommendation: Review product pricing or promotion strategy to improve return on ad spend.")
            
        # Video metrics recommendations
        hook_rate_vs_benchmark = metrics_comparison.get('hook_rate_vs_benchmark', 0)
        viewthrough_rate_vs_benchmark = metrics_comparison.get('viewthrough_rate_vs_benchmark', 0)
        
        # If hook rate is poor but viewthrough rate is good
        if hook_rate_vs_benchmark < -15 and viewthrough_rate_vs_benchmark > 0:
            insights.append("Recommendation: Improve the first few seconds of your video to capture more attention while maintaining your strong content.")
        # If hook rate is good but viewthrough rate is poor
        elif hook_rate_vs_benchmark > 0 and viewthrough_rate_vs_benchmark < -15:
            insights.append("Recommendation: Your video opening is strong, but consider shortening the video or improving middle content to maintain viewer interest.")
        # If both metrics are poor
        elif hook_rate_vs_benchmark < -15 and viewthrough_rate_vs_benchmark < -15:
            insights.append("Recommendation: Consider a complete revision of video content to improve both initial engagement and retention.")
        
        return insights
    
    def generate_segment_insights(self, analysis_result: Dict[str, Any]) -> List[str]:
        """
        Generate insights about segment performance
        
        Args:
            analysis_result: Performance analysis result
            
        Returns:
            List[str]: List of segment insights
        """
        logger.info(f"Generating segment insights for ad {analysis_result.get('ad_id', 'unknown')}")
        
        insights = []
        
        # Get segment analysis data
        segment_analysis = analysis_result.get('segment_analysis', {})
        best_segments = segment_analysis.get('best_segments', [])
        worst_segments = segment_analysis.get('worst_segments', [])
        segment_performance = segment_analysis.get('segment_performance', {})
        
        # If no segments, return empty insights
        if not best_segments or not segment_performance:
            return []
        
        # Best segment insight
        best_segment = best_segments[0] if best_segments else None
        if best_segment and best_segment in segment_performance:
            best_data = segment_performance[best_segment]
            display_name = best_data.get('display_name', best_segment)
            conversions = best_data.get('metrics', {}).get('conversions', 0)
            conversion_contribution = best_data.get('conversion_contribution', 0)
            
            insights.append(f"Best performing segment: {display_name} with {conversions} conversions "
                          f"({conversion_contribution:.1f}% of total conversions).")
            
            # Add CPA insight for best segment if available
            cpa = best_data.get('metrics', {}).get('cpa', 0)
            if cpa > 0:
                insights.append(f"The {display_name} segment has a CPA of £{cpa:.2f}, "
                              f"which is your most cost-effective audience.")
        
        # Worst segment insight (if significantly different from best)
        worst_segment = worst_segments[0] if worst_segments else None
        if (worst_segment and worst_segment in segment_performance and 
                worst_segment != best_segment):
            worst_data = segment_performance[worst_segment]
            display_name = worst_data.get('display_name', worst_segment)
            spend = worst_data.get('metrics', {}).get('spend', 0)
            spend_contribution = worst_data.get('spend_contribution', 0)
            
            if spend > 0 and spend_contribution > 10:  # Only if significant spend
                insights.append(f"Least performing segment: {display_name} with £{spend:.2f} spend "
                              f"({spend_contribution:.1f}% of budget) but low conversion rate.")
        
        # Segment optimization opportunity
        if len(segment_performance) >= 3:
            # Find segments with high spend but low conversions
            segments = list(segment_performance.values())
            potential_optimizations = []
            
            for segment in segments:
                spend = segment.get('metrics', {}).get('spend', 0)
                conversions = segment.get('metrics', {}).get('conversions', 0)
                spend_contribution = segment.get('spend_contribution', 0)
                conversion_contribution = segment.get('conversion_contribution', 0)
                
                # If segment uses >15% of budget but delivers <10% of conversions
                if spend > 0 and spend_contribution > 15 and conversion_contribution < 10:
                    potential_optimizations.append({
                        'display_name': segment.get('display_name', 'Unknown'),
                        'spend': spend,
                        'spend_contribution': spend_contribution,
                        'conversion_contribution': conversion_contribution,
                        'gap': spend_contribution - conversion_contribution
                    })
            
            # Sort by largest gap between spend and conversion contribution
            potential_optimizations.sort(key=lambda x: x['gap'], reverse=True)
            
            # Add insight for top optimization opportunity
            if potential_optimizations:
                segment = potential_optimizations[0]
                insights.append(f"Budget optimization opportunity: {segment['display_name']} uses "
                              f"{segment['spend_contribution']:.1f}% of budget but delivers only "
                              f"{segment['conversion_contribution']:.1f}% of conversions.")
        
        # Add video performance insights if available
        for segment in segments:
            metrics = segment.get('metrics', {})
            if 'video_3_sec_views' in metrics and 'video_p100_watched' in metrics and metrics['video_3_sec_views'] > 0:
                display_name = segment.get('display_name', 'Unknown')
                
                # Calculate completion rate for this segment
                completion_rate = (metrics['video_p100_watched'] / metrics['video_3_sec_views']) * 100 if metrics['video_3_sec_views'] > 0 else 0
                
                # Only add insight if the completion rate is significantly different from average
                if completion_rate > 50:  # High completion rate
                    insights.append(f"Video performance highlight: {display_name} segment has a strong video completion rate "
                                  f"of {completion_rate:.1f}%, indicating high engagement with your content.")
                    break  # Just add one video highlight insight
                    
        return insights
        
    def generate_all_insights(self, analysis_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate all insights for an ad
        
        Args:
            analysis_result: Performance analysis result
            
        Returns:
            Dict: Complete insights data
        """
        logger.info(f"Generating all insights for ad {analysis_result.get('ad_id', 'unknown')}")
        
        # Generate different types of insights
        performance_insights = self.generate_performance_insights(analysis_result)
        segment_insights = self.generate_segment_insights(analysis_result)
        recommendation_insights = self.generate_recommendation_insights(analysis_result)
        
        # Combine insights
        all_insights = {
            "performance": performance_insights,
            "segments": segment_insights,
            "recommendations": recommendation_insights
        }
        
        # Create a summary with key insights
        summary = []
        
        # Add top performance insight
        if performance_insights:
            summary.append(performance_insights[0])
        
        # Add top segment insight
        if segment_insights:
            summary.append(segment_insights[0])
        
        # Add top recommendation
        if recommendation_insights:
            summary.append(recommendation_insights[0])
        
        all_insights["summary"] = summary
        
        # Create a formatted text version for reporting
        formatted_text = "\n\n".join([
            "PERFORMANCE INSIGHTS:\n" + "\n".join([f"- {insight}" for insight in performance_insights]),
            "SEGMENT INSIGHTS:\n" + "\n".join([f"- {insight}" for insight in segment_insights]),
            "RECOMMENDATIONS:\n" + "\n".join([f"- {insight}" for insight in recommendation_insights])
        ])
        
        all_insights["formatted_text"] = formatted_text
        
        logger.info(f"Generated {len(performance_insights) + len(segment_insights) + len(recommendation_insights)} insights")
        return all_insights


# Example usage
if __name__ == "__main__":
    # Import analyzer to generate sample analysis result
    import json
    from datetime import datetime, timedelta
    from performance_analyzer import PerformanceAnalyzer
    
    # Sample ad data for testing
    sample_ad = {
        "ad_id": "12345",
        "ad_name": "Test Ad",
        "campaign_name": "Test Campaign",
        "created_time": (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d'),
        "metrics": {
            "spend": 75.5,
            "impressions": 15000,
            "clicks": 225,
            "conversions": 12,
            "ctr": 1.5,
            "cpm": 5.03,
            "cpa": 6.29,
            "roas": 4.2
        },
        "breakdowns": {
            "age_gender": [
                {"age": "25-34", "gender": "female", "spend": 25.0, "impressions": 5000, "clicks": 100, "conversions": 6},
                {"age": "18-24", "gender": "male", "spend": 15.0, "impressions": 3000, "clicks": 50, "conversions": 2},
                {"age": "35-44", "gender": "female", "spend": 20.0, "impressions": 4000, "clicks": 45, "conversions": 3}
            ]
        }
    }
    
    # Get analysis result
    analyzer = PerformanceAnalyzer()
    analysis_result = analyzer.analyze_performance(sample_ad)
    
    # Generate insights
    insight_generator = InsightGeneratorSimple()
    insights = insight_generator.generate_all_insights(analysis_result)
    
    # Display results
    print("\nINSIGHTS SUMMARY:")
    for insight in insights['summary']:
        print(f"- {insight}")
    
    print("\nFORMATTED INSIGHTS:")
    print(insights['formatted_text'])
    
    # Save insights to file for review
    with open('sample_insights.json', 'w') as f:
        json.dump(insights, f, indent=2)