#!/usr/bin/env python3
"""
Meta Marketing API Client

This module handles all interactions with the Meta Marketing API,
including authentication, querying ads, and retrieving performance data.
"""

import requests
import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union

# Import settings from config
import sys
import os

# Add project root to path if running as script
if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, project_root)

from config.settings import (
    META_AD_ACCOUNT_ID,
    META_ACCESS_TOKEN,
    META_API_VERSION,
    META_BASE_URL,
    SPEND_THRESHOLD,
    DAYS_THRESHOLD
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MetaApiClient:
    """Client for interacting with Meta Marketing API"""
    
    def __init__(
        self,
        ad_account_id: str = META_AD_ACCOUNT_ID,
        access_token: str = META_ACCESS_TOKEN,
        api_version: str = META_API_VERSION,
        base_url: str = META_BASE_URL
    ):
        """
        Initialize the Meta API client
        
        Args:
            ad_account_id: Meta Ad Account ID
            access_token: Meta API Access Token
            api_version: Meta API Version
            base_url: Meta API Base URL
        """
        self.ad_account_id = ad_account_id
        self.access_token = access_token
        self.api_version = api_version
        self.base_url = base_url
        self.rate_limit_wait = 1  # seconds to wait between requests
        
        # Validate credentials
        if not self.ad_account_id or not self.access_token:
            logger.error("Missing Meta API credentials")
            raise ValueError("META_AD_ACCOUNT_ID and META_ACCESS_TOKEN must be provided")
        
        logger.info(f"Meta API client initialized for account {ad_account_id}")
    
    def test_connection(self) -> bool:
        """
        Test connection to Meta API
        
        Returns:
            bool: True if connection successful
        
        Raises:
            Exception: If connection fails
        """
        logger.info("Testing Meta API connection...")
        
        url = f"{self.base_url}/me"
        params = {"access_token": self.access_token}
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Connected successfully! User: {data.get('name', 'Unknown')}")
                return True
            else:
                error_msg = f"Connection failed: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
        except Exception as e:
            logger.exception(f"Connection error: {str(e)}")
            raise
    
    def get_account_info(self) -> Dict[str, Any]:
        """
        Get information about the ad account
        
        Returns:
            Dict: Account information
        """
        logger.info("Getting ad account information...")
        
        url = f"{self.base_url}/act_{self.ad_account_id}"
        params = {
            "access_token": self.access_token,
            "fields": "name,currency,timezone_name,amount_spent"
        }
        
        try:
            response = self._make_api_request(url, params)
            logger.info(f"Account info retrieved: {response.get('name')}")
            return response
        except Exception as e:
            logger.exception(f"Error retrieving account info: {str(e)}")
            raise
    
    def _make_api_request(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make a request to the Meta API with rate limiting and error handling
        
        Args:
            url: API endpoint URL
            params: Query parameters
            
        Returns:
            Dict: API response data
            
        Raises:
            Exception: If API request fails
        """
        try:
            response = requests.get(url, params=params)
            
            # Handle rate limiting
            if response.status_code == 429:
                logger.warning("Rate limit reached, waiting before retry...")
                time.sleep(self.rate_limit_wait * 2)  # Double the wait time
                self.rate_limit_wait += 1  # Increase wait time for future requests
                return self._make_api_request(url, params)  # Retry request
            
            # Reset wait time on successful request
            self.rate_limit_wait = 1
            
            # Check for success
            if response.status_code == 200:
                return response.json()
            else:
                error_msg = f"API request failed: {response.status_code} - {response.text}"
                logger.error(error_msg)
                raise Exception(error_msg)
                
        except requests.exceptions.RequestException as e:
            logger.exception(f"Request error: {str(e)}")
            raise
        
        except Exception as e:
            logger.exception(f"Unexpected error: {str(e)}")
            raise
    
    def _handle_pagination(self, url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Handle pagination for API requests that return multiple items
        
        Args:
            url: API endpoint URL
            params: Query parameters
            
        Returns:
            List[Dict]: List of all items across pages
        """
        all_data = []
        next_page = None
        
        try:
            # First request
            response = self._make_api_request(url, params)
            
            # Add data from first page
            data = response.get('data', [])
            all_data.extend(data)
            
            # Check for pagination
            paging = response.get('paging', {})
            next_page = paging.get('next')
            
            # Fetch subsequent pages if they exist
            while next_page:
                logger.info(f"Fetching next page of results...")
                time.sleep(self.rate_limit_wait)  # Rate limiting
                
                response = requests.get(next_page)
                if response.status_code != 200:
                    break
                    
                response_data = response.json()
                data = response_data.get('data', [])
                all_data.extend(data)
                
                # Update pagination info
                paging = response_data.get('paging', {})
                next_page = paging.get('next')
            
            logger.info(f"Retrieved {len(all_data)} total items")
            return all_data
            
        except Exception as e:
            logger.exception(f"Error handling pagination: {str(e)}")
            raise

    def get_eligible_ads(self, days_threshold: int = DAYS_THRESHOLD) -> List[Dict[str, Any]]:
        """
        Query ads that were created exactly N days ago, regardless of spend
        
        Args:
            days_threshold: Number of days since ad creation (default: DAYS_THRESHOLD)
            
        Returns:
            List[Dict]: List of eligible ads with basic info
        """
        logger.info(f"Querying ads created exactly {days_threshold} days ago")
        
        # Calculate exact target date (exactly N days ago)
        today = datetime.now()
        target_date = today - timedelta(days=days_threshold)
        
        # Format dates for API (YYYY-MM-DD format)
        target_date_str = target_date.strftime('%Y-%m-%d')
        next_day_str = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Build URL and params - we'll use time_range to get a specific day's ads
        url = f"{self.base_url}/act_{self.ad_account_id}/ads"
        params = {
            "access_token": self.access_token,
            "fields": "id,name,campaign{id,name},adset{id,name},created_time,status,creative{id}",
            "time_range": json.dumps({
                "since": target_date_str,
                "until": target_date_str  # Same day for exact targeting
            }),
            "limit": 1000  # Set a high limit to get all matching ads
        }
        
        try:
            # Use pagination to get all matching ads
            ads = self._handle_pagination(url, params)
            
            # Further filter ads by checking their created_time
            exact_date_ads = []
            for ad in ads:
                if 'created_time' in ad:
                    ad_date = ad['created_time'].split('T')[0]  # Extract just the date part YYYY-MM-DD
                    if ad_date == target_date_str:
                        exact_date_ads.append(ad)
            
            logger.info(f"Found {len(exact_date_ads)} ads created exactly {days_threshold} days ago")
            # Replace ads with only those from the exact date
            ads = exact_date_ads
            
            # If no ads found, return empty list
            if not ads:
                logger.info("No ads found for the target date")
                return []
            
            # Process ads and include metrics
            eligible_ads = []
            
            for ad in ads:
                ad_id = ad.get('id')
                ad_metrics = self.get_ad_metrics(ad_id, days=days_threshold)
                
                # Combine ad data with metrics
                ad_data = {
                    "ad_id": ad_id,
                    "ad_name": ad.get('name'),
                    "campaign_id": ad.get('campaign', {}).get('id'),
                    "campaign_name": ad.get('campaign', {}).get('name'),
                    "adset_id": ad.get('adset', {}).get('id'),
                    "adset_name": ad.get('adset', {}).get('name'),
                    "created_time": ad.get('created_time'),
                    "status": ad.get('status'),
                    "metrics": ad_metrics
                }
                
                # Just include the creative ID if available
                creative = ad.get('creative', {})
                if creative and 'id' in creative:
                    ad_data.update({
                        "creative_id": creative.get('id')
                    })
                
                eligible_ads.append(ad_data)
            
            logger.info(f"Processed {len(eligible_ads)} eligible ads created exactly {days_threshold} days ago")
            return eligible_ads
            
        except Exception as e:
            logger.exception(f"Error querying eligible ads: {str(e)}")
            raise
    
    def get_ad_metrics(self, ad_id: str, days: int = DAYS_THRESHOLD) -> Dict[str, Any]:
        """
        Get basic performance metrics for a specific ad over the specified time period
        
        Args:
            ad_id: Meta Ad ID
            days: Number of days to analyze (default: DAYS_THRESHOLD)
            
        Returns:
            Dict: Basic performance metrics
        """
        logger.info(f"Getting basic metrics for ad {ad_id}")
        
        # Calculate date range
        today = datetime.now()
        since_date = today - timedelta(days=days)
        
        # Format dates for API (YYYY-MM-DD format)
        since_date_str = since_date.strftime('%Y-%m-%d')
        today_str = today.strftime('%Y-%m-%d')
        
        # Build URL and params
        url = f"{self.base_url}/{ad_id}/insights"
        params = {
            "access_token": self.access_token,
            "fields": "spend,impressions,clicks,conversions,ctr,cost_per_conversion,cost_per_action_type,"
                     "video_thruplay_watched_actions,video_p100_watched_actions,video_p75_watched_actions,"
                     "outbound_clicks,outbound_clicks_ctr",
            "time_range": json.dumps({
                "since": since_date_str,
                "until": today_str
            }),
            "level": "ad"
        }
        
        try:
            response = self._make_api_request(url, params)
            data = response.get('data', [])
            
            if data:
                metrics_data = data[0]  # Get the first (should be only) result
                
                # Extract and format the metrics
                metrics = {
                    "spend": float(metrics_data.get('spend', 0)),
                    "impressions": int(metrics_data.get('impressions', 0)),
                    "clicks": int(metrics_data.get('clicks', 0)),
                    "ctr": float(metrics_data.get('ctr', 0)) * 100,  # Convert to percentage
                }
                
                # Handle outbound_clicks which could be a list or a number
                outbound_clicks = metrics_data.get('outbound_clicks', 0)
                if isinstance(outbound_clicks, list):
                    # If it's a list, sum the values if there are any, otherwise use 0
                    outbound_clicks_sum = 0
                    for item in outbound_clicks:
                        if isinstance(item, dict) and 'value' in item:
                            outbound_clicks_sum += int(float(item.get('value', 0)))
                    metrics['outbound_clicks'] = outbound_clicks_sum
                else:
                    # If it's a scalar value, convert to int
                    metrics['outbound_clicks'] = int(float(outbound_clicks) if outbound_clicks else 0)
                
                # Handle conversions which could be a list or a number
                conversions = metrics_data.get('conversions', 0)
                if isinstance(conversions, list):
                    # If it's a list, sum the values if there are any, otherwise use 0
                    conv_sum = 0
                    for conv in conversions:
                        if isinstance(conv, dict) and 'value' in conv:
                            conv_sum += int(float(conv.get('value', 0)))
                    metrics['conversions'] = conv_sum
                else:
                    # If it's a scalar value, convert to int
                    metrics['conversions'] = int(float(conversions))
                
                # Calculate additional metrics
                if metrics['impressions'] > 0:
                    metrics['cpm'] = (metrics['spend'] / metrics['impressions']) * 1000
                else:
                    metrics['cpm'] = 0
                    
                if metrics['conversions'] > 0:
                    metrics['cpa'] = metrics['spend'] / metrics['conversions']
                else:
                    metrics['cpa'] = 0
                    
                # Try to get ROAS from cost_per_action_type
                cost_per_action = metrics_data.get('cost_per_action_type', [])
                purchase_action = next((a for a in cost_per_action if a.get('action_type') == 'purchase'), None)
                
                if purchase_action:
                    purchase_value = float(purchase_action.get('value', 0))
                    if metrics['spend'] > 0 and purchase_value > 0:
                        metrics['roas'] = purchase_value / metrics['spend']
                    else:
                        metrics['roas'] = 0
                else:
                    metrics['roas'] = 0
                
                # CTR (destination) from outbound_clicks_ctr
                ctr_destination = metrics_data.get('outbound_clicks_ctr', [])
                if ctr_destination and isinstance(ctr_destination, list) and len(ctr_destination) > 0:
                    metrics['ctr_destination'] = float(ctr_destination[0].get('value', 0))
                else:
                    # Calculate manually if we have the data
                    if metrics['impressions'] > 0 and metrics['outbound_clicks'] > 0:
                        metrics['ctr_destination'] = (metrics['outbound_clicks'] / metrics['impressions']) * 100
                    else:
                        metrics['ctr_destination'] = 0
                
                # Process video metrics
                # 3 second views
                video_3_sec_views = 0
                video_thruplay = metrics_data.get('video_thruplay_watched_actions', [])
                if video_thruplay:
                    for action in video_thruplay:
                        if action.get('action_type') == 'video_view':
                            video_3_sec_views = int(action.get('value', 0))
                            break
                metrics['video_3_sec_views'] = video_3_sec_views
                
                # 100% video watches
                video_p100_watched = 0
                video_p100 = metrics_data.get('video_p100_watched_actions', [])
                if video_p100:
                    for action in video_p100:
                        if action.get('action_type') == 'video_view':
                            video_p100_watched = int(action.get('value', 0))
                            break
                metrics['video_p100_watched'] = video_p100_watched
                
                # Calculate Hook Rate and Viewthrough Rate if we have impressions
                if metrics['impressions'] > 0:
                    # Hook Rate: (3-second views / impressions) * 100
                    if video_3_sec_views > 0:
                        metrics['hook_rate'] = (video_3_sec_views / metrics['impressions']) * 100
                    else:
                        metrics['hook_rate'] = 0
                        
                    # Viewthrough Rate: (100% views / impressions) * 100
                    if video_p100_watched > 0:
                        metrics['viewthrough_rate'] = (video_p100_watched / metrics['impressions']) * 100
                    else:
                        metrics['viewthrough_rate'] = 0
                
                return metrics
            else:
                logger.warning(f"No metrics found for ad {ad_id}")
                return {
                    "spend": 0.0,
                    "impressions": 0,
                    "clicks": 0,
                    "conversions": 0,
                    "ctr": 0.0,
                    "cpm": 0.0,
                    "cpa": 0.0,
                    "roas": 0.0,
                    "outbound_clicks": 0,
                    "ctr_destination": 0.0,
                    "video_3_sec_views": 0,
                    "video_p100_watched": 0,
                    "hook_rate": 0.0,
                    "viewthrough_rate": 0.0
                }
                
        except Exception as e:
            logger.exception(f"Error retrieving ad metrics: {str(e)}")
            raise

    def get_detailed_ad_metrics(self, ad_id: str, days: int = DAYS_THRESHOLD) -> Dict[str, Any]:
        """
        Get detailed performance metrics for a specific ad over the specified time period
        
        Args:
            ad_id: Meta Ad ID
            days: Number of days to analyze (default: DAYS_THRESHOLD)
            
        Returns:
            Dict: Detailed performance metrics
        """
        logger.info(f"Getting detailed metrics for ad {ad_id}")
        
        # Calculate date range
        today = datetime.now()
        since_date = today - timedelta(days=days)
        
        # Format dates for API (YYYY-MM-DD format)
        since_date_str = since_date.strftime('%Y-%m-%d')
        today_str = today.strftime('%Y-%m-%d')
        
        # Build URL and params for detailed metrics
        url = f"{self.base_url}/{ad_id}/insights"
        params = {
            "access_token": self.access_token,
            "fields": "spend,impressions,clicks,conversions,conversion_values,ctr,cpm,cpp,"  
                      "cost_per_conversion,cost_per_action_type,conversion_rate_ranking,"
                      "quality_ranking,engagement_rate_ranking,video_play_actions,"
                      "reach,frequency,unique_clicks,unique_ctr,website_ctr,video_p25_watched_actions,"
                      "video_p50_watched_actions,video_p75_watched_actions,video_p95_watched_actions,"
                      "video_p100_watched_actions,video_avg_time_watched_actions",
            "time_range": json.dumps({
                "since": since_date_str,
                "until": today_str
            }),
            "level": "ad"
        }
        
        try:
            response = self._make_api_request(url, params)
            data = response.get('data', [])
            
            if data:
                # Extract the first (should be only) result
                metrics_data = data[0]  
                
                # Extract and format core metrics
                metrics = {
                    "spend": float(metrics_data.get('spend', 0)),
                    "impressions": int(metrics_data.get('impressions', 0)),
                    "clicks": int(metrics_data.get('clicks', 0)),
                    "conversions": int(metrics_data.get('conversions', 0)),
                    "conversion_values": float(metrics_data.get('conversion_values', 0)),
                    "ctr": float(metrics_data.get('ctr', 0)) * 100,  # Convert to percentage
                    "cpm": float(metrics_data.get('cpm', 0)),
                    "cpp": float(metrics_data.get('cpp', 0)),
                    "frequency": float(metrics_data.get('frequency', 0)),
                    "reach": int(metrics_data.get('reach', 0)),
                    "unique_clicks": int(metrics_data.get('unique_clicks', 0)),
                    "unique_ctr": float(metrics_data.get('unique_ctr', 0)) * 100,  # Convert to percentage
                    "quality_ranking": metrics_data.get('quality_ranking', 'UNKNOWN'),
                    "conversion_rate_ranking": metrics_data.get('conversion_rate_ranking', 'UNKNOWN'),
                    "engagement_rate_ranking": metrics_data.get('engagement_rate_ranking', 'UNKNOWN')
                }
                
                # Calculate additional metrics
                if metrics['impressions'] > 0:
                    metrics['cpm'] = (metrics['spend'] / metrics['impressions']) * 1000
                else:
                    metrics['cpm'] = 0
                    
                if metrics['conversions'] > 0:
                    metrics['cpa'] = metrics['spend'] / metrics['conversions']
                else:
                    metrics['cpa'] = 0
                    
                if metrics['spend'] > 0 and metrics['conversion_values'] > 0:
                    metrics['roas'] = metrics['conversion_values'] / metrics['spend']
                else:
                    metrics['roas'] = 0
                
                # Extract video metrics if available
                video_metrics = {}
                video_play_actions = metrics_data.get('video_play_actions', [])
                if video_play_actions:
                    for action in video_play_actions:
                        action_type = action.get('action_type')
                        if action_type == 'video_view':
                            video_metrics['views'] = int(action.get('value', 0))
                
                # Extract video completion rates
                for completion_key in ['video_p25_watched_actions', 'video_p50_watched_actions', 
                                      'video_p75_watched_actions', 'video_p95_watched_actions', 
                                      'video_p100_watched_actions']:
                    actions = metrics_data.get(completion_key, [])
                    if actions:
                        for action in actions:
                            if action.get('action_type') == 'video_view':
                                rate_key = completion_key.replace('video_', '').replace('_watched_actions', '')
                                video_metrics[rate_key] = int(action.get('value', 0))
                
                # Add video metrics if they exist
                if video_metrics:
                    metrics['video'] = video_metrics
                    
                return metrics
            else:
                logger.warning(f"No metrics found for ad {ad_id}")
                return self._empty_metrics_template()
                
        except Exception as e:
            logger.exception(f"Error retrieving detailed ad metrics: {str(e)}")
            raise
    
    def get_ad_creative_details(self, ad_id: str) -> Dict[str, Any]:
        """
        Get creative details for a specific ad
        
        Args:
            ad_id: Meta Ad ID
            
        Returns:
            Dict: Creative details
        """
        logger.info(f"Getting creative details for ad {ad_id}")
        
        url = f"{self.base_url}/{ad_id}"
        params = {
            "access_token": self.access_token,
            "fields": "creative{id,name,title,body,object_type,image_url,video_id,"
                      "thumbnail_url,link_url,call_to_action_type,object_story_spec{link_data{message,"
                      "description,name,image_url,call_to_action{type,value}}}}"
        }
        
        try:
            ad_data = self._make_api_request(url, params)
            creative_data = ad_data.get('creative', {})
            
            if not creative_data:
                logger.warning(f"No creative found for ad {ad_id}")
                return {}
            
            creative_details = {
                "creative_id": creative_data.get('id'),
                "name": creative_data.get('name'),
                "title": creative_data.get('title'),
                "body": creative_data.get('body'),
                "object_type": creative_data.get('object_type'),
                "image_url": creative_data.get('image_url'),
                "video_id": creative_data.get('video_id'),
                "thumbnail_url": creative_data.get('thumbnail_url'),
                "link_url": creative_data.get('link_url'),
                "call_to_action_type": creative_data.get('call_to_action_type')
            }
            
            # Extract additional details from object_story_spec if available
            story_spec = creative_data.get('object_story_spec', {})
            link_data = story_spec.get('link_data', {})
            if link_data:
                creative_details.update({
                    "headline": link_data.get('name'),
                    "message": link_data.get('message'),
                    "description": link_data.get('description'),
                    "image_url": link_data.get('image_url') or creative_details['image_url']
                })
                
                # Get call to action details
                cta = link_data.get('call_to_action', {})
                if cta:
                    creative_details['call_to_action'] = {
                        "type": cta.get('type'),
                        "value": cta.get('value')
                    }
            
            return creative_details
            
        except Exception as e:
            logger.exception(f"Error retrieving creative details: {str(e)}")
            raise
    
    def get_benchmark_data(self) -> Dict[str, Any]:
        """
        Get benchmark data from Meta for the ad account
        Note: In a real implementation, this would fetch actual benchmark data from Meta
              For MVP, we'll use the benchmarks defined in the config file
        
        Returns:
            Dict: Benchmark data
        """
        logger.info("Getting benchmark data")
        
        # For a real implementation, this would call Meta's API to get benchmarks
        # For now, this is just a placeholder to show where actual benchmark data would be fetched
        # The actual benchmarks are defined in the config/benchmarks.json file
        
        try:
            # In a real implementation, you would call Meta's API here
            # For example, fetch benchmark data for the industry or region
            
            # Placeholder implementation
            return {
                "success": True,
                "message": "Using benchmarks from configuration file"
            }
            
        except Exception as e:
            logger.exception(f"Error getting benchmark data: {str(e)}")
            return {
                "success": False,
                "message": f"Error: {str(e)}"
            }
    
    def _empty_metrics_template(self) -> Dict[str, Any]:
        """
        Return an empty metrics template with default values
        
        Returns:
            Dict: Empty metrics template
        """
        return {
            "spend": 0.0,
            "impressions": 0,
            "clicks": 0,
            "conversions": 0,
            "conversion_values": 0.0,
            "ctr": 0.0,
            "cpm": 0.0,
            "cpp": 0.0,
            "cpa": 0.0,
            "roas": 0.0,
            "frequency": 0.0,
            "reach": 0,
            "unique_clicks": 0,
            "unique_ctr": 0.0,
            "quality_ranking": "UNKNOWN",
            "conversion_rate_ranking": "UNKNOWN",
            "engagement_rate_ranking": "UNKNOWN"
        }

    def get_demographic_breakdown(self, ad_id: str, days: int = DAYS_THRESHOLD) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get demographic breakdowns (age and gender) for a specific ad
        
        Args:
            ad_id: Meta Ad ID
            days: Number of days to analyze (default: DAYS_THRESHOLD)
            
        Returns:
            Dict: Demographic breakdowns by age and gender
        """
        logger.info(f"Getting demographic breakdown for ad {ad_id}")
        
        # Calculate date range
        today = datetime.now()
        since_date = today - timedelta(days=days)
        
        # Format dates for API (YYYY-MM-DD format)
        since_date_str = since_date.strftime('%Y-%m-%d')
        today_str = today.strftime('%Y-%m-%d')
        
        # Get age and gender breakdown
        age_gender_breakdown = self._get_breakdown(ad_id, since_date_str, today_str, breakdowns=['age', 'gender'])
        
        # Format the breakdown data
        result = {
            "age_gender": []
        }
        
        # Process age_gender breakdown
        if age_gender_breakdown:
            for item in age_gender_breakdown:
                breakdown_item = {
                    "age": item.get('age'),
                    "gender": item.get('gender'),
                    "spend": float(item.get('spend', 0)),
                    "impressions": int(item.get('impressions', 0)),
                    "clicks": int(item.get('clicks', 0)),
                    "conversions": int(item.get('conversions', 0)),
                    "ctr": float(item.get('ctr', 0)) * 100,  # Convert to percentage
                    "cpm": float(item.get('cpm', 0)),
                }
                
                # Calculate additional metrics
                if breakdown_item['conversions'] > 0:
                    breakdown_item['cpa'] = breakdown_item['spend'] / breakdown_item['conversions']
                else:
                    breakdown_item['cpa'] = 0
                
                result['age_gender'].append(breakdown_item)
        
        # Get platform/device breakdown
        platform_breakdown = self._get_breakdown(ad_id, since_date_str, today_str, breakdowns=['publisher_platform', 'platform_position', 'impression_device'])
        
        # Format the platform breakdown data
        if platform_breakdown:
            result['platform'] = []
            for item in platform_breakdown:
                breakdown_item = {
                    "platform": item.get('publisher_platform'),
                    "position": item.get('platform_position'),
                    "device": item.get('impression_device'),
                    "spend": float(item.get('spend', 0)),
                    "impressions": int(item.get('impressions', 0)),
                    "clicks": int(item.get('clicks', 0)),
                    "conversions": int(item.get('conversions', 0)),
                    "ctr": float(item.get('ctr', 0)) * 100,  # Convert to percentage
                }
                result['platform'].append(breakdown_item)
        
        return result
    
    def _get_breakdown(self, ad_id: str, since_date: str, until_date: str, 
                      breakdowns: List[str]) -> List[Dict[str, Any]]:
        """
        Helper method to get breakdown data for an ad
        
        Args:
            ad_id: Meta Ad ID
            since_date: Start date in YYYY-MM-DD format
            until_date: End date in YYYY-MM-DD format
            breakdowns: List of breakdown dimensions
            
        Returns:
            List[Dict]: List of breakdown data items
        """
        # Convert breakdowns list to comma-separated string
        breakdown_str = ",".join(breakdowns)
        
        # Build URL and params
        url = f"{self.base_url}/{ad_id}/insights"
        params = {
            "access_token": self.access_token,
            "fields": "spend,impressions,clicks,conversions,ctr,cpm,cost_per_conversion",
            "time_range": json.dumps({
                "since": since_date,
                "until": until_date
            }),
            "breakdowns": breakdown_str,
            "level": "ad"
        }
        
        try:
            # This might return a lot of data with multiple pages
            result = self._handle_pagination(url, params)
            return result
        except Exception as e:
            logger.exception(f"Error getting breakdown data: {str(e)}")
            return []
    
    def get_complete_ad_data(self, ad_id: str, days: int = DAYS_THRESHOLD) -> Dict[str, Any]:
        """
        Get complete ad data including metrics, creative, and breakdowns
        
        Args:
            ad_id: Meta Ad ID
            days: Number of days to analyze (default: DAYS_THRESHOLD)
            
        Returns:
            Dict: Complete ad data
        """
        logger.info(f"Getting complete data for ad {ad_id}")
        
        try:
            # Get ad details
            url = f"{self.base_url}/{ad_id}"
            params = {
                "access_token": self.access_token,
                "fields": "id,name,campaign{id,name},adset{id,name},status,created_time"
            }
            
            ad_details = self._make_api_request(url, params)
            
            # Build complete ad data
            ad_data = {
                "ad_id": ad_id,
                "ad_name": ad_details.get('name'),
                "campaign_id": ad_details.get('campaign', {}).get('id'),
                "campaign_name": ad_details.get('campaign', {}).get('name'),
                "adset_id": ad_details.get('adset', {}).get('id'),
                "adset_name": ad_details.get('adset', {}).get('name'),
                "status": ad_details.get('status'),
                "created_time": ad_details.get('created_time'),
            }
            
            # Get metrics
            metrics = self.get_detailed_ad_metrics(ad_id, days)
            ad_data["metrics"] = metrics
            
            # Get creative details
            creative = self.get_ad_creative_details(ad_id)
            ad_data["creative"] = creative
            
            # Get demographic breakdowns
            breakdowns = self.get_demographic_breakdown(ad_id, days)
            ad_data["breakdowns"] = breakdowns
            
            return ad_data
            
        except Exception as e:
            logger.exception(f"Error getting complete ad data: {str(e)}")
            raise

    # ============================================
    # NEW METHODS FOR SPECIFIC METRICS
    # ============================================
    
    def get_any_recent_ads(self, days: int = 30, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get any recent ads from the account (for testing purposes)
        
        Args:
            days: Number of days to look back
            limit: Maximum number of ads to return
            
        Returns:
            List[Dict]: List of recent ads
        """
        logger.info(f"Getting any recent ads from the last {days} days (limited to {limit})")
        
        # Calculate date range
        today = datetime.now()
        since_date = today - timedelta(days=days)
        since_date_str = since_date.strftime('%Y-%m-%d')
        
        # Build URL and params
        url = f"{self.base_url}/act_{self.ad_account_id}/ads"
        params = {
            "access_token": self.access_token,
            "fields": "id,name,campaign{id,name},adset{id,name},created_time,status",
            "time_range": json.dumps({
                "since": since_date_str,
                "until": today.strftime('%Y-%m-%d')
            }),
            "limit": limit  # Restrict to exactly the number we need
        }
        
        try:
            # Use a non-paginated request to avoid retrieving too much data
            response = self._make_api_request(url, params)
            ads = response.get('data', [])
            
            # Format the response
            formatted_ads = []
            for ad in ads[:limit]:  # Additional safety to ensure we don't process more than limit
                formatted_ads.append({
                    "ad_id": ad.get('id'),
                    "ad_name": ad.get('name'),
                    "campaign_name": ad.get('campaign', {}).get('name'),
                    "created_time": ad.get('created_time'),
                    "status": ad.get('status')
                })
            
            logger.info(f"Retrieved {len(formatted_ads)} ads for testing")
            return formatted_ads
            
        except Exception as e:
            logger.exception(f"Error getting recent ads: {str(e)}")
            return []

    def get_comprehensive_ad_metrics(self, ad_id: str, days: int = 7) -> Dict[str, Any]:
        """
        Get comprehensive metrics including all required fields:
        - Date Launched
        - Media Spend
        - CPM
        - Impressions
        - 3 second views
        - 100% video watches
        - CTR (destination)
        - CPC
        - Registrations
        - CPR (Cost Per Registration)
        
        Args:
            ad_id: Meta Ad ID
            days: Number of days to analyze
            
        Returns:
            Dict: Comprehensive metrics
        """
        logger.info(f"Getting comprehensive metrics for ad {ad_id}")
        
        # First get the ad creation date (Date Launched)
        try:
            ad_url = f"{self.base_url}/{ad_id}"
            ad_params = {
                "access_token": self.access_token,
                "fields": "created_time,name"
            }
            ad_info = self._make_api_request(ad_url, ad_params)
            date_launched = ad_info.get('created_time')
        except Exception as e:
            logger.error(f"Error getting ad creation date: {str(e)}")
            date_launched = None
        
        # Calculate date range for metrics
        today = datetime.now()
        since_date = today - timedelta(days=days)
        since_date_str = since_date.strftime('%Y-%m-%d')
        today_str = today.strftime('%Y-%m-%d')
        
        # Build URL and params for insights
        url = f"{self.base_url}/{ad_id}/insights"
        
        # Request all needed fields
        fields = [
            "spend",  # Media Spend
            "cpm",    # CPM
            "impressions",  # Impressions
            "cpc",    # CPC
            "clicks",
            "outbound_clicks",  # For CTR destination
            "outbound_clicks_ctr",  # CTR destination
            "video_thruplay_watched_actions",  # 3 second views
            "video_p100_watched_actions",  # 100% video watches
            "actions",  # For registrations
            "cost_per_action_type",  # For CPR
            "website_ctr"  # Website CTR
            # Removed link_click_ctr as it's no longer supported by Meta API
        ]
        
        params = {
            "access_token": self.access_token,
            "fields": ",".join(fields),
            "time_range": json.dumps({
                "since": since_date_str,
                "until": today_str
            }),
            "level": "ad"
        }
        
        try:
            response = self._make_api_request(url, params)
            data = response.get('data', [])
            
            if data:
                metrics_data = data[0]
                
                # Build comprehensive metrics
                metrics = {
                    "date_launched": date_launched,
                    "spend": float(metrics_data.get('spend', 0)),
                    "cpm": float(metrics_data.get('cpm', 0)),
                    "impressions": int(metrics_data.get('impressions', 0)),
                    "cpc": float(metrics_data.get('cpc', 0)),
                    "clicks": int(metrics_data.get('clicks', 0)),
                }
                
                # Handle outbound_clicks which could be a list or a number
                outbound_clicks = metrics_data.get('outbound_clicks', 0)
                if isinstance(outbound_clicks, list):
                    # If it's a list, sum the values if there are any, otherwise use 0
                    outbound_clicks_sum = 0
                    for item in outbound_clicks:
                        if isinstance(item, dict) and 'value' in item:
                            outbound_clicks_sum += int(float(item.get('value', 0)))
                    metrics['outbound_clicks'] = outbound_clicks_sum
                else:
                    # If it's a scalar value, convert to int
                    metrics['outbound_clicks'] = int(float(outbound_clicks) if outbound_clicks else 0)
                
                # CTR (destination) - prefer outbound_clicks_ctr, fallback to website_ctr or link_click_ctr
                ctr_destination = metrics_data.get('outbound_clicks_ctr', [])
                if ctr_destination and isinstance(ctr_destination, list) and len(ctr_destination) > 0:
                    metrics['ctr_destination'] = float(ctr_destination[0].get('value', 0))
                elif metrics_data.get('website_ctr'):
                    metrics['ctr_destination'] = float(metrics_data.get('website_ctr', 0))
                # Removed link_click_ctr fallback as it's no longer supported by Meta API
                else:
                    # Calculate manually if we have the data
                    if metrics['impressions'] > 0 and metrics['outbound_clicks'] > 0:
                        metrics['ctr_destination'] = (metrics['outbound_clicks'] / metrics['impressions']) * 100
                    else:
                        metrics['ctr_destination'] = 0
                
                # 3 second video views
                video_3_sec = 0
                video_thruplay = metrics_data.get('video_thruplay_watched_actions', [])
                if video_thruplay:
                    for action in video_thruplay:
                        if action.get('action_type') == 'video_view':
                            video_3_sec = int(action.get('value', 0))
                            break
                metrics['video_3_sec_views'] = video_3_sec
                
                # 100% video watches
                video_100 = 0
                video_p100 = metrics_data.get('video_p100_watched_actions', [])
                if video_p100:
                    for action in video_p100:
                        if action.get('action_type') == 'video_view':
                            video_100 = int(action.get('value', 0))
                            break
                metrics['video_p100_watched'] = video_100
                
                # Registrations (look for lead or complete_registration actions)
                registrations = 0
                actions = metrics_data.get('actions', [])
                if actions:
                    for action in actions:
                        action_type = action.get('action_type')
                        if action_type in ['lead', 'complete_registration', 'lead_grouped']:
                            registrations = int(action.get('value', 0))
                            break
                metrics['registrations'] = registrations
                
                # CPR (Cost Per Registration)
                cpr = 0
                cost_per_action = metrics_data.get('cost_per_action_type', [])
                if cost_per_action:
                    for cost in cost_per_action:
                        action_type = cost.get('action_type')
                        if action_type in ['lead', 'complete_registration', 'lead_grouped']:
                            cpr = float(cost.get('value', 0))
                            break
                
                # Calculate CPR manually if not provided
                if cpr == 0 and registrations > 0 and metrics['spend'] > 0:
                    cpr = metrics['spend'] / registrations
                
                metrics['cpr'] = cpr
                
                return metrics
            else:
                logger.warning(f"No metrics data found for ad {ad_id}")
                return None
                
        except Exception as e:
            logger.exception(f"Error retrieving comprehensive metrics: {str(e)}")
            return None

    def get_metrics_with_demographics(self, ad_id: str, days: int = 7) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get metrics broken down by demographics:
        - Age groups
        - Gender
        - Age + Gender combinations
        
        Args:
            ad_id: Meta Ad ID
            days: Number of days to analyze
            
        Returns:
            Dict: Metrics broken down by demographics
        """
        logger.info(f"Getting demographic breakdowns for ad {ad_id}")
        
        # Calculate date range
        today = datetime.now()
        since_date = today - timedelta(days=days)
        since_date_str = since_date.strftime('%Y-%m-%d')
        today_str = today.strftime('%Y-%m-%d')
        
        result = {}
        
        # Fields to request for each breakdown
        fields = [
            "spend",
            "cpm",
            "impressions",
            "cpc",
            "clicks",
            "outbound_clicks",
            "outbound_clicks_ctr",
            "video_thruplay_watched_actions",
            "video_p100_watched_actions",
            "actions",
            "cost_per_action_type"
        ]
        
        # 1. Age breakdown
        logger.info("Getting age breakdown...")
        age_breakdown = self._get_demographic_breakdown(
            ad_id, since_date_str, today_str, fields, ['age']
        )
        if age_breakdown:
            result['age'] = self._format_breakdown_data(age_breakdown, ['age'])
        
        # 2. Gender breakdown
        logger.info("Getting gender breakdown...")
        gender_breakdown = self._get_demographic_breakdown(
            ad_id, since_date_str, today_str, fields, ['gender']
        )
        if gender_breakdown:
            result['gender'] = self._format_breakdown_data(gender_breakdown, ['gender'])
        
        # 3. Age + Gender combination
        logger.info("Getting age + gender combination breakdown...")
        age_gender_breakdown = self._get_demographic_breakdown(
            ad_id, since_date_str, today_str, fields, ['age', 'gender']
        )
        if age_gender_breakdown:
            result['age_gender'] = self._format_breakdown_data(age_gender_breakdown, ['age', 'gender'])
        
        return result

    def _get_demographic_breakdown(self, ad_id: str, since_date: str, until_date: str, 
                                  fields: List[str], breakdowns: List[str]) -> List[Dict[str, Any]]:
        """
        Helper method to get breakdown data for specific demographics
        
        Args:
            ad_id: Meta Ad ID
            since_date: Start date in YYYY-MM-DD format
            until_date: End date in YYYY-MM-DD format
            fields: List of fields to request
            breakdowns: List of breakdown dimensions
            
        Returns:
            List[Dict]: Raw breakdown data from API
        """
        url = f"{self.base_url}/{ad_id}/insights"
        params = {
            "access_token": self.access_token,
            "fields": ",".join(fields),
            "time_range": json.dumps({
                "since": since_date,
                "until": until_date
            }),
            "breakdowns": ",".join(breakdowns),
            "level": "ad"
        }
        
        try:
            response = self._make_api_request(url, params)
            return response.get('data', [])
        except Exception as e:
            logger.error(f"Error getting breakdown for {breakdowns}: {str(e)}")
            return []

    def _format_breakdown_data(self, raw_data: List[Dict[str, Any]], 
                              breakdown_fields: List[str]) -> List[Dict[str, Any]]:
        """
        Format raw breakdown data into structured metrics
        
        Args:
            raw_data: Raw data from API
            breakdown_fields: Fields used for breakdown (e.g., ['age', 'gender'])
            
        Returns:
            List[Dict]: Formatted breakdown data
        """
        formatted_data = []
        
        for item in raw_data:
            # Start with demographic identifiers
            formatted_item = {}
            for field in breakdown_fields:
                formatted_item[field] = item.get(field, 'unknown')
            
            # Add core metrics
            formatted_item.update({
                "spend": float(item.get('spend', 0)),
                "cpm": float(item.get('cpm', 0)),
                "impressions": int(item.get('impressions', 0)),
                "cpc": float(item.get('cpc', 0)),
                "clicks": int(item.get('clicks', 0)),
                "outbound_clicks": 0,  # Initialize to 0 and handle below
            })
            
            # Handle outbound_clicks which could be a list
            outbound_clicks = item.get('outbound_clicks', 0)
            if isinstance(outbound_clicks, list):
                # If it's a list, sum the values if there are any
                outbound_clicks_sum = 0
                for outbound_item in outbound_clicks:
                    if isinstance(outbound_item, dict) and 'value' in outbound_item:
                        outbound_clicks_sum += int(float(outbound_item.get('value', 0)))
                formatted_item['outbound_clicks'] = outbound_clicks_sum
            else:
                # If it's a scalar value, convert to int
                formatted_item['outbound_clicks'] = int(float(outbound_clicks) if outbound_clicks else 0)
            
            # CTR (destination)
            ctr_destination = item.get('outbound_clicks_ctr', [])
            if ctr_destination and isinstance(ctr_destination, list) and len(ctr_destination) > 0:
                formatted_item['ctr_destination'] = float(ctr_destination[0].get('value', 0))
            else:
                # Calculate manually if we have the data
                if formatted_item['impressions'] > 0 and formatted_item['outbound_clicks'] > 0:
                    formatted_item['ctr_destination'] = (formatted_item['outbound_clicks'] / formatted_item['impressions']) * 100
                else:
                    formatted_item['ctr_destination'] = 0
            
            # 3 second video views
            video_3_sec = 0
            video_thruplay = item.get('video_thruplay_watched_actions', [])
            if video_thruplay:
                for action in video_thruplay:
                    if action.get('action_type') == 'video_view':
                        video_3_sec = int(action.get('value', 0))
                        break
            formatted_item['video_3_sec_views'] = video_3_sec
            
            # 100% video watches
            video_100 = 0
            video_p100 = item.get('video_p100_watched_actions', [])
            if video_p100:
                for action in video_p100:
                    if action.get('action_type') == 'video_view':
                        video_100 = int(action.get('value', 0))
                        break
            formatted_item['video_p100_watched'] = video_100
            
            # Registrations
            registrations = 0
            actions = item.get('actions', [])
            if actions:
                for action in actions:
                    action_type = action.get('action_type')
                    if action_type in ['lead', 'complete_registration', 'lead_grouped']:
                        registrations = int(action.get('value', 0))
                        break
            formatted_item['registrations'] = registrations
            
            # CPR (Cost Per Registration)
            cpr = 0
            cost_per_action = item.get('cost_per_action_type', [])
            if cost_per_action:
                for cost in cost_per_action:
                    action_type = cost.get('action_type')
                    if action_type in ['lead', 'complete_registration', 'lead_grouped']:
                        cpr = float(cost.get('value', 0))
                        break
            
            # Calculate CPR manually if not provided
            if cpr == 0 and registrations > 0 and formatted_item['spend'] > 0:
                cpr = formatted_item['spend'] / registrations
            
            formatted_item['cpr'] = cpr
            
            formatted_data.append(formatted_item)
        
        return formatted_data


# Example usage
if __name__ == "__main__":
    client = MetaApiClient()
    if client.test_connection():
        account_info = client.get_account_info()
        print(f"Account Name: {account_info.get('name')}")
        print(f"Currency: {account_info.get('currency')}")
        
        # Get eligible ads
        eligible_ads = client.get_eligible_ads()
        print(f"\nFound {len(eligible_ads)} eligible ads")
        
        # Print details of first ad
        if eligible_ads:
            ad = eligible_ads[0]
            print(f"\nAd: {ad['ad_name']}")
            print(f"Campaign: {ad['campaign_name']}")
            print(f"Created: {ad['created_time']}")
            print(f"Status: {ad['status']}")
            print(f"\nMetrics:")
            print(f"  Spend: £{ad['metrics']['spend']:.2f}")
            print(f"  Impressions: {ad['metrics']['impressions']}")
            print(f"  Clicks: {ad['metrics']['clicks']}")
            print(f"  CTR: {ad['metrics']['ctr']:.2f}%")
            
            # Get complete data for this ad
            print(f"\nGetting complete data for ad {ad['ad_id']}...")
            complete_data = client.get_complete_ad_data(ad['ad_id'])
            
            # Print demographic breakdown
            if 'breakdowns' in complete_data and 'age_gender' in complete_data['breakdowns']:
                print(f"\nDemographic Breakdown:")
                for segment in complete_data['breakdowns']['age_gender']:
                    print(f"  {segment['age']} {segment['gender']}: "
                          f"£{segment['spend']:.2f} spend, {segment['conversions']} conversions")
            
            # Print creative details
            if 'creative' in complete_data:
                creative = complete_data['creative']
                print(f"\nCreative:")
                print(f"  Headline: {creative.get('headline', 'N/A')}")
                print(f"  Body: {creative.get('message', 'N/A')}")
                print(f"  Image URL: {creative.get('image_url', 'N/A')}")