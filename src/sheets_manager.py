#!/usr/bin/env python3
"""
Sheets Manager

This module handles all interactions with Google Sheets for the Creative Analysis Tool,
including creating and updating spreadsheets with ad performance data and insights.
"""

import logging
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Import settings from config
import sys

# Add project root to path if running as script
if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, project_root)

from config.settings import SHEETS_SPREADSHEET_ID

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SheetsManager:
    """Manages Google Sheets operations for Creative Analysis Tool"""
    
    # Tab names for the spreadsheet
    DASHBOARD_TAB = "Dashboard"
    AD_DETAILS_TAB = "Ad Details"
    SEGMENTS_TAB = "Segments"
    
    # Column definitions for each tab
    DASHBOARD_COLUMNS = [
        "Date", "Ads Analyzed", "Avg Performance Score", "Top Performer", 
        "Score", "Bottom Performer", "Score"
    ]
    
    AD_DETAILS_COLUMNS = [
        "Ad ID", "Ad Name", "Campaign", "Created Date", "Analysis Date", "Spend (£)",
        "Impressions", "Clicks", "Conversions", "CTR (%)", "CPA (£)", "ROAS", "CPM (£)",
        "Performance vs Benchmark (%)", "Rating", "Best Segment", "Creative Preview"
    ]
    
    SEGMENTS_COLUMNS = [
        "Ad ID", "Ad Name", "Segment", "Spend (£)", "Impressions", "Clicks",
        "Conversions", "CTR (%)", "CPA (£)", "Performance (%)"
    ]
    
    def __init__(
        self,
        spreadsheet_id: Optional[str] = SHEETS_SPREADSHEET_ID,
        credentials_path: Optional[str] = None
    ):
        """
        Initialize the Sheets Manager
        
        Args:
            spreadsheet_id: ID of the Google Sheet to use (from settings or .env)
            credentials_path: Path to service account credentials file
        """
        self.spreadsheet_id = spreadsheet_id
        self.credentials_path = credentials_path
        
        # Initialize sheets client
        self.service = self._authenticate()
        
        # Create or validate spreadsheet
        if not self.spreadsheet_id:
            logger.info("No spreadsheet ID provided, creating a new spreadsheet")
            self.spreadsheet_id = self._create_spreadsheet("Creative Performance Analysis")
        
        logger.info(f"Sheets Manager initialized with spreadsheet ID: {self.spreadsheet_id}")
        
        # Ensure required tabs exist
        self._ensure_tabs_exist()
    
    def _authenticate(self):
        """
        Authenticate with Google Sheets API
        
        Returns:
            Google Sheets API service
        
        Raises:
            Exception: If authentication fails
        """
        try:
            # If credentials file is provided, use service account
            if self.credentials_path:
                credentials = Credentials.from_service_account_file(
                    self.credentials_path,
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
                service = build('sheets', 'v4', credentials=credentials)
                logger.info("Authenticated with service account")
            
            # Otherwise use application default credentials
            else:
                from google.oauth2 import service_account
                import os

                # Try to use GOOGLE_APPLICATION_CREDENTIALS environment variable
                credentials = None
                
                try:
                    credentials = service_account.Credentials.from_service_account_info(
                        info=json.loads(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '{}')),
                        scopes=['https://www.googleapis.com/auth/spreadsheets']
                    )
                except (json.JSONDecodeError, TypeError):
                    # If not a valid JSON string, try as a file path
                    if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
                        try:
                            credentials = service_account.Credentials.from_service_account_file(
                                os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'),
                                scopes=['https://www.googleapis.com/auth/spreadsheets']
                            )
                        except Exception as e:
                            logger.warning(f"Could not load credentials from file: {e}")
                
                # Fall back to application default credentials
                if not credentials:
                    from google.auth import default
                    credentials, _ = default(scopes=['https://www.googleapis.com/auth/spreadsheets'])
                    logger.info("Using application default credentials")
                
                service = build('sheets', 'v4', credentials=credentials)
            
            return service
            
        except Exception as e:
            logger.exception(f"Authentication failed: {str(e)}")
            raise
    
    def _create_spreadsheet(self, title: str) -> str:
        """
        Create a new Google Sheet
        
        Args:
            title: Title of the new spreadsheet
            
        Returns:
            str: ID of the created spreadsheet
            
        Raises:
            Exception: If spreadsheet creation fails
        """
        try:
            spreadsheet = {
                'properties': {
                    'title': title
                }
            }
            
            request = self.service.spreadsheets().create(body=spreadsheet)
            response = request.execute()
            
            spreadsheet_id = response['spreadsheetId']
            logger.info(f"Created new spreadsheet with ID: {spreadsheet_id}")
            
            return spreadsheet_id
            
        except Exception as e:
            logger.exception(f"Failed to create spreadsheet: {str(e)}")
            raise
    
    def _ensure_tabs_exist(self) -> None:
        """
        Ensure that all required tabs exist in the spreadsheet, creating them if not
        
        Raises:
            Exception: If tab creation fails
        """
        try:
            # Get existing sheets
            sheet_metadata = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            sheets = sheet_metadata.get('sheets', [])
            existing_titles = [sheet.get('properties', {}).get('title') for sheet in sheets]
            
            # Check if required tabs exist and create if not
            required_tabs = [self.DASHBOARD_TAB, self.AD_DETAILS_TAB, self.SEGMENTS_TAB]
            requests = []
            
            for tab in required_tabs:
                if tab not in existing_titles:
                    requests.append({
                        'addSheet': {
                            'properties': {
                                'title': tab
                            }
                        }
                    })
            
            # Execute batch update if any tabs need to be created
            if requests:
                body = {'requests': requests}
                self.service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=body
                ).execute()
                logger.info(f"Created {len(requests)} missing tabs")
            
            # Initialize columns for each tab if not already populated
            self._initialize_columns(self.DASHBOARD_TAB, self.DASHBOARD_COLUMNS)
            self._initialize_columns(self.AD_DETAILS_TAB, self.AD_DETAILS_COLUMNS)
            self._initialize_columns(self.SEGMENTS_TAB, self.SEGMENTS_COLUMNS)
            
        except Exception as e:
            logger.exception(f"Failed to ensure tabs exist: {str(e)}")
            raise
    
    def _initialize_columns(self, tab_name: str, columns: List[str]) -> None:
        """
        Initialize column headers for a tab if not already populated
        
        Args:
            tab_name: Name of the tab to initialize
            columns: List of column headers
            
        Raises:
            Exception: If initialization fails
        """
        try:
            # Check if headers already exist
            range_name = f"{tab_name}!A1:{chr(65 + len(columns) - 1)}1"
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [[]])
            
            # If no headers or incomplete headers, update them
            if not values or not values[0] or len(values[0]) < len(columns):
                body = {
                    'values': [columns]
                }
                
                self.service.spreadsheets().values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=range_name,
                    valueInputOption='RAW',
                    body=body
                ).execute()
                
                # Format header row
                self._format_header_row(tab_name, len(columns))
                logger.info(f"Initialized column headers for tab {tab_name}")
            
        except Exception as e:
            logger.exception(f"Failed to initialize columns for tab {tab_name}: {str(e)}")
            raise
    
    def _format_header_row(self, tab_name: str, num_columns: int) -> None:
        """
        Format the header row with bold text and background color
        
        Args:
            tab_name: Name of the tab to format
            num_columns: Number of columns in the header
            
        Raises:
            Exception: If formatting fails
        """
        try:
            # Get sheet ID for the tab
            sheet_metadata = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            sheets = sheet_metadata.get('sheets', [])
            sheet_id = None
            
            for sheet in sheets:
                if sheet.get('properties', {}).get('title') == tab_name:
                    sheet_id = sheet.get('properties', {}).get('sheetId')
                    break
            
            if not sheet_id:
                logger.warning(f"Could not find sheet ID for tab {tab_name}")
                return
            
            # Apply formatting
            requests = [
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': 0,
                            'endColumnIndex': num_columns
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {
                                    'red': 0.2,
                                    'green': 0.2,
                                    'blue': 0.2
                                },
                                'textFormat': {
                                    'bold': True,
                                    'foregroundColor': {
                                        'red': 1.0,
                                        'green': 1.0,
                                        'blue': 1.0
                                    }
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(backgroundColor,textFormat)'
                    }
                },
                {
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'COLUMNS',
                            'startIndex': 0,
                            'endIndex': num_columns
                        },
                        'properties': {
                            'pixelSize': 150
                        },
                        'fields': 'pixelSize'
                    }
                },
                {
                    'updateDimensionProperties': {
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'ROWS',
                            'startIndex': 0,
                            'endIndex': 1
                        },
                        'properties': {
                            'pixelSize': 30
                        },
                        'fields': 'pixelSize'
                    }
                }
            ]
            
            body = {'requests': requests}
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body
            ).execute()
            
        except Exception as e:
            logger.exception(f"Failed to format header row for tab {tab_name}: {str(e)}")
            raise
    
    def get_spreadsheet_url(self) -> str:
        """
        Get the URL for the Google Sheet
        
        Returns:
            str: URL to access the spreadsheet
        """
        return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/edit"
    
    def update_dashboard(self, summary_data: Dict[str, Any]) -> bool:
        """
        Update the Dashboard tab with summary metrics
        
        Args:
            summary_data: Dictionary containing summary metrics
                - date: Analysis date (str in YYYY-MM-DD format)
                - ads_analyzed: Number of ads analyzed (int)
                - avg_performance_score: Average performance score (float)
                - top_performers: List of top performing ads [{"ad_id": str, "ad_name": str, "score": float}, ...]
                - bottom_performers: List of bottom performing ads [{"ad_id": str, "ad_name": str, "score": float}, ...]
                
        Returns:
            bool: True if update was successful
            
        Raises:
            Exception: If update fails
        """
        logger.info("Updating Dashboard tab with summary metrics")
        
        try:
            # Format data for dashboard row
            date_str = summary_data.get('date', datetime.now().strftime('%Y-%m-%d'))
            ads_analyzed = summary_data.get('ads_analyzed', 0)
            avg_performance_score = summary_data.get('avg_performance_score', 0)
            
            # Get top performer
            top_performers = summary_data.get('top_performers', [])
            top_performer = f"{top_performers[0].get('ad_name', 'Unknown')}" if top_performers else "None"
            top_score = top_performers[0].get('score', 0) if top_performers else 0
            
            # Get bottom performer
            bottom_performers = summary_data.get('bottom_performers', [])
            bottom_performer = f"{bottom_performers[0].get('ad_name', 'Unknown')}" if bottom_performers else "None"
            bottom_score = bottom_performers[0].get('score', 0) if bottom_performers else 0
            
            # Create row values
            row_values = [
                date_str,
                ads_analyzed,
                avg_performance_score,
                top_performer,
                top_score,
                bottom_performer,
                bottom_score
            ]
            
            # Find the next empty row in the Dashboard tab
            range_name = f"{self.DASHBOARD_TAB}!A:A"
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            next_row = len(values) + 1
            
            # Check if today's date already exists in the first column
            today_row = None
            for i, row in enumerate(values):
                if row and row[0] == date_str:
                    today_row = i + 1  # 1-based indexing for Sheets API
                    break
            
            # Determine whether to insert a new row or update existing
            update_row = today_row if today_row else next_row
            range_name = f"{self.DASHBOARD_TAB}!A{update_row}:{chr(65 + len(row_values) - 1)}{update_row}"
            
            # Update or insert the row
            body = {
                'values': [row_values]
            }
            
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            
            # If this is a new row, apply formatting
            if not today_row:
                self._format_dashboard_row(update_row)
            
            logger.info(f"Dashboard updated successfully at row {update_row}")
            
            # Update dashboard summary visualization (recent trend chart)
            self._update_dashboard_summary()
            
            return True
            
        except Exception as e:
            logger.exception(f"Failed to update Dashboard: {str(e)}")
            return False
    
    def _update_dashboard_summary(self) -> None:
        """
        Update the dashboard summary visualization (trend charts)
        
        This creates or updates charts showing performance trends over time
        """
        try:
            # Get sheet ID for dashboard tab
            sheet_metadata = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            sheets = sheet_metadata.get('sheets', [])
            sheet_id = None
            
            for sheet in sheets:
                if sheet.get('properties', {}).get('title') == self.DASHBOARD_TAB:
                    sheet_id = sheet.get('properties', {}).get('sheetId')
                    break
            
            if not sheet_id:
                logger.warning("Could not find dashboard sheet ID for charts")
                return
            
            # Check if charts already exist (to avoid duplicating them)
            charts_response = self.service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id,
                ranges=[self.DASHBOARD_TAB],
                fields='sheets.charts'
            ).execute()
            
            dashboard_sheet = charts_response.get('sheets', [])[0]
            existing_charts = dashboard_sheet.get('charts', [])
            
            # If charts already exist, don't recreate them
            if existing_charts:
                logger.info("Dashboard charts already exist, skipping creation")
                return
            
            # Create performance trend chart
            requests = [
                {
                    'addChart': {
                        'chart': {
                            'spec': {
                                'title': 'Performance Trend',
                                'basicChart': {
                                    'chartType': 'LINE',
                                    'legendPosition': 'BOTTOM_LEGEND',
                                    'axis': [
                                        {
                                            'position': 'BOTTOM_AXIS',
                                            'title': 'Date'
                                        },
                                        {
                                            'position': 'LEFT_AXIS',
                                            'title': 'Performance Score'
                                        }
                                    ],
                                    'domains': [
                                        {
                                            'domain': {
                                                'sourceRange': {
                                                    'sources': [
                                                        {
                                                            'sheetId': sheet_id,
                                                            'startRowIndex': 1,  # Skip header row
                                                            'endRowIndex': 15,  # Show up to 14 days of data
                                                            'startColumnIndex': 0,  # Date column
                                                            'endColumnIndex': 1
                                                        }
                                                    ]
                                                }
                                            }
                                        }
                                    ],
                                    'series': [
                                        {
                                            'series': {
                                                'sourceRange': {
                                                    'sources': [
                                                        {
                                                            'sheetId': sheet_id,
                                                            'startRowIndex': 1,  # Skip header row
                                                            'endRowIndex': 15,  # Show up to 14 days of data
                                                            'startColumnIndex': 2,  # Avg performance score column
                                                            'endColumnIndex': 3
                                                        }
                                                    ]
                                                }
                                            },
                                            'targetAxis': 'LEFT_AXIS',
                                            'chartType': 'LINE'
                                        }
                                    ],
                                    'headerCount': 1
                                }
                            },
                            'position': {
                                'overlayPosition': {
                                    'anchorCell': {
                                        'sheetId': sheet_id,
                                        'rowIndex': 1,
                                        'columnIndex': 8  # Position chart to the right of data
                                    },
                                    'offsetXPixels': 10,
                                    'offsetYPixels': 10,
                                    'widthPixels': 600,
                                    'heightPixels': 400
                                }
                            }
                        }
                    }
                }
            ]
            
            # Execute batch update to add charts
            body = {'requests': requests}
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body
            ).execute()
            
            logger.info("Dashboard summary charts created successfully")
            
        except Exception as e:
            logger.exception(f"Failed to update dashboard summary: {str(e)}")
    
    def _format_dashboard_row(self, row_index: int) -> None:
        """
        Apply formatting to a dashboard row
        
        Args:
            row_index: The 1-based row index to format
        """
        try:
            # Get sheet ID for dashboard tab
            sheet_metadata = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            sheets = sheet_metadata.get('sheets', [])
            sheet_id = None
            
            for sheet in sheets:
                if sheet.get('properties', {}).get('title') == self.DASHBOARD_TAB:
                    sheet_id = sheet.get('properties', {}).get('sheetId')
                    break
            
            if not sheet_id:
                logger.warning(f"Could not find sheet ID for tab {self.DASHBOARD_TAB}")
                return
            
            # Apply alternating row background
            background_color = {
                'red': 0.95,
                'green': 0.95,
                'blue': 0.95
            } if row_index % 2 == 0 else {
                'red': 1.0,
                'green': 1.0,
                'blue': 1.0
            }
            
            requests = [
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_index - 1,  # 0-based for API
                            'endRowIndex': row_index,
                            'startColumnIndex': 0,
                            'endColumnIndex': len(self.DASHBOARD_COLUMNS)
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': background_color,
                                'horizontalAlignment': 'RIGHT',
                                'textFormat': {
                                    'bold': False
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)'
                    }
                },
                # Left-align text columns
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_index - 1,
                            'endRowIndex': row_index,
                            'startColumnIndex': 0,  # Date column
                            'endColumnIndex': 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'horizontalAlignment': 'LEFT'
                            }
                        },
                        'fields': 'userEnteredFormat(horizontalAlignment)'
                    }
                },
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_index - 1,
                            'endRowIndex': row_index,
                            'startColumnIndex': 3,  # Top performer column
                            'endColumnIndex': 4
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'horizontalAlignment': 'LEFT'
                            }
                        },
                        'fields': 'userEnteredFormat(horizontalAlignment)'
                    }
                },
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_index - 1,
                            'endRowIndex': row_index,
                            'startColumnIndex': 5,  # Bottom performer column
                            'endColumnIndex': 6
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'horizontalAlignment': 'LEFT'
                            }
                        },
                        'fields': 'userEnteredFormat(horizontalAlignment)'
                    }
                }
            ]
            
            body = {'requests': requests}
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body
            ).execute()
            
        except Exception as e:
            logger.exception(f"Failed to format dashboard row: {str(e)}")
    
    def update_ad_details(self, ad_data: Dict[str, Any], analysis_result: Dict[str, Any]) -> bool:
        """
        Update the Ad Details tab with ad performance data
        
        Args:
            ad_data: Ad data from Meta API
            analysis_result: Performance analysis result
            
        Returns:
            bool: True if update was successful
            
        Raises:
            Exception: If update fails
        """
        logger.info(f"Updating Ad Details tab for ad {ad_data.get('ad_id', 'unknown')}")
        
        try:
            # Extract ad data fields
            ad_id = ad_data.get('ad_id', 'unknown')
            ad_name = ad_data.get('ad_name', 'Unknown Ad')
            campaign_name = ad_data.get('campaign_name', 'Unknown Campaign')
            
            # Format dates
            created_date = ad_data.get('created_time', '')
            if created_date and 'T' in created_date:
                # Extract just the date part if it's in ISO format
                created_date = created_date.split('T')[0]
                
            analysis_date = datetime.now().strftime('%Y-%m-%d')
            
            # Extract metrics
            metrics = ad_data.get('metrics', {})
            spend = metrics.get('spend', 0)
            impressions = metrics.get('impressions', 0)
            clicks = metrics.get('clicks', 0)
            conversions = metrics.get('conversions', 0)
            ctr = metrics.get('ctr', 0)
            cpa = metrics.get('cpa', 0)
            roas = metrics.get('roas', 0)
            cpm = metrics.get('cpm', 0)
            
            # Extract analysis data
            benchmark_comparison = analysis_result.get('benchmark_comparison', {})
            performance_vs_benchmark = benchmark_comparison.get('overall_performance_score', 0)
            performance_rating = benchmark_comparison.get('performance_rating', 'Average')
            
            # Get best segment
            segment_analysis = analysis_result.get('segment_analysis', {})
            best_segment = segment_analysis.get('best_segments', ['Unknown'])[0] if segment_analysis.get('best_segments') else 'Unknown'
            
            # Get creative preview URL
            creative = ad_data.get('creative', {}) or {}
            image_url = creative.get('image_url', '')
            
            # Format image URL as a clickable link formula in Google Sheets
            creative_preview = f'=HYPERLINK("{image_url}", "View Creative")' if image_url else ''
            
            # Create row values
            row_values = [
                ad_id,
                ad_name,
                campaign_name,
                created_date,
                analysis_date,
                spend,
                impressions,
                clicks,
                conversions,
                ctr,
                cpa,
                roas,
                cpm,
                performance_vs_benchmark,
                performance_rating,
                best_segment,
                creative_preview
            ]
            
            # Find if the ad already exists in the Ad Details tab
            range_name = f"{self.AD_DETAILS_TAB}!A:A"
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            existing_row = None
            
            # Skip header row and search for ad ID
            for i, row in enumerate(values[1:], 2):  # Start from row 2 (index 1+1)
                if row and row[0] == ad_id:
                    existing_row = i
                    break
            
            # Determine whether to insert a new row or update existing
            next_row = len(values) + 1
            update_row = existing_row if existing_row else next_row
            range_name = f"{self.AD_DETAILS_TAB}!A{update_row}:{chr(65 + len(row_values) - 1)}{update_row}"
            
            # Update or insert the row
            body = {
                'values': [row_values]
            }
            
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',  # Use USER_ENTERED for formulas to work
                body=body
            ).execute()
            
            # If this is a new row, apply formatting
            if not existing_row:
                self._format_ad_details_row(update_row, performance_vs_benchmark)
            
            logger.info(f"Ad details updated successfully for ad {ad_id} at row {update_row}")
            return True
            
        except Exception as e:
            logger.exception(f"Failed to update ad details: {str(e)}")
            return False
    
    def _format_ad_details_row(self, row_index: int, performance_score: float) -> None:
        """
        Apply formatting to an ad details row based on performance
        
        Args:
            row_index: The 1-based row index to format
            performance_score: Performance score to determine formatting
        """
        try:
            # Get sheet ID for ad details tab
            sheet_metadata = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            sheets = sheet_metadata.get('sheets', [])
            sheet_id = None
            
            for sheet in sheets:
                if sheet.get('properties', {}).get('title') == self.AD_DETAILS_TAB:
                    sheet_id = sheet.get('properties', {}).get('sheetId')
                    break
            
            if not sheet_id:
                logger.warning(f"Could not find sheet ID for tab {self.AD_DETAILS_TAB}")
                return
            
            # Apply alternating row background
            background_color = {
                'red': 0.95,
                'green': 0.95,
                'blue': 0.95
            } if row_index % 2 == 0 else {
                'red': 1.0,
                'green': 1.0,
                'blue': 1.0
            }
            
            # Determine performance color (green for good, red for bad)
            performance_color = {}
            if performance_score >= 20:
                # Good performance - green
                performance_color = {'red': 0.0, 'green': 0.6, 'blue': 0.0}
            elif performance_score <= -20:
                # Poor performance - red
                performance_color = {'red': 0.8, 'green': 0.0, 'blue': 0.0}
            
            requests = [
                # Basic row formatting
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_index - 1,  # 0-based for API
                            'endRowIndex': row_index,
                            'startColumnIndex': 0,
                            'endColumnIndex': len(self.AD_DETAILS_COLUMNS)
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': background_color,
                                'horizontalAlignment': 'RIGHT',
                                'textFormat': {
                                    'bold': False
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(backgroundColor,horizontalAlignment,textFormat)'
                    }
                },
                # Left-align text columns
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_index - 1,
                            'endRowIndex': row_index,
                            'startColumnIndex': 0,  # Ad ID column
                            'endColumnIndex': 3  # Through Campaign column
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'horizontalAlignment': 'LEFT'
                            }
                        },
                        'fields': 'userEnteredFormat(horizontalAlignment)'
                    }
                },
                # Left-align more text columns
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_index - 1,
                            'endRowIndex': row_index,
                            'startColumnIndex': 14,  # Rating column
                            'endColumnIndex': 16  # Through Best Segment
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'horizontalAlignment': 'LEFT'
                            }
                        },
                        'fields': 'userEnteredFormat(horizontalAlignment)'
                    }
                }
            ]
            
            # Add performance score color if applicable
            if performance_color:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_index - 1,
                            'endRowIndex': row_index,
                            'startColumnIndex': 13,  # Performance vs Benchmark column
                            'endColumnIndex': 14
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'foregroundColor': performance_color,
                                    'bold': True
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(textFormat)'
                    }
                })
                
                # Also color the rating column
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_index - 1,
                            'endRowIndex': row_index,
                            'startColumnIndex': 14,  # Rating column
                            'endColumnIndex': 15
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'textFormat': {
                                    'foregroundColor': performance_color,
                                    'bold': True
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(textFormat)'
                    }
                })
            
            # Apply number formatting
            # Spend, CPA, CPM columns (currency format)
            currency_columns = [5, 10, 12]  # Spend, CPA, CPM
            for column_index in currency_columns:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_index - 1,
                            'endRowIndex': row_index,
                            'startColumnIndex': column_index,
                            'endColumnIndex': column_index + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'CURRENCY',
                                    'pattern': '£#,##0.00'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(numberFormat)'
                    }
                })
            
            # Percentage formatting for CTR and Performance vs Benchmark
            percentage_columns = [9, 13]  # CTR, Performance vs Benchmark
            for column_index in percentage_columns:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_index - 1,
                            'endRowIndex': row_index,
                            'startColumnIndex': column_index,
                            'endColumnIndex': column_index + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'PERCENT',
                                    'pattern': '0.00%'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(numberFormat)'
                    }
                })
            
            # Number formatting for impressions, clicks, conversions
            number_columns = [6, 7, 8]  # Impressions, Clicks, Conversions
            for column_index in number_columns:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_index - 1,
                            'endRowIndex': row_index,
                            'startColumnIndex': column_index,
                            'endColumnIndex': column_index + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'numberFormat': {
                                    'type': 'NUMBER',
                                    'pattern': '#,##0'
                                }
                            }
                        },
                        'fields': 'userEnteredFormat(numberFormat)'
                    }
                })
            
            # ROAS formatting (decimal format)
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': row_index - 1,
                        'endRowIndex': row_index,
                        'startColumnIndex': 11,  # ROAS column
                        'endColumnIndex': 12
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'NUMBER',
                                'pattern': '0.0x'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat(numberFormat)'
                }
            })
            
            body = {'requests': requests}
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body
            ).execute()
            
        except Exception as e:
            logger.exception(f"Failed to format ad details row: {str(e)}")
    
    def update_ad_details_batch(self, ads_data: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        Batch update multiple ad records in the Ad Details tab
        
        Args:
            ads_data: List of dictionaries containing ad_data and analysis_result pairs
                [{"ad_data": {...}, "analysis_result": {...}}, ...]
                
        Returns:
            Tuple[int, int]: (success_count, error_count)
        """
        logger.info(f"Batch updating Ad Details for {len(ads_data)} ads")
        
        success_count = 0
        error_count = 0
        
        for item in ads_data:
            ad_data = item.get('ad_data', {})
            analysis_result = item.get('analysis_result', {})
            
            if not ad_data or not analysis_result:
                error_count += 1
                continue
                
            result = self.update_ad_details(ad_data, analysis_result)
            if result:
                success_count += 1
            else:
                error_count += 1
                
        logger.info(f"Ad details batch update complete: {success_count} successes, {error_count} errors")
        return success_count, error_count


# Example usage
if __name__ == "__main__":
    # Initialize the Sheets Manager
    manager = SheetsManager()
    
    # Print spreadsheet URL
    print(f"Spreadsheet URL: {manager.get_spreadsheet_url()}")