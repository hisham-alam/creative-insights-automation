Instructions for Google Sheets Formatting Script (V2)
Objective
Write a Python script that takes a list of dictionaries containing ad performance data, processes it, and saves it to a Google Sheet with specific, complex formatting rules. The script must also be capable of generating a simplified .csv file for testing purposes.

Input Data Specification
The input will be a list of Python dictionaries. Each dictionary represents a single row and must now include keys for ad_link and demographics.

Example Input:

Python

[
    {
        "launched": "10/08/2025",
        "ad_name": "CEE - EG3 - Price_Transparency-Video",
        "ad_link": "https://example.com/path/to/video.mp4",
        "creative_angle": "Comparison",
        "status": "Winning",
        "action": "Scale",
        "cpr_value": 12.00,
        "cpr_percent_change": -0.34,
        "demographics": [
            "Strong performance with Males 18-34.",
            "Consistent engagement from Females 35-44."
        ],
        "ai_analysis": [
            "Direct comparison of a transparent fee vs. a hidden markup resonates strongly.",
            "Simple animation makes a complex financial pain point easy to grasp instantly without sound."
        ]
    },
    # ... more dictionaries
]
Output Specification
Primary Output: A Google Sheet with data formatted according to the rules below. The Ad Name column will be hyperlinked, and there will be a new Demographics column before AI Analysis.

Secondary Output (for testing): A .csv file.

Implementation Plan
You will need the following Python libraries: gspread, gspread-formatting, google-auth, and pandas.

Step 1: Data Preparation & CSV Export (Testing)
Load Data: Convert the input list of dictionaries into a pandas DataFrame.

Format Strings: Create string representations for metric, bullet, and link columns.

For the ad_name, you could create a column like [CEE...](https://example.com...) for a markdown-style link placeholder.

For metrics, use markdown-style asterisks for bolding: **£12.00** (-34%).

For Demographics and AI Analysis, combine their lists into single strings using hyphens and newlines (\n).

Export to CSV: Save the prepared DataFrame to output.csv.

Step 2: Google Sheets Authentication
Implement standard Google Cloud Platform authentication using a service account and a credentials.json file to authorize API access.

Step 3: Write and Format Data in Google Sheets
This is the core logic. The order of operations is important.

Format the Ad Name as a Hyperlink:

When writing data, do not write the ad name as a plain string. Instead, construct a Google Sheets formula string: =HYPERLINK("URL_from_input", "ad_name_from_input").

Example: =HYPERLINK("https://example.com/path/to/video.mp4", "CEE - EG3 - Price_Transparency-Video")

Write this complete formula string directly into each cell of the Ad Name column.

Set Up Dropdowns (Data Validation):

For the Creative Angle, Status, and Action columns, apply data validation rules from a pre-defined list (e.g., ['Comparison', 'Testimonial']).

Use the API to set a DataValidationRule for the entire range of these columns.

Format Demographics and AI Analysis Columns (Bulleted Lists):

For each cell in both the Demographics and AI Analysis columns, construct a single string.

Each point must be prefixed with a bullet character (• ) and separated by a newline character (\n).

Update the cells with this single formatted string.

Format the Metric Columns (Mixed Bold/Plain Text):

Use the Google Sheets API's textFormatRuns property for each metric cell.

For a string like £12 (-34%), define two formatting runs:

Run 1: Applies to £12. Set textFormat to { "bold": true }.

Run 2: Applies to  (-34%). Set textFormat to { "bold": false }.

Use a batch update request to apply this formatting efficiently across all metric cells.