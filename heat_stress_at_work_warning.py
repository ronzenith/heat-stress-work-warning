# Google Spreadsheet
# Google Spreadsheet 20240922
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import urljoin
import time
import logging
import numpy as np
import os
import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_page(url, date):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        heat_stress_articles = []
        keywords = [
            "heat stress at work warning in force",
            "cancellation of heat stress at work warning"
        ]
        
        for link in soup.find_all('a'):
            link_text = link.text.strip().lower()
            if any(keyword in link_text for keyword in keywords):
                article_url = urljoin(url, link.get('href'))
                article_content, numeric_values = fetch_article_content(article_url)
                article_month = date.strftime('%B')  # Get month name
                
                time_value = ""  # Default value if no time is found
                time_numeric = ""  # To store only numeric part

                if numeric_values:
                    time_split = numeric_values[0].split()
                    if len(time_split) > 1:
                        time_value = time_split[1]  # Get AM/PM from the first numeric value
                    else:
                        time_value = ""  # If no AM/PM found
                    
                    time_numeric = ''.join(filter(str.isdigit, numeric_values[0]))

                    # Convert time to 24-hour format
                    if time_value == "PM":
                        if int(time_numeric) < 1200:
                            time_numeric = str(int(time_numeric) + 1200)
                    elif time_value == "AM":
                        if int(time_numeric) == 1200:
                            time_numeric = "0000"

                if not time_value:
                    time_value = 0
                if not time_numeric:
                    time_numeric = "0"

                heat_stress_articles.append({
                    'month': article_month,
                    'date': date.strftime('%Y%m%d'),
                    'title': link.text.strip(),
                    'content': article_content,
                    'url': article_url,
                    'type': 'Warning' if 'in force' in link_text else 'Cancellation',
                    'numeric_values': ', '.join(numeric_values),
                    'time': time_value,
                    'time_value': time_numeric
                })
        
        return heat_stress_articles
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return []

def fetch_article_content(article_url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        response = requests.get(article_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        paragraphs = soup.find_all('p')
        full_content = ""
        for para in paragraphs:
            if "heat stress at work warning" in para.text.lower():
                full_content = para.text.strip()
                break
        
        if full_content:
            numeric_values = re.findall(r'\d+\.?\d*\s*[APM]{2}', full_content)
            return full_content, numeric_values
        
        return "Content not found", []
    except requests.RequestException as e:
        logging.error(f"Error fetching article {article_url}: {e}")
        return "Content not found", []

def upload_to_google_sheets(dataframe, sheet_name):
    # Define the scope and credentials for Google Sheets API
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    # Load credentials from environment variable
    creds_json = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
    creds_dict = json.loads(creds_json)
    
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    # Create or open the Google Sheet and upload data
    try:
        spreadsheet = client.open(sheet_name)
    except gspread.exceptions.SpreadsheetNotFound:
        spreadsheet = client.create(sheet_name)

    # Access the "Detailed data" worksheet
    try:
        worksheet = spreadsheet.worksheet("Detailed data")
    except gspread.exceptions.WorksheetNotFound:
        # If the worksheet does not exist, create it
        worksheet = spreadsheet.add_worksheet(title="Detailed data", rows="100", cols="20")

    # Prepare data for appending
    existing_rows = worksheet.get_all_values()  # Get existing data in the sheet
    header = dataframe.columns.values.tolist()  # Get DataFrame headers

    #if not existing_rows or existing_rows[0] != header:
        #worksheet.append_row(header)  # Append header if it doesn't exist

    # Convert DataFrame to list of lists for appending
    rows_to_append = dataframe.values.tolist()

    # Clean up rows before appending
    for row in rows_to_append:
        for i in range(len(row)):
            if isinstance(row[i], float):
                if not (np.isfinite(row[i])):  # Check if it's finite
                    row[i] = 0  # Replace with zero or any other placeholder

    worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')

    return spreadsheet  # Return the spreadsheet object

def generate_summary_data(detailed_df):
    # Filter for rows with type 'Cancellation' or 0
    filtered_df = detailed_df[(detailed_df['type'] == 'Cancellation') | (detailed_df['type'] == 0)]
    
    # Group by date and sum duration_in_number
    summary_df = filtered_df.groupby('date', as_index=False).agg({'no_of_hours': 'sum'})
    
    return summary_df

def main(start_date, end_date):
    base_url = "https://www.info.gov.hk/gia/wr/{year}{month:02d}/{day:02d}.htm"
    
    all_articles = []
    
    current_date = start_date
    while current_date <= end_date:
        url = base_url.format(year=current_date.year, 
                              month=current_date.month, 
                              day=current_date.day)
        
        logging.info(f"Scraping: {url}")
        articles = scrape_page(url, current_date)
        
        if articles:
            all_articles.extend(articles)
        else:
            all_articles.append({
                'month': current_date.strftime('%B'),
                'date': current_date.strftime('%Y%m%d'),
                'title': 'No record',
                'content': 'No warnings or cancellations issued.',
                'url': '',
                'type': 0,
                'numeric_values': 0,
                'time': 0,
                'time_value': "0",
                'no_of_hours': 0  # Ensure this column exists for consistency
            })
            logging.info(f"No articles found for {current_date.strftime('%Y-%m-%d')}")

        current_date += timedelta(days=1)
        time.sleep(1)

    new_df = pd.DataFrame(all_articles)

     # Upload detailed data and capture the spreadsheet object
    spreadsheet = upload_to_google_sheets(new_df, "Heat_Stress_Index")

    # Ensure time_value is zero-padded and numeric
    new_df['time_value'] = new_df['time_value'].astype(str).str.zfill(4)
    new_df['time_value'] = pd.to_numeric(new_df['time_value'], errors='coerce').fillna(0).astype(int)

    # Create a new column for duration
    new_df['duration'] = None

    # Calculate duration for each day
    for date, group in new_df.groupby('date'):
        for index in range(len(group) - 1):  # Iterate through the group but stop at the second last index
            if (index + 1) % 2 != 0:  # Check if it's an odd index
                current_row = group.iloc[index]
                next_row = group.iloc[index + 1]

                current_time_value = current_row['time_value']
                next_time_value = next_row['time_value']

                # Calculate the duration based on larger and smaller time values
                if current_time_value > next_time_value:
                    duration = current_time_value - next_time_value
                
                    # Format duration as HH:MM
                    hours = duration // 100
                    minutes = duration % 100
                    duration_str = f"{hours}:{str(minutes).zfill(2)}"
                
                    new_df.at[group.index[index], 'duration'] = duration_str
            
                else:
                    # If negative difference, do not assign anything (leave as None)
                    logging.debug(f"No positive difference for {current_row['title']} and {next_row['title']} on {date}")

   # Create a new column for duration_in_number by converting HH:MM to decimal hours
    new_df['no_of_hours'] = None  # Initialize the new column

    for index, row in new_df.iterrows():
        if row['duration'] is not None:
            # Split duration into hours and minutes
            time_parts = row['duration'].split(':')
            if len(time_parts) == 2:
                hours = int(time_parts[0])
                minutes = int(time_parts[1])
                
                # Convert to decimal
                decimal_duration = hours + (minutes / 60)
                
                new_df.at[index, 'no_of_hours'] = round(decimal_duration, 2)  # Round to 2 decimal places

    # Inspect DataFrame before uploading to Google Sheets
    print(new_df[['date', 'title', 'type', 'time_value', 'duration', 'no_of_hours']])
    
    # Upload detailed data
    #upload_to_google_sheets(new_df, "Heat_Stress_Index")

    # Generate summary data
    summary_df = generate_summary_data(new_df)

    # Upload summary data to "Summary data" tab
    try:
        summary_worksheet = spreadsheet.worksheet("Summary data")
    except gspread.exceptions.WorksheetNotFound:
        summary_worksheet = spreadsheet.add_worksheet(title="Summary data", rows="1000", cols="20")

    existing_summary_rows = summary_worksheet.get_all_values()
    
    if not existing_summary_rows or existing_summary_rows[0] != summary_df.columns.values.tolist():
        summary_worksheet.append_row(summary_df.columns.values.tolist())

    summary_rows_to_append = summary_df.values.tolist()
    
    summary_worksheet.append_rows(summary_rows_to_append, value_input_option='USER_ENTERED')

if __name__ == "__main__":
    today = datetime.now()
    start_date = datetime(2024,10,10)
    end_date = today
    main(start_date, end_date)
