from __future__ import annotations

import json
import time
import sys
import io
import os
from typing import Dict, List, Optional
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import pandas as pd
from openpyxl.styles import PatternFill, Font
from openpyxl.utils.dataframe import dataframe_to_rows

class Logger:
    def __init__(self, filename: str):
        # Create store directory if it doesn't exist
        self.store_dir = "store"
        os.makedirs(self.store_dir, exist_ok=True)
        
        # Put log file in store directory
        log_path = os.path.join(self.store_dir, filename)
        self.terminal = sys.stdout
        self.log_file = open(log_path, 'w', encoding='utf-8')
        self.string_buffer = io.StringIO()

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)
        self.string_buffer.write(message)
        
    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

class AnjaniCourierClient:
    _BASE_URL = "http://www.anjanicourier.in/"
    _PINCODE_ENDPOINT = _BASE_URL + "Rpt_PinCodeShow.aspx"

    def __init__(self) -> None:
        self.username = "ADR25"
        self.password = "ADR25"
        self.headless = True
        
        # Create store and temp directories
        self.store_dir = "store"
        self.temp_dir = os.path.join(self.store_dir, "temp_data")
        os.makedirs(self.store_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # JSON file paths
        self.pincode_file = os.path.join(self.temp_dir, "pincodes.json")
        self.success_file = os.path.join(self.temp_dir, "pincode_successes.json")
        self.failed_file = os.path.join(self.temp_dir, "pincode_failures.json")
        
        # Initialize JSON files if they don't exist
        for file_path in [self.pincode_file, self.success_file, self.failed_file]:
            if not os.path.exists(file_path):
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump([], f)

        # Start Selenium login once per client instance
        self.session_id: str = self._login_and_get_session_id()

    def _read_json_file(self, file_path: str) -> List[Dict]:
        """Read data from JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []

    def _write_json_file(self, file_path: str, data: List[Dict]) -> None:
        """Write data to JSON file"""
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)

    def fetch_pincode_details(self, pc_code: str) -> bool:
        """Return structured information for a *pincode* (PC) as a list."""
        params = {"EC": 2, "PC": pc_code}
        cookies = {"ASP.NET_SessionId": self.session_id}

        max_retries = 2  # Allow one retry with fresh session
        for attempt in range(max_retries):
            try:
                response = httpx.get(self._PINCODE_ENDPOINT, params=params, cookies=cookies, follow_redirects=False)
                
                # Check for 302 redirect or redirect to _NotAvailable.aspx
                if response.status_code == 302 or (response.status_code == 200 and "_NotAvailable.aspx" in str(response.url)):
                    if attempt == 0:  # Only retry once
                        print(f"Session expired for pincode {pc_code}. Re-logging in...")
                        self.session_id = self._login_and_get_session_id()
                        cookies = {"ASP.NET_SessionId": self.session_id}
                        print("Session refreshed. Retrying...")
                        continue
                    else:
                        print(f"Failed to access pincode {pc_code} even after session refresh")
                        return False
                
                response.raise_for_status()
                break  # Success, exit retry loop
                
            except Exception as e:
                if attempt == 0:
                    print(f"Error accessing pincode {pc_code}: {e}. Trying to refresh session...")
                    self.session_id = self._login_and_get_session_id()
                    cookies = {"ASP.NET_SessionId": self.session_id}
                    continue
                else:
                    print(f"Failed to access pincode {pc_code} even after session refresh: {e}")
                    return False

        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", {"id": "ReportTbl"})
        if not table:
            return False

        current_branch: Optional[str] = None
        found_records: bool = False  # Track if we captured any rows

        # Read existing data
        pincode_data = self._read_json_file(self.pincode_file)

        for row in table.find_all("tr"):
            cols = row.find_all("td")
            if not cols:
                # Skip empty spacer rows
                continue

            # Branch header rows look like:  <td>KILLA PARDI, VALSAD</td><td>Contact To:</td> ...
            if len(cols) >= 2 and "Contact To:" in cols[1].text:
                current_branch = cols[0].get_text(strip=True)
                continue

            # Valid data rows have exactly 7 <td> elements and the 2nd column is a serial no.
            if len(cols) == 7 and cols[1].text.strip().isdigit():
                item = {
                    "Pin Code": pc_code,
                    "Inserted At": datetime.now().isoformat(),
                    "Branch Name": current_branch or "Unknown",
                    "Area Name": cols[2].get_text(strip=True),
                    "Zone Type": cols[3].get_text(strip=True),
                    "Delivery Type": cols[5].get_text(strip=True),
                    "Transit Days": cols[6].get_text(strip=True),
                }

                # Add to pincode data
                pincode_data.append(item)
                found_records = True

        # Write updated pincode data
        if found_records:
            self._write_json_file(self.pincode_file, pincode_data)

        # After processing the table, log success/failure summary per pincode
        summary_doc = {
            "Pin Code": pc_code,
            "Checked At": datetime.now().isoformat(),
        }

        if found_records:
            summary_doc["Status"] = "success"
            success_data = self._read_json_file(self.success_file)
            success_data.append(summary_doc)
            self._write_json_file(self.success_file, success_data)
        else:
            summary_doc["Status"] = "failed"
            summary_doc["Reason"] = "No records found"
            failed_data = self._read_json_file(self.failed_file)
            failed_data.append(summary_doc)
            self._write_json_file(self.failed_file, failed_data)

        return found_records

    def process_pincodes(self, pincodes: List[str]) -> Dict[str, List[str]]:
        """Fetch details for multiple pincodes and return a summary dict."""
        results = {"success": [], "failed": []}
        request_count = 0  # Counter to track requests
        
        for i, pc in enumerate(pincodes, 1):
            print(f"Processing pincode {i}/{len(pincodes)}: {pc}")
            # Check if pincode already exists in success collection
            success_data = self._read_json_file(self.success_file)
            existing_success = any(item["Pin Code"] == str(pc) for item in success_data)
            if existing_success:
                print(f"Pincode {pc} already processed successfully. Skipping...")
                continue
        
            try:
                ok = self.fetch_pincode_details(pc)
                if ok:
                    results["success"].append(pc)
                else:
                    results["failed"].append(pc)
                    
                request_count += 1
                
                # Add 20 second delay after every 20 requests
                if request_count % 20 == 0 and i < len(pincodes):
                    print(f"✅ Processed {request_count} requests. Taking 20 second break...")
                    print(f"⏰ Remaining pincodes: {len(pincodes) - i}")
                    time.sleep(20)
                    print("🚀 Resuming processing...")
                    
            except Exception as exc:
                # Treat unhandled errors as failures and log them
                failed_data = self._read_json_file(self.failed_file)
                failed_data.append({
                    "Pin Code": pc,
                    "Checked At": datetime.now().isoformat(),
                    "Status": "failed",
                    "Reason": str(exc),
                })
                self._write_json_file(self.failed_file, failed_data)
                results["failed"].append(pc)
                request_count += 1
                
                # Add delay even for failed requests
                if request_count % 20 == 0 and i < len(pincodes):
                    print(f"✅ Processed {request_count} requests. Taking 20 second break...")
                    print(f"⏰ Remaining pincodes: {len(pincodes) - i}")
                    time.sleep(20)
                    print("🚀 Resuming processing...")

        return results

    def _login_and_get_session_id(self) -> str:
        """Perform Selenium login and return the *ASP.NET_SessionId* value."""
        options = Options()
        if self.headless:
            options.add_argument("--headless")

        # Rely on PATH lookup for *chromedriver*:
        service = Service()
        driver = webdriver.Chrome(service=service, options=options)

        try:
            driver.get(self._BASE_URL)
            time.sleep(2)  # Wait for page JS

            driver.find_element(By.ID, "txtUserID").send_keys(self.username)
            driver.find_element(By.ID, "txtPassword").send_keys(self.password)
            driver.find_element(By.ID, "cmdLogin").click()
            time.sleep(3)  # Allow redirect and cookie creation

            cookies = driver.get_cookies()
            for cookie in cookies:
                if cookie.get("name") == "ASP.NET_SessionId":
                    print("Session ID:", cookie.get("value"))
                    print("Logged in successfully")
                    return str(cookie.get("value"))

            raise RuntimeError("Login succeeded but session cookie not found")
        finally:
            driver.quit()

class JsonToExcelExporter:
    def __init__(self):
        # Setup directories
        self.store_dir = "store"
        self.temp_dir = os.path.join(self.store_dir, "temp_data")
        os.makedirs(self.store_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # JSON file paths
        self.pincode_file = os.path.join(self.temp_dir, "pincodes.json")
        self.success_file = os.path.join(self.temp_dir, "pincode_successes.json")
        self.failed_file = os.path.join(self.temp_dir, "pincode_failures.json")
    
    def fetch_all_data(self):
        """Fetch data from all JSON files"""
        print("Fetching data from JSON files...")
        
        # Fetch all data
        with open(self.pincode_file, 'r', encoding='utf-8') as f:
            all_data = json.load(f)
        print(f"Total pincode records: {len(all_data)}")
        
        # Fetch success data
        with open(self.success_file, 'r', encoding='utf-8') as f:
            success_data = json.load(f)
        print(f"Success records: {len(success_data)}")
        
        # Fetch failed data
        with open(self.failed_file, 'r', encoding='utf-8') as f:
            failed_data = json.load(f)
        print(f"Failed records: {len(failed_data)}")
        
        return all_data, success_data, failed_data
    
    def convert_to_dataframes(self, all_data, success_data, failed_data):
        """Convert JSON data to pandas DataFrames"""
        print("Converting data to DataFrames...")
        
        # Convert all data to DataFrame
        df_all = pd.DataFrame(all_data) if all_data else pd.DataFrame()
        
        # Convert success data to DataFrame
        df_success = pd.DataFrame(success_data) if success_data else pd.DataFrame()
        
        # Convert failed data to DataFrame
        df_failed = pd.DataFrame(failed_data) if failed_data else pd.DataFrame()
        
        return df_all, df_success, df_failed
    
    def format_worksheet(self, worksheet, df):
        """Format worksheet with light yellow headers and auto-adjusted columns"""
        # Define light yellow fill for headers
        yellow_fill = PatternFill(start_color="FFFFE066", end_color="FFFFE066", fill_type="solid")
        header_font = Font(bold=True)
        
        # Apply formatting to header row
        for cell in worksheet[1]:
            cell.fill = yellow_fill
            cell.font = header_font
        
        # Auto-adjust column widths
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            
            # Set column width (with some padding)
            adjusted_width = min(max_length + 2, 50)  # Max width of 50
            worksheet.column_dimensions[column_letter].width = adjusted_width

    def create_excel_file(self, df_all, df_success, df_failed, df_delivery_zone):
        """Create Excel file with multiple sheets"""
        # Put Excel file in store directory
        filename = os.path.join(self.store_dir, "anjani_courier_data.xlsx")
        
        print(f"Creating Excel file: {filename}")
        
        # Create Excel writer object
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Prepare dataframes first
            df_all_copy = df_all.copy()
            
            # Sheet 1: Delivery Pincode Details
            if not df_all_copy.empty:
                df_delivery_only = df_all_copy[df_all_copy['Zone Type'] == 'Delivery Zone'].copy()
                if not df_delivery_only.empty:
                    df_delivery_only = df_delivery_only.drop(['Inserted At'], axis=1, errors='ignore')
                    df_delivery_only.to_excel(writer, sheet_name='Delivery Pincode Details', index=False)
                    
                    # Format the worksheet
                    worksheet = writer.sheets['Delivery Pincode Details']
                    self.format_worksheet(worksheet, df_delivery_only)
                    print(f"Delivery Pincode Details sheet created with {len(df_delivery_only)} records")
            
            # Sheet 2: All Pincode Details (sorted)
            if not df_all_copy.empty:
                df_all_sorted = df_all_copy.copy()
                df_all_sorted = df_all_sorted.drop(['Inserted At'], axis=1, errors='ignore')
                df_all_sorted = df_all_sorted.sort_values(by='Pin Code')    
                df_all_sorted.to_excel(writer, sheet_name='All Pincode Details', index=False)
                
                # Format the worksheet
                worksheet = writer.sheets['All Pincode Details']
                self.format_worksheet(worksheet, df_all_sorted)
                print(f"All Pincode Details sheet created with {len(df_all_sorted)} records")
            
            # Sheet 3: Process Success
            if not df_success.empty:
                df_success_clean = df_success.copy()
                df_success_clean = df_success_clean.drop(['Checked At','Status'], axis=1, errors='ignore')
                df_success_clean.to_excel(writer, sheet_name='Found Pincode', index=False)
                
                # Format the worksheet
                worksheet = writer.sheets['Found Pincode']
                self.format_worksheet(worksheet, df_success_clean)
                print(f"Success sheet created with {len(df_success_clean)} records")
            
            # Sheet 4: Process Failed
            if not df_failed.empty:
                df_failed_clean = df_failed.copy()
                df_failed_clean = df_failed_clean.drop(['Checked At','Status','Reason'], axis=1, errors='ignore')
                df_failed_clean.to_excel(writer, sheet_name='Not Found Pincode', index=False)
                
                # Format the worksheet
                worksheet = writer.sheets['Not Found Pincode']
                self.format_worksheet(worksheet, df_failed_clean)
                print(f"Failed sheet created with {len(df_failed_clean)} records")
            
            # Sheet 5: Delivery Zone Summary
            if not df_delivery_zone.empty:
                df_zone_summary = df_delivery_zone[["Pin Code"]]
                df_zone_summary.to_excel(writer, sheet_name='Possible Delivery Zone', index=False)
                
                # Format the worksheet
                worksheet = writer.sheets['Possible Delivery Zone']
                self.format_worksheet(worksheet, df_zone_summary)
                print(f"Delivery Zone Summary sheet created with {len(df_zone_summary)} records")
        
        print(f"Excel file '{filename}' created successfully!")
        return filename

    def get_delivery_zone_data(self,df):
        """Get delivery zone data"""
        # Convert Pin Code to string type to avoid comparison issues
        df['Pin Code'] = df['Pin Code'].astype(str)
        
        df_grouped = df.groupby('Pin Code')['Zone Type'].value_counts().unstack(fill_value=0)
        df_grouped['Total'] = df_grouped.sum(axis=1)
        
        if 'Delivery Zone' in df_grouped.columns:
            # Convert to numeric values and calculate percentage
            delivery_zone = pd.to_numeric(df_grouped['Delivery Zone'], errors='coerce')
            total = pd.to_numeric(df_grouped['Total'], errors='coerce')
            pr = (delivery_zone / total) * 100
            # Filter where percentage is >= 80
            df_grouped = df_grouped[pr >= 80]
            
        df_grouped = df_grouped.reset_index()
        return df_grouped
    
    def export_to_excel(self):
        """Main method to export JSON data to Excel"""
        try:
            # Fetch data from JSON files
            all_data, success_data, failed_data = self.fetch_all_data()
            
            # Convert to DataFrames
            df_all, df_success, df_failed = self.convert_to_dataframes(all_data, success_data, failed_data)
            df_delivery_zone = self.get_delivery_zone_data(df_all)

            # Create Excel file
            filename = self.create_excel_file(df_all, df_success, df_failed, df_delivery_zone)
            
            # Print summary
            print("\n" + "="*50)
            print("EXPORT SUMMARY")
            print("="*50)
            print(f"File created: {filename}")
            print(f"Total data records: {len(df_all)}")
            print(f"Success records: {len(df_success)}")
            print(f"Failed records: {len(df_failed)}")
            print("="*50)
            
            return filename
            
        except Exception as e:
            print(f"Error occurred: {str(e)}")
            return None

def get_pincode_list(file_path):
    try:
        # Read CSV
        df = pd.read_csv(file_path, encoding='utf-8')

        # Normalize column names to handle case mismatches
        df.columns = [col.strip().upper() for col in df.columns]

        if "PINCODE" not in df.columns:
            print("No 'PINCODE' column found in the CSV.")
            return []

        # Drop rows where PINCODE is NaN or empty
        df = df[df["PINCODE"].notna()]
        df = df[df["PINCODE"].astype(str).str.strip() != ""]

        # Total entries
        total_count = len(df)

        # Drop duplicates
        unique_pincodes = df["PINCODE"].drop_duplicates()

        # Count after removing duplicates
        unique_count = len(unique_pincodes)
        duplicate_count = total_count - unique_count

        print(f"Total PINCODE entries: {total_count}")
        print(f"Duplicate PINCODEs: {duplicate_count}")
        print(f"Unique PINCODEs: {unique_count}")

        return unique_pincodes.tolist()

    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []
    except Exception as e:
        print(f"Error: {e}")
        return []

def run_scraping_and_analysis():
    # Create timestamp for log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"scraping_analysis_log_{timestamp}.txt"
    
    # Set up logging
    logger = Logger(log_filename)
    sys.stdout = logger
    
    try:
        print(f"Starting scraping and analysis process at {datetime.now()}")
        print("="*50)
        
        # Step 1: Run Scraping Process
        print("\nSTEP 1: SCRAPING PROCESS")
        print("="*50)
        
        # Get pincode list
        pincodes = get_pincode_list("pincodes.csv")
        if not pincodes:
            print("Error: No pincodes found to process!")
            return
        
        # Initialize scraper and process pincodes
        client = AnjaniCourierClient()
        summary = client.process_pincodes(pincodes)
        
        print("\nScraping Summary:")
        print(f"Successfully processed: {len(summary['success'])} pincodes")
        print(f"Failed to process: {len(summary['failed'])} pincodes")
        
        # Step 2: Run Analysis Process
        print("\nSTEP 2: ANALYSIS PROCESS")
        print("="*50)
        
        # Initialize exporter and run analysis
        exporter = JsonToExcelExporter()
        excel_file = exporter.export_to_excel()
        
        if excel_file:
            print(f"\nProcess completed successfully!")
            print(f"Excel file generated: {excel_file}")
            print(f"Log file generated: {log_filename}")
            print(f"JSON files stored in: temp_data/")
        else:
            print("\nError: Failed to generate Excel file!")
            
    except Exception as e:
        print(f"\nError occurred during processing: {str(e)}")
    finally:
        # Restore original stdout
        sys.stdout = sys.__stdout__
        print(f"\nProcess completed. Check {log_filename} for detailed logs.")

if __name__ == "__main__":
    run_scraping_and_analysis()