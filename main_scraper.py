from __future__ import annotations

import json
import time
import sys
import io
import os
from typing import Dict, List, Optional
from datetime import datetime
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from threading import Thread

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

class ProgressWindow:
    def __init__(self):
        # Create main window
        self.root = tk.Tk()
        self.root.title("Anjani Courier Data Processing")
        
        # Set window size and position it in center
        window_width = 500
        window_height = 200
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        center_x = int(screen_width/2 - window_width/2)
        center_y = int(screen_height/2 - window_height/2)
        self.root.geometry(f'{window_width}x{window_height}+{center_x}+{center_y}')
        
        # Add padding around the window
        self.main_frame = ttk.Frame(self.root, padding="20")
        self.main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Loading spinner (initially hidden)
        self.loading_label = ttk.Label(self.main_frame, text="⟳ Initializing...", font=("Arial", 12))
        self.loading_label.grid(row=0, column=0, sticky=tk.W, pady=(0, 10))
        self.loading_label.grid_remove()
        
        # Progress Frame (initially hidden)
        self.progress_frame = ttk.Frame(self.main_frame)
        self.progress_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=10)
        self.progress_frame.grid_remove()
        
        # Progress Label
        self.label_var = tk.StringVar(value="")
        self.label = ttk.Label(self.progress_frame, textvariable=self.label_var, font=("Arial", 10))
        self.label.grid(row=0, column=0, sticky=tk.W, pady=(0, 5))
        
        # Progress Bar
        self.progress = ttk.Progressbar(
            self.progress_frame, 
            orient="horizontal", 
            length=400, 
            mode="determinate"
        )
        self.progress.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Status Label
        self.status_var = tk.StringVar(value="")
        self.status = ttk.Label(self.progress_frame, textvariable=self.status_var, font=("Arial", 9))
        self.status.grid(row=2, column=0, sticky=tk.W)
        
        # Center the frame
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        
        # Make window stay on top
        self.root.attributes('-topmost', True)
        
        # Disable close button
        self.root.protocol("WM_DELETE_WINDOW", lambda: None)
        
        self.total = 0
        self.current = 0
        
    def show_loading(self, message: str):
        """Show loading spinner with message"""
        self.progress_frame.grid_remove()
        self.loading_label.configure(text=f"⟳ {message}")
        self.loading_label.grid()
        self._animate_loading()
        self.root.update()
    
    def _animate_loading(self):
        """Animate the loading spinner"""
        current_text = self.loading_label.cget("text")
        if "⟳" in current_text:
            new_text = current_text.replace("⟳", "⟲")
        else:
            new_text = current_text.replace("⟲", "⟳")
        self.loading_label.configure(text=new_text)
        self.root.after(500, self._animate_loading)
    
    def start_progress(self, total_pincodes: int, message: str):
        """Switch to progress bar mode"""
        self.total = total_pincodes
        self.current = 0
        self.loading_label.grid_remove()
        self.progress_frame.grid()
        self.label_var.set(message)
        self.status_var.set(f"Processed: 0/{total_pincodes}")
        self.progress["value"] = 0
        self.root.update()
    
    def update_progress(self, current: int, message: str):
        """Update progress bar and message"""
        self.current = current
        progress = (current / self.total) * 100
        
        self.label_var.set(message)
        self.progress["value"] = progress
        self.status_var.set(f"Processed: {current}/{self.total}")
        self.root.update()
    
    def show_completion(self, excel_path: str, log_path: str):
        """Show completion message and close on OK"""
        excel_dir = os.path.dirname(excel_path)
        excel_name = os.path.basename(excel_path)
        log_name = os.path.basename(log_path)
        
        message = (
            "Process completed successfully!\n\n"
            f"Files saved in: {excel_dir}\n"
            f"Excel file: {excel_name}\n"
            f"Log file: {log_name}\n\n"
            "Click OK to close the application."
        )
        
        messagebox.showinfo("Success", message, parent=self.root)
        self.root.quit()
        sys.exit(0)
    
    def show_error(self, error_message: str):
        """Show error message and close on OK"""
        messagebox.showerror(
            "Error", 
            f"An error occurred:\n\n{error_message}\n\nClick OK to close the application.",
            parent=self.root
        )
        self.root.quit()
        sys.exit(1)

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

    def process_pincodes(self, pincodes: List[str], progress_window: ProgressWindow = None) -> Dict[str, List[str]]:
        """Fetch details for multiple pincodes and return a summary dict."""
        results = {"success": [], "failed": []}
        request_count = 0  # Counter to track requests
        
        for i, pc in enumerate(pincodes, 1):
            if progress_window:
                progress_window.update_progress(i-1, f"Processing pincode: {pc}")
            
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
                    if progress_window:
                        progress_window.update_progress(i, "Taking a short break to avoid rate limiting...")
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
                    if progress_window:
                        progress_window.update_progress(i, "Taking a short break to avoid rate limiting...")
                    print(f"✅ Processed {request_count} requests. Taking 20 second break...")
                    print(f"⏰ Remaining pincodes: {len(pincodes) - i}")
                    time.sleep(20)
                    print("🚀 Resuming processing...")
            
            # Update progress at the end of each iteration
            if progress_window:
                progress_window.update_progress(i, f"Processed pincode: {pc}")

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

        # State code mapping based on first two digits of pincode
        self.state_code_mapping = {
            '11': 'DL',
            '12': 'HR', '13': 'HR',
            '14': 'PB', '15': 'PB',
            '16': 'CH',
            '17': 'HP',
            '18': 'JK', '19': 'JK',
            '20': 'UP', '21': 'UP', '22': 'UP', '23': 'UP', '24': 'UP', 
            '25': 'UP', '26': 'UP', '27': 'UP', '28': 'UP',
            '30': 'RJ', '31': 'RJ', '32': 'RJ', '33': 'RJ', '34': 'RJ', '35': 'RJ',
            '36': 'GJ', '37': 'GJ', '38': 'GJ', '39': 'GJ',
            '40': 'MH', '41': 'MH', '42': 'MH', '43': 'MH', '44': 'MH',
            '45': 'MP', '46': 'MP', '47': 'MP', '48': 'MP',
            '49': 'CG',
            '50': 'TG',
            '51': 'AP', '52': 'AP', '53': 'AP',
            '56': 'KA', '57': 'KA', '58': 'KA', '59': 'KA',
            '60': 'TN', '61': 'TN', '62': 'TN', '63': 'TN', '64': 'TN', '65': 'TN', '66': 'TN',
            '67': 'KL', '68': 'KL', '69': 'KL',
            '70': 'WB', '71': 'WB', '72': 'WB', '73': 'WB', '74': 'WB',
            '75': 'OD', '76': 'OD', '77': 'OD',
            '78': 'AS',
            '790': 'AR', '791': 'AR', '792': 'AR',
            '793': 'ML', '794': 'ML',
            '795': 'MN',
            '796': 'MZ',
            '797': 'NL', '798': 'NL',
            '799': 'TR',
            '80': 'BR', '81': 'BR', '82': 'BR', '83': 'BR', '84': 'BR', '85': 'BR',
            '90': 'APS', '91': 'APS', '92': 'APS', '93': 'APS', '94': 'APS',
            '95': 'APS', '96': 'APS', '97': 'APS', '98': 'APS', '99': 'APS'
        }
        
        # Full state names mapping
        self.state_name_mapping = {
            'DL': 'Delhi',
            'HR': 'Haryana',
            'PB': 'Punjab',
            'CH': 'Chandigarh',
            'HP': 'Himachal Pradesh',
            'JK': 'Jammu and Kashmir',
            'UP': 'Uttar Pradesh',
            'RJ': 'Rajasthan',
            'GJ': 'Gujarat',
            'MH': 'Maharashtra',
            'MP': 'Madhya Pradesh',
            'CG': 'Chhattisgarh',
            'TG': 'Telangana',
            'AP': 'Andhra Pradesh',
            'KA': 'Karnataka',
            'TN': 'Tamil Nadu',
            'KL': 'Kerala',
            'WB': 'West Bengal',
            'OD': 'Odisha',
            'AS': 'Assam',
            'AR': 'Arunachal Pradesh',
            'ML': 'Meghalaya',
            'MN': 'Manipur',
            'MZ': 'Mizoram',
            'NL': 'Nagaland',
            'TR': 'Tripura',
            'BR': 'Bihar',
            'APS': 'Army Post Service'
        }
        
        # Special pincode mappings (commented out as per analalysis.py)
        self.special_pincodes = {
            # '396': ('DD', 'Dadra and Nagar Haveli and Daman and Diu'),
            # '403': ('GA', 'Goa'),
            # '605': ('PY', 'Puducherry'),
            # '682': ('LD', 'Lakshadweep'),
            # '737': ('SK', 'Sikkim'),
            # '744': ('AN', 'Andaman and Nicobar Islands')
        }
    
    def get_state_from_pincode(self, pincode):
        """Get state code and name based on pincode"""
        try:
            pincode_str = str(pincode)
            
            # Check for special pincodes first
            if pincode_str[:3] in self.special_pincodes:
                return self.special_pincodes[pincode_str[:3]]
            
            # Check for three-digit prefixes (like 790-799)
            if pincode_str[:3] in self.state_code_mapping:
                state_code = self.state_code_mapping[pincode_str[:3]]
                return state_code, self.state_name_mapping.get(state_code, 'Unknown')
            
            # Check for two-digit prefixes
            first_two = pincode_str[:2]
            if first_two in self.state_code_mapping:
                state_code = self.state_code_mapping[first_two]
                return state_code, self.state_name_mapping.get(state_code, 'Unknown')
            
            return 'Unknown', 'Unknown'
        except:
            return 'Unknown', 'Unknown'

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
        if all_data:
            df_all = pd.DataFrame(all_data)
            # Add state code and name columns based on pincode
            df_all[['State Code', 'State']] = pd.DataFrame(df_all['Pin Code'].apply(self.get_state_from_pincode).tolist(), index=df_all.index)
        else:
            df_all = pd.DataFrame()
        
        # Convert success data to DataFrame
        if success_data:
            df_success = pd.DataFrame(success_data)
            # Add state code and name columns based on pincode
            df_success[['State Code', 'State']] = pd.DataFrame(df_success['Pin Code'].apply(self.get_state_from_pincode).tolist(), index=df_success.index)
        else:
            df_success = pd.DataFrame()
        
        # Convert failed data to DataFrame
        if failed_data:
            df_failed = pd.DataFrame(failed_data)
            # Add state code and name columns based on pincode
            df_failed[['State Code', 'State']] = pd.DataFrame(df_failed['Pin Code'].apply(self.get_state_from_pincode).tolist(), index=df_failed.index)
        else:
            df_failed = pd.DataFrame()
        
        return df_all, df_success, df_failed

    def get_delivery_zone_data(self, df, state_code=None):
        """Get delivery zone data from MongoDB"""
        if state_code:
            df = df[(df['State Code'] == state_code)]

        # Convert Pin Code to string type to avoid comparison issues
        df['Pin Code'] = df['Pin Code'].astype(str)
        
        # Create a copy of the DataFrame with required columns
        df_with_states = df[['Pin Code', 'Zone Type', 'State Code', 'State']].copy()
        
        # Group by Pin Code and get value counts for Zone Type
        zone_counts = df.groupby('Pin Code')['Zone Type'].value_counts().unstack(fill_value=0)
        zone_counts['Total'] = zone_counts.sum(axis=1)
        
        if 'Delivery Zone' in zone_counts.columns:
            # Convert to numeric values and calculate percentage
            delivery_zone = pd.to_numeric(zone_counts['Delivery Zone'], errors='coerce')
            total = pd.to_numeric(zone_counts['Total'], errors='coerce')
            pr = (delivery_zone / total) * 100
            # Filter where percentage is >= 80
            filtered_pincodes = zone_counts[pr >= 80].index
            not_delivery_zone = zone_counts[pr < 80].index
            
            # Filter the original DataFrame to get state information
            df_delivery_zone = df_with_states[df_with_states['Pin Code'].isin(filtered_pincodes)]
            df_not_delivery_zone = df_with_states[df_with_states['Pin Code'].isin(not_delivery_zone)]
            # Drop duplicates to get one row per pincode
            df_delivery_zone = df_delivery_zone.drop_duplicates(subset='Pin Code')
            df_not_delivery_zone = df_not_delivery_zone.drop_duplicates(subset='Pin Code')
            
        else:
            df_delivery_zone = pd.DataFrame(columns=['Pin Code', 'State Code', 'State'])
            df_not_delivery_zone = pd.DataFrame(columns=['Pin Code', 'State Code', 'State'])
            
        return df_delivery_zone, df_not_delivery_zone

    def create_excel_file(self, df_all, df_success, df_failed, df_delivery_zone, df_not_delivery_zone_only_gujrat):
        """Create Excel file with multiple sheets"""
        # Put Excel file in store directory
        filename = os.path.join(self.store_dir, "anjani_courier_data.xlsx")
        
        print(f"Creating Excel file: {filename}")
        
        # Create Excel writer object
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Prepare dataframes first
            df_all_copy = df_all.copy()
            df_all_copy = df_all_copy.sort_values(by='Pin Code')
            
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
            
            # Sheet 2: All Pincode Details
            if not df_all_copy.empty:
                df_all_sorted = df_all_copy.copy()
                df_all_sorted = df_all_sorted.drop(['Inserted At'], axis=1, errors='ignore')
                df_all_sorted.to_excel(writer, sheet_name='All Pincode Details', index=False)
                
                # Format the worksheet
                worksheet = writer.sheets['All Pincode Details']
                self.format_worksheet(worksheet, df_all_sorted)
                print(f"All Pincode Details sheet created with {len(df_all_sorted)} records")
            
            # Sheet 5: Delivery Zone Summary
            if not df_delivery_zone.empty:
                df_delivery_zone = df_delivery_zone.sort_values(by='Pin Code')
                df_zone_summary = df_delivery_zone[["Pin Code", "State Code", "State"]]
                df_zone_summary.to_excel(writer, sheet_name='Possible Delivery Zone', index=False)
                
                # Format the worksheet
                worksheet = writer.sheets['Possible Delivery Zone']
                self.format_worksheet(worksheet, df_zone_summary)
                print(f"Delivery Zone Summary sheet created with {len(df_zone_summary)} records")

            # Sheet 6: Gujarat Not Delivery Zone
            if not df_not_delivery_zone_only_gujrat.empty:
                df_not_delivery_zone_only_gujrat = df_not_delivery_zone_only_gujrat.sort_values(by='Pin Code')
                df_zone_summary = df_not_delivery_zone_only_gujrat[["Pin Code"]]
                df_zone_summary.to_excel(writer, sheet_name='Gujarat Not Delivery Zone', index=False)
                # Format the worksheet
                worksheet = writer.sheets['Gujarat Not Delivery Zone']
                self.format_worksheet(worksheet, df_zone_summary)
                print(f"Gujarat Not Delivery Zone sheet created with {len(df_zone_summary)} records")
        
        print(f"Excel file '{filename}' created successfully!")
        return filename
    
    def export_to_excel(self):
        """Main method to export JSON data to Excel"""
        try:
            # Fetch data from JSON files
            all_data, success_data, failed_data = self.fetch_all_data()
            
            # Convert to DataFrames
            df_all, df_success, df_failed = self.convert_to_dataframes(all_data, success_data, failed_data)
            df_delivery_zone, df_not_delivery_zone = self.get_delivery_zone_data(df_all)
            df_delivery_zone_only_gujrat, df_not_delivery_zone_only_gujrat = self.get_delivery_zone_data(df_all, 'GJ')

            # Create Excel file
            filename = self.create_excel_file(df_all, df_success, df_failed, df_delivery_zone, df_not_delivery_zone_only_gujrat)
            
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

def resource_path(relative_path):
    """Get absolute path to resource, works for dev and for PyInstaller"""
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    
    return os.path.join(base_path, relative_path)

def select_csv_file():
    """Open file dialog to select CSV file"""
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    
    # Show file dialog
    file_path = filedialog.askopenfilename(
        title="Select Pincode CSV File",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
    )
    
    if not file_path:
        messagebox.showerror("Error", "No file selected. Please select a CSV file with pincodes.")
        return None
        
    return file_path

def get_pincode_list(file_path=None):
    try:
        # If no file path provided, try default or ask user to select
        if not file_path:
            default_path = resource_path("pincodes.csv")
            if os.path.exists(default_path):
                file_path = default_path
            else:
                file_path = select_csv_file()
                if not file_path:
                    return []

        # Read CSV
        df = pd.read_csv(file_path, encoding='utf-8')

        # Normalize column names to handle case mismatches
        df.columns = [col.strip().upper() for col in df.columns]

        if "PINCODE" not in df.columns:
            messagebox.showerror("Error", "No 'PINCODE' column found in the CSV file.")
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
        messagebox.showerror("Error", f"File not found: {file_path}")
        return []
    except Exception as e:
        messagebox.showerror("Error", f"Error reading CSV file: {str(e)}")
        return []

def run_scraping_and_analysis():
    # Create directories
    store_dir = "store"
    os.makedirs(store_dir, exist_ok=True)
    
    # Create timestamp for log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"scraping_analysis_log_{timestamp}.txt"
    log_path = os.path.join(store_dir, log_filename)
    
    # Set up logging
    logger = Logger(log_filename)
    sys.stdout = logger
    
    # Create progress window
    progress_window = ProgressWindow()
    
    try:
        print(f"Starting scraping and analysis process at {datetime.now()}")
        print("="*50)
        
        # Step 1: Initialize and get pincode list
        progress_window.show_loading("Loading and validating input data...")
        
        # Get pincode list - will prompt for file if needed
        pincodes = get_pincode_list()
        if not pincodes:
            progress_window.show_error("No pincodes found to process!")
            return
        
        # Ask user if they want to proceed
        proceed = messagebox.askyesno(
            "Confirm",
            f"Found {len(pincodes)} pincodes to process.\nDo you want to continue?",
            parent=progress_window.root
        )
        
        if not proceed:
            progress_window.show_error("Process cancelled by user.")
            return
        
        # Step 2: Process Pincodes
        progress_window.start_progress(len(pincodes), "Starting pincode processing...")
        
        # Initialize scraper and process pincodes
        client = AnjaniCourierClient()
        summary = client.process_pincodes(pincodes, progress_window)
        
        print("\nScraping Summary:")
        print(f"Successfully processed: {len(summary['success'])} pincodes")
        print(f"Failed to process: {len(summary['failed'])} pincodes")
        
        # Step 3: Generate Excel
        progress_window.show_loading("Generating Excel report...")
        
        # Initialize exporter and run analysis
        exporter = JsonToExcelExporter()
        excel_file = exporter.export_to_excel()
        
        if excel_file:
            print(f"\nProcess completed successfully!")
            print(f"Excel file generated: {excel_file}")
            print(f"Log file generated: {log_path}")
            print(f"JSON files stored in: {os.path.join(store_dir, 'temp_data')}")
            
            # Show completion message and close
            progress_window.show_completion(excel_file, log_path)
        else:
            progress_window.show_error("Failed to generate Excel file!")
            
    except Exception as e:
        error_msg = f"Error occurred during processing: {str(e)}"
        print(f"\n{error_msg}")
        progress_window.show_error(error_msg)
    finally:
        # Restore original stdout
        sys.stdout = sys.__stdout__
        print(f"\nProcess completed. Check {log_path} for detailed logs.")

if __name__ == "__main__":
    run_scraping_and_analysis()