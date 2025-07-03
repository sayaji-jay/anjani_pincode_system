from __future__ import annotations

import json
import time
from typing import Dict, List, Optional
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import pymongo

__all__ = [
    "AnjaniCourierClient",
]
class AnjaniCourierClient:
    _BASE_URL = "http://www.anjanicourier.in/"
    _PINCODE_ENDPOINT = _BASE_URL + "Rpt_PinCodeShow.aspx"

    def __init__(self) -> None:
        self.username =  "ADR25"
        self.password =  "ADR25"
        self.headless = True
        # MongoDB setup
        self.mongo_uri: str = "mongodb+srv://justj:justjay19@cluster0.fsgzjrl.mongodb.net/"
        self.db_name: str = "anjani"

        # Collection names
        self.pincode_collection_name: str = "pincodes"          # Stores detailed rows
        self.success_collection_name: str = "pincode_successes"  # Stores only pincode + timestamp for successes
        self.failed_collection_name: str = "pincode_failures"    # Stores only pincode + timestamp for failures

        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]

        # Pre-fetch collections for easy reuse
        self.pincode_collection = self.db[self.pincode_collection_name]
        self.success_collection = self.db[self.success_collection_name]
        self.failed_collection = self.db[self.failed_collection_name]

        # Start Selenium login once per client instance
        self.session_id: str = self._login_and_get_session_id()

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
                    "Inserted At": datetime.now(),
                    "Branch Name": current_branch or "Unknown",
                    "Area Name": cols[2].get_text(strip=True),
                    "Zone Type": cols[3].get_text(strip=True),
                    "Delivery Type": cols[5].get_text(strip=True),
                    "Transit Days": cols[6].get_text(strip=True),
                }

                # Insert detailed row
                self.pincode_collection.insert_one(item)
                found_records = True

        # After processing the table, log success/failure summary per pincode
        summary_doc = {
            "Pin Code": pc_code,
            "Checked At": datetime.now(),
        }

        if found_records:
            summary_doc["Status"] = "success"
            self.success_collection.insert_one(summary_doc)
        else:
            summary_doc["Status"] = "failed"
            summary_doc["Reason"] = "No records found"
            self.failed_collection.insert_one(summary_doc)

        return found_records

    def process_pincodes(self, pincodes: List[str]) -> Dict[str, List[str]]:
        """Fetch details for multiple pincodes and return a summary dict."""
        results = {"success": [], "failed": []}
        request_count = 0  # Counter to track requests
        
        for i, pc in enumerate(pincodes, 1):
            print(f"Processing pincode {i}/{len(pincodes)}: {pc}")
                    # Check if pincode already exists in success collection
            existing_success = self.success_collection.find_one({"Pin Code": int(pc)})
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
                self.failed_collection.insert_one({
                    "Pin Code": pc,
                    "Checked At": datetime.now(),
                    "Status": "failed",
                    "Reason": str(exc),
                })
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


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------
import pandas as pd

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


if __name__ == "__main__":
    client = AnjaniCourierClient()

    # Replace with whatever list of pincodes you need to process
    sample_pincodes =get_pincode_list("pincodes.csv")
    # sample_pincodes =["382165"]

    summary = client.process_pincodes(sample_pincodes)
    # print("Processing summary:", summary)

