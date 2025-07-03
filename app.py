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

        response = httpx.get(self._PINCODE_ENDPOINT, params=params, cookies=cookies)
        response.raise_for_status()

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
                    "pc_code": pc_code,
                    "inserted_at": datetime.now(),
                    "branch_name": current_branch or "Unknown",
                    "area_name": cols[2].get_text(strip=True),
                    "zone_type": cols[3].get_text(strip=True),
                    "delivery_type": cols[5].get_text(strip=True),
                    "transit_days": cols[6].get_text(strip=True),
                }

                # Insert detailed row
                self.pincode_collection.insert_one(item)
                found_records = True

        # After processing the table, log success/failure summary per pincode
        summary_doc = {
            "pc_code": pc_code,
            "checked_at": datetime.now(),
        }

        if found_records:
            summary_doc["status"] = "success"
            self.success_collection.insert_one(summary_doc)
        else:
            summary_doc["status"] = "failed"
            summary_doc["reason"] = "No records found"
            self.failed_collection.insert_one(summary_doc)

        return found_records

    def process_pincodes(self, pincodes: List[str]) -> Dict[str, List[str]]:
        """Fetch details for multiple pincodes and return a summary dict."""
        results = {"success": [], "failed": []}

        for pc in pincodes:
            try:
                ok = self.fetch_pincode_details(pc)
                if ok:
                    results["success"].append(pc)
                else:
                    results["failed"].append(pc)
            except Exception as exc:
                # Treat unhandled errors as failures and log them
                self.failed_collection.insert_one({
                    "pc_code": pc,
                    "checked_at": datetime.now(),
                    "status": "failed",
                    "reason": str(exc),
                })
                results["failed"].append(pc)

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
                    return str(cookie.get("value"))

            raise RuntimeError("Login succeeded but session cookie not found")
        finally:
            driver.quit()


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    client = AnjaniCourierClient()

    # Replace with whatever list of pincodes you need to process
    sample_pincodes = [
        "110001",  # Example: New Delhi
        "400001",  # Example: Mumbai
        "999999",  # Likely invalid – will demonstrate failure logging
    ]

    summary = client.process_pincodes(sample_pincodes)
    print("Processing summary:", summary)

