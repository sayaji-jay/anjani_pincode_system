import httpx
from bs4 import BeautifulSoup
from typing import Dict, List, Any
from datetime import datetime
import logging
import sys
import asyncio
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - [%(module)s] %(message)s | %(msecs)dms',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('tracking.log')
    ]
)
logger = logging.getLogger(__name__)

def convert_to_iso_datetime(date_str: str) -> str:
    """Convert various date formats to ISO format (YYYY-MM-DDTHH:mm:ss)"""
    try:
        # Remove any extra spaces, arrows and normalize separators
        date_str = date_str.replace("->", "").strip()
        
        # Try to extract date and time parts
        parts = date_str.split()
        if len(parts) < 2:  # If only date is present
            date_part = parts[0]
            time_part = "00:00"
            am_pm = ""
        else:
            date_part = parts[0]
            time_part = parts[1]
            am_pm = parts[2] if len(parts) > 2 else ""
        
        # Parse date part
        date_separators = ['/', '-']
        for sep in date_separators:
            if sep in date_part:
                day, month, year = date_part.split(sep)
                # Handle 2-digit year
                if len(year) == 2:
                    year = '20' + year  # Assuming years 2000-2099
                break
        
        # Parse time part
        hour, minute = time_part.split(':')
        hour = int(hour)
        
        # Handle AM/PM
        if am_pm.upper() == 'PM' and hour < 12:
            hour += 12
        elif am_pm.upper() == 'AM' and hour == 12:
            hour = 0
            
        # Format to ISO
        try:
            formatted_date = f"{year}-{int(month):02d}-{int(day):02d}T{hour:02d}:{int(minute):02d}:00"
            # Validate the date by parsing it
            datetime.strptime(formatted_date, "%Y-%m-%dT%H:%M:%S")
            return formatted_date
        except ValueError as e:
            logger.warning(f"Invalid date components: {date_str}, Error: {str(e)}")
            return date_str
            
    except Exception as e:
        logger.error(f"Error converting date: {date_str}, Error: {str(e)}")
        return date_str

class TrackingInfoScraper:
    """Class to handle tracking information scraping from Anjani Courier website"""
    
    def __init__(self):
        self.base_url = "http://anjanicourier.in/Doc_Track.aspx"
        self.async_client = None

    async def fetch_page(self, tracking_number: str) -> BeautifulSoup:
        """Fetch the tracking page and create BeautifulSoup object"""
        url = f"{self.base_url}?No={tracking_number}"
        if self.async_client is None:
            self.async_client = httpx.AsyncClient()

        try:
            start_time = datetime.now()
            response = await self.async_client.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            time_taken = (datetime.now() - start_time).total_seconds() * 1000
            logger.info(f"Successfully fetched tracking page for {tracking_number} | {time_taken:.2f}")
            return soup
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            raise

    def extract_tracking_steps(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract and process tracking steps from the page"""
        tracking_steps = []
        tracking_rows = soup.select("#EntryTbl tr")
        
        i = 0
        while i < len(tracking_rows):
            tds = tracking_rows[i].find_all("td")
            if len(tds) >= 2:
                text = tds[1].get_text(strip=True)
                
                # Process ROUTE entries
                if not text.startswith(("OUT", "IN")):
                    parts = [p.strip() for p in text.split("->")]
                    location_from = parts[0]
                    location_to = parts[1] if len(parts) > 1 else ""

                    # Check if next row has status
                    status = None
                    datetime_str = None
                    if i + 1 < len(tracking_rows):
                        next_tds = tracking_rows[i + 1].find_all("td")
                        if len(next_tds) >= 2:
                            next_text = next_tds[1].get_text(strip=True)
                            if next_text.startswith(("OUT", "IN")):
                                status = "OUT" if next_text.startswith("OUT") else "IN"
                                raw_datetime = next_text.replace(f"{status} -> ", "").replace("->", "").strip()
                                datetime_str = convert_to_iso_datetime(raw_datetime)
                                i += 1  # Skip next row as we processed it

                    tracking_steps.append({
                        "type": "ROUTE",
                        "status": status,
                        "location_from": location_from,
                        "location_to": location_to if location_to else None,
                        "datetime": datetime_str
                    })
            i += 1
        
        return tracking_steps

    def extract_additional_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract additional tracking information"""
        def safe_get_text(element_id: str) -> str:
            try:
                return soup.find("span", id=element_id).get_text(strip=True)
            except AttributeError:
                return ""

        # Get basic info
        status = safe_get_text("lblStatus").upper()
        from_center_text = safe_get_text("lblCenterDetail")
        
        # Parse from_center
        from_center = {"name": "", "address": ""}
        if from_center_text:
            parts = from_center_text.split(" - ")
            if len(parts) >= 2:
                from_center["name"] = parts[0].upper()
                from_center["address"] = parts[1]
            else:
                from_center["name"] = from_center_text.upper()
                from_center["address"] = from_center_text

        # Parse last_center info
        last_center = {
            "name": safe_get_text("lastCenterName"),
            "phone": safe_get_text("lastCenterph"),
            "contact": {"name": "", "mobile": ""},
            "manager": {"phone": "", "note": ""}
        }
        
        # Parse contact
        contact_text = safe_get_text("lastCenterContact")
        if "Mobile:" in contact_text:
            parts = contact_text.split("Mobile:")
            last_center["contact"]["name"] = parts[0].strip().rstrip(",").strip()
            last_center["contact"]["mobile"] = parts[1].strip()
        else:
            last_center["contact"]["name"] = contact_text

        # Parse manager
        manager_text = safe_get_text("lastCenterMgr")
        if "Ph:" in manager_text:
            phone = manager_text.split("Ph:")[1].strip()
            last_center["manager"]["phone"] = phone
            last_center["manager"]["note"] = "Call for gate pass" if phone else ""
        elif "Ph" in manager_text:
            last_center["manager"]["note"] = "Call for gate pass"

        return {
            "status": status,
            "from_center": from_center,
            "last_center": last_center
        }

    async def get_tracking_info(self, tracking_number: str) -> Dict[str, Any]:
        """Get tracking information for a single tracking number"""
        try:
            start_time = datetime.now()
            
            soup = await self.fetch_page(tracking_number)
            tracking_steps = self.extract_tracking_steps(soup)
            additional_info = self.extract_additional_info(soup)

            time_taken = (datetime.now() - start_time).total_seconds() * 1000
            logger.info(f"Successfully processed tracking information for {tracking_number} | {time_taken:.2f}")

            return {
                "trackingno": tracking_number,
                "status": additional_info["status"],
                "from_center": additional_info["from_center"],
                "last_center": additional_info["last_center"],
                "tracking_steps": tracking_steps
            }
        except Exception as e:
            logger.error(f"Error processing tracking information for {tracking_number}: {str(e)}")
            return {"trackingno": tracking_number, "error": str(e)}

    async def get_multiple_tracking_info(self, tracking_numbers: List[str]) -> List[Dict[str, Any]]:
        """Get tracking information for multiple tracking numbers concurrently"""
        try:
            start_time = datetime.now()
            tasks = [self.get_tracking_info(number) for number in tracking_numbers]
            results = await asyncio.gather(*tasks)
            
            time_taken = (datetime.now() - start_time).total_seconds() * 1000
            logger.info(f"Processed {len(tracking_numbers)} tracking numbers | {time_taken:.2f}")
            
            return results
        finally:
            if self.async_client:
                await self.async_client.aclose()

async def fetch_tracking_numbers() -> List[str]:
    """Fetch tracking numbers from API"""
    api_url = "http://15.206.233.194:3002/paymentms/unicommerce_detail/anjaniundeliveredtrakingno"
    
    async with httpx.AsyncClient() as client:
        try:
            start_time = datetime.now()
            response = await client.post(api_url)
            response.raise_for_status()
            data = response.json()
            
            time_taken = (datetime.now() - start_time).total_seconds() * 1000
            logger.info(f"Successfully fetched tracking numbers from API | {time_taken:.2f}")
            
            if data.get("code") == 200 and data.get("flag") == 1:
                return data.get("data", [])
            else:
                raise Exception(f"API returned error: {data.get('message', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"Error fetching tracking numbers: {str(e)}")
            raise

async def update_tracking_details(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Send tracking results to API"""
    api_url = "http://15.206.233.194:3002/paymentms/unicommerce_detail/updateanjanitraking"
    
    print(json.dumps(results))
    
    async with httpx.AsyncClient() as client:
        try:
            start_time = datetime.now()
            response = await client.post(api_url, json=results)
            response.raise_for_status()
            data = response.json()
            
            time_taken = (datetime.now() - start_time).total_seconds() * 1000
            logger.info(f"Successfully sent tracking results to API | {time_taken:.2f}")
            
            return data
        except Exception as e:
            logger.error(f"Error sending results: {str(e)}")
            raise

async def main():
    """Main function to process tracking information"""
    try:
        # For testing, use hardcoded data
        tracking_numbers = ["1354658818"]
        
        # Uncomment below to fetch from actual API
        # tracking_numbers = await fetch_tracking_numbers()
        
        if tracking_numbers:
            logger.info(f"Received {len(tracking_numbers)} tracking numbers to process")
            
            scraper = TrackingInfoScraper()
            results = await scraper.get_multiple_tracking_info(tracking_numbers)
            
            # Send results to API
            api_response = await update_tracking_details(results)
            logger.info(f"API Response: {api_response}")
        else:
            logger.error("No tracking numbers to process")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())