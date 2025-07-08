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

def parse_status(status_text: str) -> str:
    """Parse status text to extract the main status"""
    try:
        if not status_text:
            return ""
            
        # Convert to uppercase and split by spaces
        parts = status_text.upper().split()
        
        # Handle different status types
        if "DELIVERED" in parts:
            return "DELIVERED"
        elif "UNDELIVERED" in parts:
            return "UNDELIVERED"
        elif "PENDING" in parts:
            return "PENDING"
        elif "RETURN" in parts or "RTD" in parts:
            return "RETURN"
        elif "TRANSIT" in parts:
            return "IN_TRANSIT"
        else:
            # If no specific status found, return first word
            return parts[0] if parts else ""
            
    except Exception as e:
        logger.error(f"Error parsing status: {status_text}, Error: {str(e)}")
        return status_text

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
        raw_status = safe_get_text("lblStatus")
        status = parse_status(raw_status)
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
        #tracking_numbers =[
    #     "1354659050",
    #     "1354659041",
    #     "1354659040",
    #     "1354659039",
    #     "1354659090",
    #     "1354659048",
    #     "1354659049",
    #     "1354659031",
    #     "1354659038",
    #     "1354659027",
    #     "1354659025",
    #     "1354659032",
    #     "1354659033",
    #     "1354659036",
    #     "1354659098",
    #     "1354659037",
    #     "1354659030",
    #     "1354659047",
    #     "1354659044",
    #     "1354659029",
    #     "1354659035",
    #     "1354659046",
    #     "1354659019",
    #     "1354659021",
    #     "1354659091",
    #     "1354659022",
    #     "1354659018",
    #     "1354659034",
    #     "1354659043",
    #     "1354659023",
    #     "1354659045",
    #     "1354659042",
    #     "1354659089",
    #     "1354659026",
    #     "1354659020",
    #     "1354658987",
    #     "1354659017",
    #     "1354659028",
    #     "1354659024",
    #     "1354659009",
    #     "1354659015",
    #     "1354659086",
    #     "1354659080",
    #     "1354659079",
    #     "1354659085",
    #     "1354659081",
    #     "1354659084",
    #     "1354659087",
    #     "1354659088",
    #     "1354659083",
    #     "1354659078",
    #     "1354659010",
    #     "1354659012",
    #     "1354659014",
    #     "1354659013",
    #     "1354659003",
    #     "1354659005",
    #     "1354659007",
    #     "1354659006",
    #     "1354658993",
    #     "1354658999",
    #     "1354659011",
    #     "1354659002",
    #     "1354659016",
    #     "1354659008",
    #     "1354659001",
    #     "1354659000",
    #     "1354658997",
    #     "1354658992",
    #     "1354659082",
    #     "1354658988",
    #     "1354658996",
    #     "1354659004",
    #     "1354658998",
    #     "1354659077",
    #     "1354658995",
    #     "1354658984",
    #     "1354658994",
    #     "1354658986",
    #     "1354658983",
    #     "1354658982",
    #     "1354658990",
    #     "1354658980",
    #     "1354658989",
    #     "1354658981",
    #     "1354658978",
    #     "1354658979",
    #     "1354659072",
    #     "1354658972",
    #     "1354658969",
    #     "1354658967",
    #     "1354658985",
    #     "1354658970",
    #     "1354658973",
    #     "1354658968",
    #     "1354658964",
    #     "1354658963",
    #     "1354659071",
    #     "1354658971",
    #     "1354658974",
    #     "1354658977",
    #     "1354658962",
    #     "1354659076",
    #     "1354658976",
    #     "1354658975",
    #     "1354658958",
    #     "1354658961",
    #     "1354658966",
    #     "1354658965",
    #     "1354658957",
    #     "1354658955",
    #     "1354658953",
    #     "1354658956",
    #     "1354658949",
    #     "1354658954",
    #     "1354658960",
    #     "1354658952",
    #     "1354659074",
    #     "1354659075",
    #     "1354659073",
    #     "1354659060",
    #     "1354659064",
    #     "1354659069",
    #     "1354658947",
    #     "1354658950",
    #     "1354658959",
    #     "1354658951",
    #     "1354658944",
    #     "1354658943",
    #     "1354659067",
    #     "1354658945",
    #     "1354658946",
    #     "1354658941",
    #     "1354658948",
    #     "1354658935",
    #     "1354659066",
    #     "1354658940",
    #     "1354658937",
    #     "1354658933",
    #     "1354658931",
    #     "1354658925",
    #     "1354658929",
    #     "1354658942",
    #     "1354658934",
    #     "1354658930",
    #     "1354659062",
    #     "1354659070",
    #     "1354658926",
    #     "1354658924",
    #     "1354658928",
    #     "1354658918",
    #     "1354658920",
    #     "1354658927",
    #     "1354658923",
    #     "1354658917",
    #     "1354658939",
    #     "1354658938",
    #     "1354658936",
    #     "1354658916",
    #     "1354658915",
    #     "1354658911",
    #     "1354659705",
    #     "1354658913",
    #     "1354658922",
    #     "1354658903",
    #     "1354658907",
    #     "1354658906",
    #     "1354659093",
    #     "1354658932",
    #     "1354658912",
    #     "1354658910",
    #     "1354659716",
    #     "1354658905",
    #     "1354658901",
    #     "1354658921",
    #     "1354659714",
    #     "1354658892",
    #     "1354659715",
    #     "1354659708",
    #     "1354658889",
    #     "1354659702",
    #     "1354659751",
    #     "1354659713",
    #     "1354658891",
    #     "1354658908",
    #     "1354658890",
    #     "1354658887",
    #     "1354658888",
    #     "1354658909",
    #     "1354658919",
    #     "1354658904",
    #     "1354658900",
    #     "1354658914",
    #     "1354659696",
    #     "1354659700",
    #     "1354658893",
    #     "1354658902",
    #     "1354658885",
    #     "1354659706",
    #     "1354659710",
    #     "1354658895",
    #     "1354658894",
    #     "1354659704",
    #     "1354658881",
    #     "1354659698",
    #     "1354658899",
    #     "1354658897",
    #     "1354658898",
    #     "1354659707",
    #     "1354658896",
    #     "1354658870",
    #     "1354658864",
    #     "1354658863",
    #     "1354658878",
    #     "1354658872",
    #     "1354659709",
    #     "1354659701",
    #     "1354658877",
    #     "1354658886",
    #     "1354658855",
    #     "1354659711",
    #     "1354658853",
    #     "1354659703",
    #     "1354658879",
    #     "1354658848",
    #     "1354658868",
    #     "1354658856",
    #     "1354658869",
    #     "1354658873",
    #     "1354658851",
    #     "1354658865",
    #     "1354658852",
    #     "1354658882",
    #     "1354658867",
    #     "1354658859",
    #     "1354659065",
    #     "1354658880",
    #     "1354659068",
    #     "1354658846",
    #     "1354659746",
    #     "1354658874",
    #     "1354658871",
    #     "1354658858",
    #     "1354659056",
    #     "1354659055",
    #     "1354658839",
    #     "1354659053",
    #     "1354658850",
    #     "1354658837",
    #     "1354659063",
    #     "1354658860",
    #     "1354658857",
    #     "1354658875",
    #     "1354658833",
    #     "1354658876",
    #     "1354658840",
    #     "1354658843",
    #     "1354658845",
    #     "1354658883",
    #     "1354658844",
    #     "1354658884",
    #     "1354658841",
    #     "1354659097",
    #     "1354658866",
    #     "1354659092",
    #     "1354659054",
    #     "1354659061",
    #     "1354658862",
    #     "1354658838",
    #     "1354658835",
    #     "1354658827",
    #     "1354658849",
    #     "1354658824",
    #     "1354658861",
    #     "1354658854",
    #     "1354658836",
    #     "1354658826",
    #     "1354658842",
    #     "1354658832",
    #     "1354658825",
    #     "1354658829",
    #     "1354658831",
    #     "1354658828",
    #     "1354659059",
    #     "1354659052",
    #     "1354659096",
    #     "1354659058",
    #     "1354658847",
    #     "1354658820",
    #     "1354658821",
    #     "1354658830",
    #     "1354658834",
    #     "1354658823",
    #     "1354658822",
    #     "1354658819",
    #     "1354658818"
    # ]
        # Uncomment below to fetch from actual API   
        tracking_numbers = await fetch_tracking_numbers()
        for i in range(0, len(tracking_numbers), 10):
            tracking_numbers_batch = tracking_numbers[i:i+10]
            if tracking_numbers_batch:
                logger.info(f"Received {len(tracking_numbers_batch)} tracking numbers to process")
                
                scraper = TrackingInfoScraper()
                results = await scraper.get_multiple_tracking_info(tracking_numbers_batch)
                
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