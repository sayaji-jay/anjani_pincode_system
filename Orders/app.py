import httpx
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
import logging
import sys
from dataclasses import dataclass
import asyncio
from concurrent.futures import ThreadPoolExecutor

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

@dataclass
class LastCenter:
    """Data class to store last center information"""
    name: str
    phone: str
    contact: str
    manager: str

@dataclass
class TrackingStep:
    """Data class to store tracking step information"""
    tracking_number: str
    type: str
    status: Optional[str]
    location_from: str
    location_to: str
    datetime: Optional[str]

@dataclass
class APIResponse:
    """Data class to store API response"""
    flag: int
    code: int
    message: str
    data: List[str]

class TrackingInfoScraper:
    """Class to handle tracking information scraping from Anjani Courier website"""
    
    def __init__(self):
        """Initialize the scraper"""
        self.base_url = "http://anjanicourier.in/Doc_Track.aspx"
        self.async_client = None

    def get_tracking_url(self, tracking_number: str) -> str:
        """Generate the full tracking URL"""
        return f"{self.base_url}?No={tracking_number}"

    async def fetch_page(self, tracking_number: str) -> BeautifulSoup:
        """Fetch the tracking page and create BeautifulSoup object"""
        url = self.get_tracking_url(tracking_number)
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
        except httpx.RequestError as e:
            logger.error(f"Request error occurred while fetching {url}: {str(e)}")
            raise
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred while fetching {url}: {str(e)}")
            raise

    def parse_tracking_rows(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Parse tracking table rows and extract step information"""
        raw_steps = []
        tracking_rows = soup.select("#EntryTbl tr")
        
        for tr in tracking_rows:
            tds = tr.find_all("td")
            if len(tds) >= 2:
                text = tds[1].get_text(strip=True)
                step_type = "ROUTE"
                if text.startswith("OUT"):
                    step_type = "OUT"
                elif text.startswith("IN"):
                    step_type = "IN"
                raw_steps.append({"type": step_type, "text": text})
        
        return raw_steps

    def process_tracking_steps(self, raw_steps: List[Dict[str, str]], tracking_number: str) -> List[TrackingStep]:
        """Process raw tracking steps into structured data"""
        tracking_steps = []
        i = 0
        
        while i < len(raw_steps):
            if raw_steps[i]["type"] == "ROUTE":
                route = raw_steps[i]["text"]
                parts = [p.strip() for p in route.split("->")]
                location_from = parts[0]
                location_to = parts[1] if len(parts) > 1 else ""

                if i + 1 < len(raw_steps) and raw_steps[i + 1]["type"] in ["OUT", "IN"]:
                    status = raw_steps[i + 1]["type"]
                    datetime_str = raw_steps[i + 1]["text"].replace(f"{status} -> ", "")
                    datetime_str = datetime_str.replace("->", "").strip()

                    tracking_steps.append(TrackingStep(
                        tracking_number=tracking_number,
                        type="ROUTE",
                        status=status,
                        location_from=location_from,
                        location_to=location_to,
                        datetime=datetime_str
                    ))
                    i += 2
                else:
                    tracking_steps.append(TrackingStep(
                        tracking_number=tracking_number,
                        type="ROUTE",
                        status=None,
                        location_from=location_from,
                        location_to=location_to,
                        datetime=None
                    ))
                    i += 1
            else:
                i += 1
        
        return tracking_steps

    def get_additional_info(self, soup: BeautifulSoup) -> tuple[str, str, LastCenter]:
        """Extract additional tracking information"""
        status = ""
        from_center = ""
        last_center = LastCenter(name="", phone="", contact="", manager="")

        try:
            status = soup.find("span", id="lblStatus").get_text(strip=True)
        except AttributeError:
            logger.warning("Status information not found")

        try:
            from_center = soup.find("span", id="lblCenterDetail").get_text(
                separator=" ", strip=True
            )
        except AttributeError:
            logger.warning("Center detail information not found")

        try:
            last_center = LastCenter(
                name=soup.find("span", id="lastCenterName").get_text(strip=True),
                phone=soup.find("span", id="lastCenterph").get_text(strip=True),
                contact=soup.find("span", id="lastCenterContact").get_text(strip=True),
                manager=soup.find("span", id="lastCenterMgr").get_text(strip=True)
            )
        except AttributeError:
            logger.warning("Last center information not found")

        return status, from_center, last_center

    async def get_tracking_info(self, tracking_number: str) -> Dict[str, Any]:
        """Get tracking information for a single tracking number"""
        try:
            start_time = datetime.now()
            
            soup = await self.fetch_page(tracking_number)
            raw_steps = self.parse_tracking_rows(soup)
            tracking_steps = self.process_tracking_steps(raw_steps, tracking_number)
            status, from_center, last_center = self.get_additional_info(soup)

            time_taken = (datetime.now() - start_time).total_seconds() * 1000
            logger.info(f"Successfully processed tracking information for {tracking_number} | {time_taken:.2f}")

            # Convert tracking steps to dictionaries
            tracking_steps_dict = [
                {
                    "tracking_number": step.tracking_number,
                    "type": step.type,
                    "status": step.status,
                    "location_from": step.location_from,
                    "location_to": step.location_to,
                    "datetime": step.datetime
                } for step in tracking_steps
            ]

            # Convert last center to dictionary
            last_center_dict = {
                "name": last_center.name,
                "phone": last_center.phone,
                "contact": last_center.contact,
                "manager": last_center.manager
            }

            return {
                "tracking_number": tracking_number,
                "tracking_steps": tracking_steps_dict,
                "status": status,
                "from_center": from_center,
                "last_center": last_center_dict
            }
        except Exception as e:
            logger.error(f"Error processing tracking information for {tracking_number}: {str(e)}")
            return {
                "tracking_number": tracking_number,
                "error": str(e)
            }

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














async def fetch_tracking_numbers() -> APIResponse:
    """Fetch tracking numbers from API"""
    api_url = "http://15.206.233.194:3002/paymentms/unicommerce_detail/anjaniundeliveredtrakingno"  # Replace this URL with your actual API endpoint
    
    async with httpx.AsyncClient() as client:
        try:
            start_time = datetime.now()
            response = await client.post(api_url)
            response.raise_for_status()
            data = response.json()
            
            time_taken = (datetime.now() - start_time).total_seconds() * 1000
            logger.info(f"Successfully fetched tracking numbers from API | {time_taken:.2f}")
            
            return APIResponse(
                flag=data["flag"],
                code=data["code"],
                message=data["message"],
                data=data["data"]
            )
        except httpx.RequestError as e:
            logger.error(f"Request error occurred while fetching tracking numbers: {str(e)}")
            raise
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred while fetching tracking numbers: {str(e)}")
            raise
        except KeyError as e:
            logger.error(f"Invalid API response format: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while fetching tracking numbers: {str(e)}")
            raise

async def update_tracking_details(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Send tracking results to API"""
    api_url = "http://15.206.233.194:3002/paymentms/unicommerce_detail/updateanjanitraking"  # Your result API endpoint
    
    # Prepare the data to send
    payload = results    
    
    async with httpx.AsyncClient() as client:
        try:
            start_time = datetime.now()
            response = await client.post(api_url, json=payload)
            response.raise_for_status()
            data = response.json()
            
            time_taken = (datetime.now() - start_time).total_seconds() * 1000
            logger.info(f"Successfully sent tracking results to API | {time_taken:.2f}")
            
            return data
        except httpx.RequestError as e:
            logger.error(f"Request error occurred while sending results: {str(e)}")
            raise
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred while sending results: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while sending results: {str(e)}")
            raise

async def main():
    """Main function to demonstrate usage"""
    try:
        # Fetch tracking numbers from API
        # api_response = await fetch_tracking_numbers()
        api_response : APIResponse = APIResponse(
                flag=1,
                code=200,
                message="Success",
                data=["1354724014"]
            )
        if api_response.code == 200 and api_response.flag == 1:
            logger.info(f"Received {len(api_response.data)} tracking numbers to process")
            
            # Initialize scraper and get tracking info for all numbers
            scraper = TrackingInfoScraper()
            results = await scraper.get_multiple_tracking_info(api_response.data)
            # Send results to API
            api_response = await update_tracking_details(results)
            logger.info(f"API Response: {api_response}")          
        else:
            logger.error(f"API returned error: {api_response.message}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())