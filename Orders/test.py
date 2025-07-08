import httpx
from datetime import datetime
import sys
import asyncio
from app import APIResponse, TrackingInfoScraper,logger


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
            
            # Print results for each tracking number
            for tracking_info in results:
                print(tracking_info)
        else:
            logger.error(f"API returned error: {api_response.message}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())