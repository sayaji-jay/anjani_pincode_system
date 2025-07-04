# Anjani Courier Data Scraping Project

This project consists of two main components:
1. `app.py` - Scrapes data from Anjani Courier website
2. `analalysis.py` - Generates Excel reports from the scraped data

## Setup Instructions

### 1. Python Installation
First, make sure you have Python installed (Python 3.8 or higher recommended):
- Download Python from [python.org](https://www.python.org/downloads/)
- During installation, make sure to check "Add Python to PATH"

### 2. Project Setup
```bash
# Clone or download the project
# Navigate to project directory
cd Scraping

# Create and activate virtual environment (recommended)
python -m venv venv
# For Windows:
venv\Scripts\activate
# For Linux/Mac:
source venv/bin/activate
```

### 3. Install Required Packages
```bash
# Install all required packages
pip install pandas
pip install pymongo
pip install openpyxl
pip install requests
pip install beautifulsoup4
```

## Project Components

### 1. Data Scraping (`app.py`)
This script scrapes pincode data from Anjani Courier website and stores it in MongoDB.

**How to Run:**
```bash
python app.py
```

The script will:
- Scrape pincode data
- Store results in MongoDB collections:
  - `pincodes`: Detailed pincode information
  - `pincode_successes`: Successfully processed pincodes
  - `pincode_failures`: Failed pincode checks

### 2. Data Analysis (`analalysis.py`)
This script generates Excel reports from the MongoDB data.

**How to Run:**
```bash
python analalysis.py
```

The script will create `anjani_courier_data.xlsx` with multiple sheets:
- Delivery Pincode Details
- All Pincode Details
- Found Pincode
- Not Found Pincode
- Possible Delivery Zone

## Output Files
- `anjani_courier_data.xlsx`: Contains all analyzed data in multiple sheets
- `pincodes.csv`: Contains raw pincode data

## MongoDB Configuration
The project uses MongoDB Atlas. Make sure to:
1. Have a MongoDB Atlas account
2. Update the MongoDB connection string in both scripts if needed:
   ```python
   mongo_uri = "mongodb+srv://your_username:your_password@your_cluster.mongodb.net/"
   ```

## Troubleshooting
1. If you get MongoDB connection errors:
   - Check your internet connection
   - Verify MongoDB Atlas credentials
   - Ensure your IP is whitelisted in MongoDB Atlas

2. If Excel file generation fails:
   - Make sure no Excel file is open while running the script
   - Check if you have write permissions in the directory

## Process Flow
1. Run `app.py` first to scrape and store data
2. Once scraping is complete, run `analalysis.py` to generate reports
3. Check the generated Excel file for results

## Notes
- The scraping process may take some time depending on the number of pincodes
- Make sure you have stable internet connection while running the scripts
- Keep the Excel file closed while running the analysis script 