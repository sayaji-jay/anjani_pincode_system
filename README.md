# Anjani Courier Data Scraping Project

This project scrapes pincode data from Anjani Courier website and generates detailed Excel reports with the results.

## For Developers (Building the Executable)

### 1. Python Installation
First, make sure you have Python installed (Python 3.8 or higher recommended):
- Download Python from [python.org](https://www.python.org/downloads/)
- During installation, make sure to check "Add Python to PATH"

### 2. Install Required Packages
```bash
# Install all required packages
pip install pandas
pip install openpyxl
pip install requests
pip install beautifulsoup4
pip install PyInstaller
```

### 3. Create Executable
```bash
# Navigate to project directory
cd Scraping

# Create executable with PyInstaller
python -m PyInstaller --onefile .\main_scraper.py --add-data ".\pincodes.csv;."
```

The executable will be created in the `dist` folder.

### 4. Prepare Distribution Package
After creating the executable, create the following folder structure:
```
Anjani Courier Scraper/
├── main_scraper.exe (from dist folder)
├── pincodes.csv
└── store/
    └── temp_data/
```

## For Users (Running the Application)

### 1. Setup
- Extract all files from the zip to a folder on your computer
- Make sure you have these folders:
  - `store`
  - `store/temp_data`

### 2. Prepare Input File
- Open `pincodes.csv` in Excel or Notepad
- Add your pincodes (one pincode per line)
- Save and close the file

### 3. Run the Application
1. Double-click `main_scraper.exe`
2. The application will show a progress window:
   - First shows "Loading and validating input data"
   - Then shows progress bar for pincode processing
   - Finally shows "Generating Excel report"
3. When complete, it will show where the files are saved

### 4. Output Files
The application creates:
- Excel file with all pincode data (in `store` folder)
- Log file with processing details (in `store` folder)
- Temporary JSON files (in `store/temp_data` folder)

## Important Notes
- Do not close the progress window while processing
- The application takes breaks every 20 pincodes to avoid server overload
- Keep the Excel file closed while the application is running
- Make sure you have internet connection

## Troubleshooting
If you encounter issues:
1. Check the log file in the `store` folder
2. Make sure your pincodes.csv is properly formatted
3. Verify all required folders exist
4. Check your internet connection
5. Try running the application again

## Process Flow
1. Application reads pincodes from CSV file
2. Processes each pincode with Anjani Courier website
3. Stores results in temporary JSON files
4. Generates final Excel report with all data
5. Shows success message with file locations 