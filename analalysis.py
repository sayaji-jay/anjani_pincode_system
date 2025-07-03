import pandas as pd
import pymongo
from datetime import datetime
import os
from openpyxl.styles import PatternFill, Font
from openpyxl.utils.dataframe import dataframe_to_rows

class MongoToExcelExporter:
    def __init__(self):
        # MongoDB configuration (same as app.py)
        self.mongo_uri = "mongodb+srv://justj:justjay19@cluster0.fsgzjrl.mongodb.net/"
        self.db_name = "anjani"
        
        # Collection names (same as app.py)
        self.pincode_collection_name = "pincodes"          # Stores detailed rows
        self.success_collection_name = "pincode_successes"  # Stores successful pincode checks
        self.failed_collection_name = "pincode_failures"    # Stores failed pincode checks
        
        # Connect to MongoDB
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]
        
        # Get collections
        self.pincode_collection = self.db[self.pincode_collection_name]
        self.success_collection = self.db[self.success_collection_name]
        self.failed_collection = self.db[self.failed_collection_name]
    
    def fetch_all_data(self):
        """Fetch data from all MongoDB collections"""
        print("Fetching data from MongoDB...")
        
        # Fetch all data from pincodes collection
        all_data = list(self.pincode_collection.find())
        print(f"Total pincode records: {len(all_data)}")
        
        # Fetch success data
        success_data = list(self.success_collection.find())
        print(f"Success records: {len(success_data)}")
        
        # Fetch failed data
        failed_data = list(self.failed_collection.find())
        print(f"Failed records: {len(failed_data)}")
        
        return all_data, success_data, failed_data
    
    def convert_to_dataframes(self, all_data, success_data, failed_data):
        """Convert MongoDB data to pandas DataFrames"""
        print("Converting data to DataFrames...")
        
        # Convert all data to DataFrame
        if all_data:
            df_all = pd.DataFrame(all_data)
            # Convert ObjectId to string for Excel compatibility
            if '_id' in df_all.columns:
                df_all['_id'] = df_all['_id'].astype(str)
        else:
            df_all = pd.DataFrame()
        
        # Convert success data to DataFrame
        if success_data:
            df_success = pd.DataFrame(success_data)
            if '_id' in df_success.columns:
                df_success['_id'] = df_success['_id'].astype(str)
        else:
            df_success = pd.DataFrame()
        
        # Convert failed data to DataFrame
        if failed_data:
            df_failed = pd.DataFrame(failed_data)
            if '_id' in df_failed.columns:
                df_failed['_id'] = df_failed['_id'].astype(str)
        else:
            df_failed = pd.DataFrame()
        
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
        # Generate filename with timestamp
        # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"anjani_courier_data.xlsx"
        
        print(f"Creating Excel file: {filename}")
        
        # Create Excel writer object
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Prepare dataframes first
            df_all_copy = df_all.copy()
            
            # Sheet 1: Delivery Pincode Details
            if not df_all_copy.empty:
                df_delivery_only = df_all_copy[df_all_copy['Zone Type'] == 'Delivery Zone'].copy()
                if not df_delivery_only.empty:
                    df_delivery_only = df_delivery_only.drop(['_id','Inserted At'], axis=1, errors='ignore')
                    df_delivery_only.to_excel(writer, sheet_name='Delivery Pincode Details', index=False)
                    
                    # Format the worksheet
                    worksheet = writer.sheets['Delivery Pincode Details']
                    self.format_worksheet(worksheet, df_delivery_only)
                    print(f"Delivery Pincode Details sheet created with {len(df_delivery_only)} records")
            
            # Sheet 2: All Pincode Details (sorted)
            if not df_all_copy.empty:
                df_all_sorted = df_all_copy.copy()
                df_all_sorted = df_all_sorted.drop(['_id','Inserted At'], axis=1, errors='ignore')
                df_all_sorted = df_all_sorted.sort_values(by='Pin Code')    
                df_all_sorted.to_excel(writer, sheet_name='All Pincode Details', index=False)
                
                # Format the worksheet
                worksheet = writer.sheets['All Pincode Details']
                self.format_worksheet(worksheet, df_all_sorted)
                print(f"All Pincode Details sheet created with {len(df_all_sorted)} records")
            
            # Sheet 3: Process Success
            if not df_success.empty:
                df_success_clean = df_success.copy()
                df_success_clean = df_success_clean.drop(['_id','Checked At','Status'], axis=1, errors='ignore')
                df_success_clean.to_excel(writer, sheet_name='Found Pincode', index=False)
                
                # Format the worksheet
                worksheet = writer.sheets['Found Pincode']
                self.format_worksheet(worksheet, df_success_clean)
                print(f"Success sheet created with {len(df_success_clean)} records")
            
            # Sheet 4: Process Failed
            if not df_failed.empty:
                df_failed_clean = df_failed.copy()
                df_failed_clean = df_failed_clean.drop(['_id','Checked At','Status','Reason'], axis=1, errors='ignore')
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
        """Get delivery zone data from MongoDB"""
        df_grouped = df.groupby('Pin Code')['Zone Type'].value_counts().unstack(fill_value=0)
        df_grouped['Total'] = df_grouped.sum(axis=1)
        if 'Delivery Zone' in df_grouped.columns:
            pr = (df_grouped['Delivery Zone'] / df_grouped['Total']) * 100
            df_grouped = df_grouped[pr >=80]
        df_grouped = df_grouped.reset_index()
        return df_grouped
    
    def export_to_excel(self):
        """Main method to export MongoDB data to Excel"""
        try:
            # Fetch data from MongoDB
            all_data, success_data, failed_data = self.fetch_all_data()
            
            # Convert to DataFrames
            df_all, df_success, df_failed = self.convert_to_dataframes(all_data, success_data, failed_data)
            df_delivery_zone = self.get_delivery_zone_data(df_all)

            # Create Excel file
            filename = self.create_excel_file(df_all, df_success, df_failed,df_delivery_zone)
            
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
        finally:
            # Close MongoDB connection
            self.client.close()
            print("MongoDB connection closed.")

def main():
    """Main function to run the export"""
    print("Starting MongoDB to Excel export...")
    print("="*50)
    
    # Create exporter instance
    exporter = MongoToExcelExporter()
    
    # Export data to Excel
    result = exporter.export_to_excel()
    
    if result:
        print(f"\nExport completed successfully!")
        print(f"Your Excel file '{result}' is ready!")
    else:
        print("\nExport failed. Please check the error messages above.")

if __name__ == "__main__":
    main()
