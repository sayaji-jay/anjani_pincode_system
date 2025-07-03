import pandas as pd
import pymongo
from datetime import datetime
import os

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
    
    def create_excel_file(self, df_all, df_success, df_failed,df_delivery_zone):
        """Create Excel file with multiple sheets"""
        # Generate filename with timestamp
        # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"anjani_courier_data.xlsx"
        
        print(f"Creating Excel file: {filename}")
        
        # Create Excel writer object
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:

            # Write all data to first sheet
            if not df_all.empty:
                df_all = df_all[df_all['zone_type'] == 'Delivery Zone']
                df_all.drop(['_id','inserted_at'], axis=1, inplace=True)    
                df_all.to_excel(writer, sheet_name='Delivery Pincode Details', index=False)
                print(f"Delivery Pincode Details sheet created with {len(df_all)} records")

            
            # Write all data to first sheet
            if not df_all.empty:
                df_all = df_all.sort_values(by='pc_code')  
                df_all.drop(['_id','inserted_at'], axis=1, inplace=True)    
                df_all.to_excel(writer, sheet_name='Row Pincode Details', index=False)
                print(f"All Data sheet created with {len(df_all)} records")

            
            # Write success data to second sheet
            if not df_success.empty:
                df_success.drop(['_id','checked_at'], axis=1, inplace=True)    
                df_success.to_excel(writer, sheet_name='Process Success', index=False)
                print(f"Success sheet created with {len(df_success)} records")

            
            # Write failed data to third sheet
            if not df_failed.empty:
                df_failed.drop(['_id','checked_at'], axis=1, inplace=True)
                df_failed.to_excel(writer, sheet_name='Process Failed', index=False)
                print(f"Failed sheet created with {len(df_failed)} records")



            # Write Delivery Zone data to third sheet
            if not df_delivery_zone.empty:
                df_delivery_zone.drop(['_id','checked_at','Total'], axis=1, inplace=True)    
                df_delivery_zone.to_excel(writer, sheet_name='Delivery Zone', index=False)
                print(f"Delivery Zone sheet created with {len(df_delivery_zone)} records")

        
        print(f"Excel file '{filename}' created successfully!")
        return filename
    

    def get_delivery_zone_data(self,df):
        """Get delivery zone data from MongoDB"""
        df_grouped = df.groupby('pc_code')['zone_type'].value_counts().unstack(fill_value=0)
        df_grouped['Total'] = df_grouped.sum(axis=1)
        df_grouped['Delivery Zone'] = (df_grouped['Delivery Zone'] / df_grouped['Total']) * 100
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
