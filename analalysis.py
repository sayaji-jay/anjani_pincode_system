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
        
        # State code mapping based on first two digits of pincode
        self.state_code_mapping = {
            '11': 'DL',
            '12': 'HR', '13': 'HR',
            '14': 'PB', '15': 'PB',
            '16': 'CH',
            '17': 'HP',
            '18': 'JK', '19': 'JK',
            '20': 'UP', '21': 'UP', '22': 'UP', '23': 'UP', '24': 'UP', 
            '25': 'UP', '26': 'UP', '27': 'UP', '28': 'UP',
            '30': 'RJ', '31': 'RJ', '32': 'RJ', '33': 'RJ', '34': 'RJ', '35': 'RJ',
            '36': 'GJ', '37': 'GJ', '38': 'GJ', '39': 'GJ',
            '40': 'MH', '41': 'MH', '42': 'MH', '43': 'MH', '44': 'MH',
            '45': 'MP', '46': 'MP', '47': 'MP', '48': 'MP',
            '49': 'CG',
            '50': 'TG',
            '51': 'AP', '52': 'AP', '53': 'AP',
            '56': 'KA', '57': 'KA', '58': 'KA', '59': 'KA',
            '60': 'TN', '61': 'TN', '62': 'TN', '63': 'TN', '64': 'TN', '65': 'TN', '66': 'TN',
            '67': 'KL', '68': 'KL', '69': 'KL',
            '70': 'WB', '71': 'WB', '72': 'WB', '73': 'WB', '74': 'WB',
            '75': 'OD', '76': 'OD', '77': 'OD',
            '78': 'AS',
            '790': 'AR', '791': 'AR', '792': 'AR',
            '793': 'ML', '794': 'ML',
            '795': 'MN',
            '796': 'MZ',
            '797': 'NL', '798': 'NL',
            '799': 'TR',
            '80': 'BR', '81': 'BR', '82': 'BR', '83': 'BR', '84': 'BR', '85': 'BR',
            '90': 'APS', '91': 'APS', '92': 'APS', '93': 'APS', '94': 'APS',
            '95': 'APS', '96': 'APS', '97': 'APS', '98': 'APS', '99': 'APS'
        }
        
        # Full state names mapping
        self.state_name_mapping = {
            'DL': 'Delhi',
            'HR': 'Haryana',
            'PB': 'Punjab',
            'CH': 'Chandigarh',
            'HP': 'Himachal Pradesh',
            'JK': 'Jammu and Kashmir',
            'UP': 'Uttar Pradesh',
            'RJ': 'Rajasthan',
            'GJ': 'Gujarat',
            'MH': 'Maharashtra',
            'MP': 'Madhya Pradesh',
            'CG': 'Chhattisgarh',
            'TG': 'Telangana',
            'AP': 'Andhra Pradesh',
            'KA': 'Karnataka',
            'TN': 'Tamil Nadu',
            'KL': 'Kerala',
            'WB': 'West Bengal',
            'OD': 'Odisha',
            'AS': 'Assam',
            'AR': 'Arunachal Pradesh',
            'ML': 'Meghalaya',
            'MN': 'Manipur',
            'MZ': 'Mizoram',
            'NL': 'Nagaland',
            'TR': 'Tripura',
            'BR': 'Bihar',
            'APS': 'Army Post Service'
        }
        
        # Special pincode mappings
        self.special_pincodes = {
            # '396': ('DD', 'Dadra and Nagar Haveli and Daman and Diu'),
            # '403': ('GA', 'Goa'),
            # '605': ('PY', 'Puducherry'),
            # '682': ('LD', 'Lakshadweep'),
            # '737': ('SK', 'Sikkim'),
            # '744': ('AN', 'Andaman and Nicobar Islands')
        }
        
        # Connect to MongoDB
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.db_name]
        
        # Get collections
        self.pincode_collection = self.db[self.pincode_collection_name]
        self.success_collection = self.db[self.success_collection_name]
        self.failed_collection = self.db[self.failed_collection_name]
    
    def get_state_from_pincode(self, pincode):
        """Get state code and name based on pincode"""
        try:
            pincode_str = str(pincode)
            
            # Check for special pincodes first
            if pincode_str[:3] in self.special_pincodes:
                return self.special_pincodes[pincode_str[:3]]
            
            # Check for three-digit prefixes (like 790-799)
            if pincode_str[:3] in self.state_code_mapping:
                state_code = self.state_code_mapping[pincode_str[:3]]
                return state_code, self.state_name_mapping.get(state_code, 'Unknown')
            
            # Check for two-digit prefixes
            first_two = pincode_str[:2]
            if first_two in self.state_code_mapping:
                state_code = self.state_code_mapping[first_two]
                return state_code, self.state_name_mapping.get(state_code, 'Unknown')
            
            return 'Unknown', 'Unknown'
        except:
            return 'Unknown', 'Unknown'
    
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
            # Add state code and name columns based on pincode
            df_all[['State Code', 'State']] = pd.DataFrame(df_all['Pin Code'].apply(self.get_state_from_pincode).tolist(), index=df_all.index)
        else:
            df_all = pd.DataFrame()
        
        # Convert success data to DataFrame
        if success_data:
            df_success = pd.DataFrame(success_data)
            if '_id' in df_success.columns:
                df_success['_id'] = df_success['_id'].astype(str)
            # Add state code and name columns based on pincode
            df_success[['State Code', 'State']] = pd.DataFrame(df_success['Pin Code'].apply(self.get_state_from_pincode).tolist(), index=df_success.index)
        else:
            df_success = pd.DataFrame()
        
        # Convert failed data to DataFrame
        if failed_data:
            df_failed = pd.DataFrame(failed_data)
            if '_id' in df_failed.columns:
                df_failed['_id'] = df_failed['_id'].astype(str)
            # Add state code and name columns based on pincode
            df_failed[['State Code', 'State']] = pd.DataFrame(df_failed['Pin Code'].apply(self.get_state_from_pincode).tolist(), index=df_failed.index)
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

    def create_excel_file(self, df_all, df_success, df_failed, df_delivery_zone,df_not_delivery_zone_only_gujrat):
        """Create Excel file with multiple sheets"""
        # Generate filename with timestamp
        # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"anjani_courier_data.xlsx"
        
        print(f"Creating Excel file: {filename}")
        
        # Create Excel writer object
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Prepare dataframes first
            df_all_copy = df_all.copy()
            df_all_copy = df_all_copy.sort_values(by='Pin Code')
            
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
                df_all_sorted.to_excel(writer, sheet_name='All Pincode Details', index=False)
                
                # Format the worksheet
                worksheet = writer.sheets['All Pincode Details']
                self.format_worksheet(worksheet, df_all_sorted)
                print(f"All Pincode Details sheet created with {len(df_all_sorted)} records")
            
            # Sheet 3: Process Success
            # if not df_success.empty:
            #     df_success_clean = df_success.copy()
            #     df_success_clean = df_success_clean.sort_values(by='Pin Code')
            #     df_success_clean = df_success_clean.drop(['_id','Checked At','Status'], axis=1, errors='ignore')
            #     df_success_clean.to_excel(writer, sheet_name='Found Pincode', index=False)
                
            #     # Format the worksheet
            #     worksheet = writer.sheets['Found Pincode']
            #     self.format_worksheet(worksheet, df_success_clean)
            #     print(f"Success sheet created with {len(df_success_clean)} records")
            
            # Sheet 4: Process Failed
            # if not df_failed.empty:
            #     df_failed_clean = df_failed.copy()
            #     df_failed_clean = df_failed_clean.sort_values(by='Pin Code')
            #     df_failed_clean = df_failed_clean.drop(['_id','Checked At','Status','Reason'], axis=1, errors='ignore')
            #     df_failed_clean.to_excel(writer, sheet_name='Not Found Pincode', index=False)
                
            #     # Format the worksheet
            #     worksheet = writer.sheets['Not Found Pincode']
            #     self.format_worksheet(worksheet, df_failed_clean)
            #     print(f"Failed sheet created with {len(df_failed_clean)} records")
            
            # Sheet 5: Delivery Zone Summary
            if not df_delivery_zone.empty:
                df_delivery_zone = df_delivery_zone.sort_values(by='Pin Code')
                df_zone_summary = df_delivery_zone[["Pin Code", "State Code", "State"]]
                df_zone_summary.to_excel(writer, sheet_name='Possible Delivery Zone', index=False)
                
                # Format the worksheet
                worksheet = writer.sheets['Possible Delivery Zone']
                self.format_worksheet(worksheet, df_zone_summary)
                print(f"Delivery Zone Summary sheet created with {len(df_zone_summary)} records")

            # Sheet 6: Gujarat Not Delivery Zone
            if not df_not_delivery_zone_only_gujrat.empty:
                df_not_delivery_zone_only_gujrat = df_not_delivery_zone_only_gujrat.sort_values(by='Pin Code')
                df_zone_summary = df_not_delivery_zone_only_gujrat[["Pin Code"]]
                df_zone_summary.to_excel(writer, sheet_name='Gujarat Not Delivery Zone', index=False)
                # Format the worksheet
                worksheet = writer.sheets['Gujarat Not Delivery Zone']
                self.format_worksheet(worksheet, df_zone_summary)
                print(f"Gujarat Not Delivery Zone sheet created with {len(df_zone_summary)} records")
        
        print(f"Excel file '{filename}' created successfully!")
        return filename
    

    def get_delivery_zone_data(self,df,state_code=None):
        """Get delivery zone data from MongoDB"""
        if state_code:
            df = df[(df['State Code'] == state_code)]


        # Convert Pin Code to string type to avoid comparison issues
        df['Pin Code'] = df['Pin Code'].astype(str)
        
        # Create a copy of the DataFrame with required columns
        df_with_states = df[['Pin Code', 'Zone Type', 'State Code', 'State']].copy()
        
        # Group by Pin Code and get value counts for Zone Type
        zone_counts = df.groupby('Pin Code')['Zone Type'].value_counts().unstack(fill_value=0)
        zone_counts['Total'] = zone_counts.sum(axis=1)
        
        if 'Delivery Zone' in zone_counts.columns:
            # Convert to numeric values and calculate percentage
            delivery_zone = pd.to_numeric(zone_counts['Delivery Zone'], errors='coerce')
            total = pd.to_numeric(zone_counts['Total'], errors='coerce')
            pr = (delivery_zone / total) * 100
            # Filter where percentage is >= 80
            filtered_pincodes = zone_counts[pr >= 80].index
            not_delivery_zone = zone_counts[pr < 80].index
            
            # Filter the original DataFrame to get state information
            df_delivery_zone = df_with_states[df_with_states['Pin Code'].isin(filtered_pincodes)]
            df_not_delivery_zone = df_with_states[df_with_states['Pin Code'].isin(not_delivery_zone)]
            # Drop duplicates to get one row per pincode
            df_delivery_zone = df_delivery_zone.drop_duplicates(subset='Pin Code')
            df_not_delivery_zone = df_not_delivery_zone.drop_duplicates(subset='Pin Code')
            
        else:
            df_delivery_zone = pd.DataFrame(columns=['Pin Code', 'State Code', 'State'])
            df_not_delivery_zone = pd.DataFrame(columns=['Pin Code', 'State Code', 'State'])
            
        return df_delivery_zone,df_not_delivery_zone
    
   
    def export_to_excel(self):
        """Main method to export MongoDB data to Excel"""
        try:
            # Fetch data from MongoDB
            all_data, success_data, failed_data = self.fetch_all_data()
            
            # Convert to DataFrames
            df_all, df_success, df_failed = self.convert_to_dataframes(all_data, success_data, failed_data)
            df_delivery_zone,df_not_delivery_zone = self.get_delivery_zone_data(df_all)
            df_delivery_zone_only_gujrat,df_not_delivery_zone_only_gujrat = self.get_delivery_zone_data(df_all, 'GJ')

            # Create Excel file
            filename = self.create_excel_file(df_all, df_success, df_failed,df_delivery_zone,df_not_delivery_zone_only_gujrat)
            
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
