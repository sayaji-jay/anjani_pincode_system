import pandas as pd

def get_unique_pincodes(file_path):
    try:
        # Read CSV
        df = pd.read_csv(file_path, encoding='utf-8')

        # Normalize column names to handle case mismatches
        df.columns = [col.strip().upper() for col in df.columns]

        if "PINCODE" not in df.columns:
            print("No 'PINCODE' column found in the CSV.")
            return []

        # Drop rows where PINCODE is NaN or empty
        df = df[df["PINCODE"].notna()]
        df = df[df["PINCODE"].astype(str).str.strip() != ""]

        # Total entries
        total_count = len(df)

        # Drop duplicates
        unique_pincodes = df["PINCODE"].drop_duplicates()

        # Count after removing duplicates
        unique_count = len(unique_pincodes)
        duplicate_count = total_count - unique_count

        print(f"Total PINCODE entries: {total_count}")
        print(f"Duplicate PINCODEs: {duplicate_count}")
        print(f"Unique PINCODEs: {unique_count}")

        return unique_pincodes.tolist()

    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return []
    except Exception as e:
        print(f"Error: {e}")
        return []

get_unique_pincodes("List of Pin Codes of Gujarat.csv")