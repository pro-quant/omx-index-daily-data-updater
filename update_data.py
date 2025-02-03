import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

# Define the file paths
existing_file = "omx30.csv"
updated_file = "omx30_updated.csv"

# Check if the existing file is present
if os.path.exists(existing_file):
    # Load the existing dataset
    existing_data = pd.read_csv(existing_file, sep=";", names=[
                                "ID", "Name", "Date", "High", "Close", "Low"], skiprows=1)

    # Convert 'Date' column to datetime format
    existing_data["Date"] = pd.to_datetime(
        existing_data["Date"], errors="coerce")

    # Get the last recorded date
    last_date = existing_data["Date"].max()

    # Print last recorded date before proceeding
    print(f"Last recorded date in file: {last_date}")

    # Define the start date for new data (one day after the last recorded date)
    start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')

    # Define end date as today
    end_date = datetime.today().strftime('%Y-%m-%d')

    print(f"Downloading new data from {start_date} to {end_date}...")
else:
    raise FileNotFoundError(
        f"File '{existing_file}' not found. Please make sure it exists.")

# Define the Yahoo Finance ticker for OMXS30
ticker = "^OMX"

# Download new data from Yahoo Finance
new_data = yf.download(ticker, start=start_date, end=end_date)

# Print the raw data columns
print("Raw downloaded columns:", new_data.columns.tolist())

# Check if data was downloaded
if new_data.empty:
    print("No new data was downloaded. Check the ticker and date range.")
    exit()

# Reset index to get 'Date' as a column
new_data.reset_index(inplace=True)

# Print column names after resetting index
print("Columns after reset_index:", new_data.columns.tolist())

# Handle MultiIndex issue if necessary
if isinstance(new_data.columns, pd.MultiIndex):
    new_data.columns = ['_'.join(col).strip()
                        for col in new_data.columns.values]

# Print column names after flattening (if applicable)
print("Columns after flattening:", new_data.columns.tolist())

# Rename columns to match the existing dataset
column_mapping = {
    'Date_': 'Date',   # Fix Date column
    'Close_^OMX': 'Close',
    'High_^OMX': 'High',
    'Low_^OMX': 'Low'
}
new_data.rename(columns=column_mapping, inplace=True)

# Print final columns before merging
print("Final renamed columns:", new_data.columns.tolist())

# # Ensure 'Date' exists after processing
# if "Date" not in new_data.columns:
#     raise KeyError(f"Error: 'Date' column is missing. Available columns: {
#                    new_data.columns.tolist()}")


# Select relevant columns
new_data = new_data[["Date", "High", "Close", "Low"]]
new_data["ID"] = 15055  # Assign the same ID as in your existing dataset
new_data["Name"] = "OMXS30"  # Assign the same index name

# Reorder columns to match the existing dataset format
new_data = new_data[["ID", "Name", "Date", "High", "Close", "Low"]]

# Convert 'Date' column to datetime format
new_data["Date"] = pd.to_datetime(new_data["Date"], errors="coerce")

# Remove rows with invalid dates
new_data = new_data.dropna(subset=["Date"])

# Print last date in new_data to confirm it's correct
print(f"Last recorded date in new data: {new_data['Date'].max()}")

# Merge old and new data, ensuring no duplicates
merged_data = pd.concat([existing_data, new_data]).drop_duplicates(
    subset=["Date"]).reset_index(drop=True)

# Print final DataFrame info for debugging
print("Final merged dataset preview:")
print(merged_data.tail())

# Save the merged dataset to a new file
merged_data.to_csv(updated_file, sep=";", index=False)

print(f"Updated dataset saved as {updated_file}")
