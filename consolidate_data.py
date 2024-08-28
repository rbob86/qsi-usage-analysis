import numpy as np
import os
import pandas as pd

# Read in the CSV files
data_dir = "customer-input-data"
july_data = pd.read_csv(os.path.join(data_dir, "july.csv"))
august_data = pd.read_csv(os.path.join(data_dir, "august.csv"))

# Merge the data on Customer
merged_data = pd.merge(
    july_data[["Customer", "Total Queries"]],
    august_data,
    on="Customer",
    how="outer",
    suffixes=(" (July)", " (Aug)"),
)

# Rename columns for clarity
merged_data.rename(
    columns={
        "Total Queries": "Total Queries (Aug)",
        "Avg MB Scanned": "Avg MB Scanned (Aug)",
        "Avg Execution Secs": "Avg Execution Secs (Aug)",
        "Max Execution Secs": "Max Execution Secs (Aug)",
    },
    inplace=True,
)

# Replace missing values ("-") with NaN for calculation purposes
merged_data["Total Queries (July)"].replace("-", np.nan, inplace=True)
merged_data["Total Queries (Aug)"].replace("-", np.nan, inplace=True)

# Convert columns to numeric types after replacement
merged_data["Total Queries (July)"] = pd.to_numeric(
    merged_data["Total Queries (July)"], errors="coerce"
)
merged_data["Total Queries (Aug)"] = pd.to_numeric(
    merged_data["Total Queries (Aug)"], errors="coerce"
)

# Adjust August queries to account for the month not being complete
days_in_august = 31
days_observed = 26  # Replace this with the number of days you've observed in August
merged_data["Total Queries (Aug) Adjusted"] = np.where(
    merged_data["Total Queries (Aug)"].notna(),
    (merged_data["Total Queries (Aug)"] / days_observed) * days_in_august,
    np.nan,
)

# Calculate Query Growth % based on the adjusted August data
merged_data["Query Growth %"] = np.where(
    merged_data["Total Queries (July)"].notna(),
    (
        (
            merged_data["Total Queries (Aug) Adjusted"]
            - merged_data["Total Queries (July)"]
        )
        / merged_data["Total Queries (July)"]
    )
    * 100,
    np.nan,
)

# Determine growth category
merged_data["Growth Category"] = np.where(
    # New Customer (NaN values)
    merged_data["Query Growth %"].isna(),
    "New Customer",
    np.where(
        # Stable Growth
        ((merged_data["Query Growth %"] >= -15) & (merged_data["Query Growth %"] <= 15))
        | (merged_data["Total Queries (Aug) Adjusted"] <= 100),
        "Stable",
        np.where(
            # Negative Growth
            merged_data["Query Growth %"] < -15,
            "Negative Growth",
            np.where(
                # Low Growth
                (merged_data["Query Growth %"] < 25),
                "Low Growth",
                np.where(
                    # Medium Growth
                    (merged_data["Query Growth %"] >= 25)
                    & (merged_data["Query Growth %"] < 50),
                    "Medium Growth",
                    # High Growth
                    "High Growth",
                ),
            ),
        ),
    ),
)

# Fill NaN values with "-" for presentation purposes
merged_data.fillna("-", inplace=True)

# Select final columns
final_columns = [
    "Customer",
    "Total Queries (July)",
    "Total Queries (Aug) Adjusted",
    "Total Queries (Aug)",
    "Avg MB Scanned (Aug)",
    "Avg Execution Secs (Aug)",
    "Max Execution Secs (Aug)",
    "Query Growth %",
    "Growth Category",
]

# Display or save the categorized data
final_data = merged_data[final_columns]

# Handling display of NaN as "-" (for example, when saving to CSV)
final_data.replace(np.nan, "-", inplace=True)

print(final_data)
final_data.to_csv("output/customer-usage-stats.csv", index=False)
