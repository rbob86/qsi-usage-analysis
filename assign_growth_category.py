import pandas as pd

# Step 1: Read in the CSV file
input_file_path = "path/to/your/input.csv"
df = pd.read_csv(input_file_path)

# Step 2: Remove a specific column (e.g., 'column_name')
df = df.drop(columns=["column_name"])

# Step 3: Save the updated DataFrame to a new CSV file
output_file_path = "path/to/your/output.csv"
df.to_csv(output_file_path, index=False)

print(f"Column 'column_name' removed and CSV saved to {output_file_path}")
