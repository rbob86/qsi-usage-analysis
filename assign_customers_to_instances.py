import pandas as pd
from natsort import natsorted

usage_scores = {
    "No Usage": 0,
    "Very Low Usage": 10,
    "Low Usage": 20,
    "Medium Usage": 30,
    "High Usage": 40,
    "Very High Usage": 50,
    "Extremely High Usage": 60,
}

growth_scores = {
    "Negative Growth": 0,
    "New Customer": 1,
    "Stable": 2,
    "Low Growth": 3,
    "Medium Growth": 4,
    "High Growth": 5,
}


def assign_usage_value(row):
    usage_score = usage_scores[row["Usage Category"]]
    growth_score = 0 if usage_score == 0 else growth_scores[row["Growth Category"]]

    return usage_score + growth_score


# Function to calculate weighted hour overlap
def calculate_weighted_hour_overlap(peak_times, customer_hours):
    weights = [1.0, 0.8, 0.6, 0.4, 0.2]
    overlap = 0
    for i, hour in enumerate(customer_hours):
        if i < len(weights):
            overlap += weights[i] * peak_times.count(hour)
    return overlap


# Function to update instance peak times after assigning a customer
def update_peak_times(peak_times, customer_hours):
    peak_times.extend(customer_hours)


# Function to calculate a weighted score
def calculate_weighted_score(
    instance, customer, customer_hours, usage_weight=0.7, overlap_weight=0.3
):
    overlap_score = calculate_weighted_hour_overlap(
        instance["Peak Times"], customer_hours
    )
    usage_score = instance["Total Usage Value"]
    return usage_weight * usage_score + overlap_weight * overlap_score


# Load datasets
usage_df = pd.read_csv("output/customer-usage-stats-with-categories.csv")
peak_times_df = pd.read_csv("output/peak-times.csv")

# Assign a usge value to each row
usage_df["Usage Value"] = usage_df.apply(assign_usage_value, axis=1)

# Combine both datasets into one
combined_data = pd.merge(
    usage_df,
    peak_times_df[["Customer", "Hour"]],
    on="Customer",
    how="left",
)

combined_data["Hour"] = combined_data["Hour"].astype("Int64")  # Handle NaN for Hour

# Group by Customer and aggregate peak usage hours (if they exist)
grouped_data = (
    combined_data.groupby("Customer")
    .agg(
        {
            "Usage Category": "first",
            "Usage Value": "first",
            "Hour": lambda x: (list(x.dropna()) if len(x.dropna()) > 0 else []),
        }
    )
    .reset_index()
)

# Initialize instances
num_instances = 30
instances = {
    i: {"Customers": [], "Total Usage Value": 0, "Peak Times": []}
    for i in range(1, num_instances + 1)
}

# Separate new customers, demo accounts, and remaining customers
no_usage_customers = grouped_data[
    (grouped_data["Usage Category"] == "No Usage")
    & (~grouped_data["Customer"].str.contains(r"^DEMO\d*$", regex=True))
]
demo_accounts = grouped_data[
    grouped_data["Customer"].str.contains(r"^DEMO\d*$", regex=True)
]
demo_accounts_sorted = demo_accounts.reindex(
    natsorted(demo_accounts.index, key=lambda x: demo_accounts.loc[x, "Customer"])
)
remaining_customers = grouped_data[
    (grouped_data["Usage Category"] != "No Usage")
    & (~grouped_data["Customer"].str.contains(r"^DEMO\d*$", regex=True))
]

assigned_instances = 0

# Evenly distribute `No Usage` customers
for _, customer in no_usage_customers.iterrows():
    instance_no = (assigned_instances % num_instances) + 1
    instances[instance_no]["Customers"].append(customer["Customer"])
    instances[instance_no]["Total Usage Value"] += customer["Usage Value"]
    assigned_instances += 1

# Evenly distribute DEMO accounts
for _, customer in demo_accounts_sorted.iterrows():
    instance_no = (assigned_instances % num_instances) + 1
    instances[instance_no]["Customers"].append(customer["Customer"])
    instances[instance_no]["Total Usage Value"] += customer["Usage Value"]
    if customer["Hour"]:  # Only update peak times if they exist
        update_peak_times(instances[instance_no]["Peak Times"], customer["Hour"])
    assigned_instances += 1

# Assign remaining customers using weighted scoring
for _, customer in remaining_customers.iterrows():
    best_instance = None
    best_score = float("inf")

    customer_hours = customer["Hour"]

    # Evaluate each instance for a weighted score
    for instance_no, instance in instances.items():
        # If the customer has no peak hours, consider only the usage value
        if not customer_hours:
            score = instance["Total Usage Value"]
        else:
            score = calculate_weighted_score(
                instance, customer, customer_hours, usage_weight=0.7, overlap_weight=0.3
            )
        if score < best_score:
            best_score = score
            best_instance = instance_no

    # Assign customer to the best instance
    instances[best_instance]["Customers"].append(customer["Customer"])
    instances[best_instance]["Total Usage Value"] += customer["Usage Value"]
    if customer_hours:  # Only update peak times if they exist
        update_peak_times(instances[best_instance]["Peak Times"], customer_hours)

# Convert instances to a more readable format
result = []
for instance_no, instance_data in instances.items():
    result.append(
        {
            "Instance No.": instance_no,
            "Customer Count": len(instance_data["Customers"]),
            "Total Usage Value": instance_data["Total Usage Value"],
            "Customers": ", ".join(
                instance_data["Customers"]
            ),  # Create a comma-separated list of customers
        }
    )

result_df = pd.DataFrame(result)

# Save the result to a CSV file
output_file = "output/proposed-instance-distribution.csv"
result_df.to_csv(output_file, index=False)

# Display the result
print(result_df)
