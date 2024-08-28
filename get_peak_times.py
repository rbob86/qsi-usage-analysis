import csv
import json
import os
import looker_sdk
from looker_sdk import error
from looker_sdk.sdk.api40 import models


def set_env_vars(creds):
    os.environ["LOOKERSDK_BASE_URL"] = creds["looker_url"]
    os.environ["LOOKERSDK_CLIENT_ID"] = creds["client_id"]
    os.environ["LOOKERSDK_CLIENT_SECRET"] = creds["client_secret"]


def get_top_5_peak_times(sdk, customer):
    group_names = (f"{customer}_Viewer", f"{customer}_Writer")
    response = sdk.run_inline_query(
        result_format="json",
        body=models.WriteQuery(
            model="system__activity",
            view="history",
            fields=[
                "group.name",
                "history.created_hour_of_day",
                "history.query_run_count",
                "history.total_runtime",
                "history.average_runtime",
                "history.results_from_cache",
            ],
            filters={
                "group.name": f"{group_names[0]}, {group_names[1]}",
            },
            sorts=["history.total_runtime desc"],
            limit=5,
            total=False,
        ),
    )
    response = json.loads(response)

    return [{"customer": customer, **row} for row in response]


# to_query = ["https://qsi069.cloud.looker.com"]
input_file = "looker-api-keys.csv"
output_file = "output/peak-times.csv"
output_headers_written = False

# Clear output file
with open(output_file, "w"):
    pass

# Get peak times
instance_details = list(csv.DictReader(open(input_file)))

with open(output_file, "w", newline="") as csvfile:
    for instance in instance_details:
        instance_url = instance["looker_url"]

        # if instance_url not in to_query:
        #     continue

        print(f"Analyzing peak times for customers on instance {instance_url}:")

        # Authenticate to instance
        set_env_vars(instance)
        sdk = looker_sdk.init40()

        # Retrieve and save peak times for each customer
        customers = instance["customers"].split(",")

        for customer in customers:
            print(f"Retrieving peak times for customer {customer}...")
            peak_times = get_top_5_peak_times(sdk, customer)

            if len(peak_times) == 0:
                print("No queries found.\n")
                continue

            peak_times_custom_headers = [
                {
                    "Customer": item["customer"],
                    "Name": item["group.name"],
                    "Hour": item["history.created_hour_of_day"],
                    "Queries": item["history.query_run_count"],
                    "Total Runtime": item["history.total_runtime"],
                    "Average Runtime": item["history.average_runtime"],
                }
                for item in peak_times
            ]

            if not output_headers_written:
                fieldnames = peak_times_custom_headers[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                output_headers_written = True

            writer.writerows(peak_times_custom_headers)
            print(f"Saved {len(peak_times_custom_headers)} rows to {output_file}.\n")
        print("--------")
