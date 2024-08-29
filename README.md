# Usage Analysis

This README outlines the process of determining usage values for customer accounts based on data retrieved from Snowflake metrics at https://qsidev.cloud.looker.com/explore/snowflake_wh_monitoring/query_history?qid=R0ccnk18WxGMpV6sKbMOBl&origin_space=230&toggle=fil,vis for the months of August and July.

First, update .env with your GCP project location and region. You should be authenticated to gcloud in your environment with [Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc).

The overall process is broken down into multiple scripts, such that output can be reviewed after each step:

## 1. Store data from Looker Snowflake dashboards

Snowflake data is stored in the `customer-input-data/` dir:

- june.csv
- july.csv
- august.csv

These files should not be altered or deleted.

## 2. Consolidate data

Consolidate the monthly data by running:

```
python consolidate_data.py
```

This script creates a single csv with the following columns:

- Customer
- Total Queries (July)
- Total Queries (Aug) Adjusted
- Total Queries (Aug)
- Avg MB Scanned (Aug)
- Avg Execution Secs (Aug)
- Max Execution Secs (Aug)
- Query Growth %
- Growth Category

Total Queries (Aug) Adjusted is calculated by increasing Total Queries (Aug) proportionally to reach the end of the month (the data was captured on 8/26).

Query Growth % is calculated with the following formula:

```
Total Queries (Aug) Adjusted - Total Queries (July) / Total Queries (July)
```

Growth Category is assigned by the following conditions, where q = Query Growth % and t = Total Queries (Aug):

```
# New Customer
q is NaN

# Stable Growth
(q >= -15 & q <= 15) | t <= 100

Negative Growth
q < -15

Low Growth
q < 25

Medium Growth
q >= 25 & q < 50

High Growth
q >= 50
```

The output is saved to the file `output/customer-usage-stats.csv`.

## 3. Assign Usage Categories

Next, to assign a Usage Category to each row in the previously saved file, run:

```
python analyze_usage.py
```

This will connect to the LLM Gemini Pro and ask it to assign one of the following categories:

- No Usage
- Very Low Usage
- Low Usage
- Medium Usage
- High Usage
- Very High Usage
- Extremely High Usage

The output is saved in `output/customer-usage-stats-with-categories.csv`.

## 4. Append peak times

To get the top 5 peak times (hours of the day with the most queries) for each customer, run:

```
python get_peak_times.py
```

This will use `looker-api-keys.csv` to authenticate to each production instance, retrieve the relevant data from System Activity, and store the output in `output/peak-times.csv`.

## 5. Map customers to instances

To map customers to instance based on the data retrieved and processed by the previous steps, run:

```
python assign_customers_to_instances.py
```

This final step reads in `customer-usage-stats-with-categories.csv` and `peak-times.csv` from the `output/` folder and
