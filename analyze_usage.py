import ast
import os
import pandas as pd
import vertexai
from vertexai.preview import rag
from dotenv import load_dotenv
from vertexai.preview.generative_models import (
    GenerativeModel,
    GenerationConfig,
)

load_dotenv()

parameters = {
    "temperature": 0.2,
    "max_output_tokens": 8192,
    "top_p": 0.8,
    "top_k": 40,
}


def init_vertexai_model():
    project = os.getenv("PROJECT")
    location = os.getenv("REGION")
    model_name = "gemini-1.5-pro"
    # model_name = "gemini-1.5-flash"
    vertexai.init(project=project, location=location)

    return GenerativeModel(model_name)


def ask_question(prompt, csv_data):
    csv_data_str = csv_data.to_string(index=False)

    prompt = f"{prompt}\n\n{csv_data_str}"

    response = model.generate_content(
        contents=prompt,
        generation_config=GenerationConfig(
            temperature=parameters["temperature"],
            top_p=parameters["top_p"],
            top_k=parameters["top_k"],
            max_output_tokens=parameters["max_output_tokens"],
            candidate_count=1,
        ),
    )

    return response.text


if __name__ == "__main__":
    model = init_vertexai_model()

    # Upload CSV file for processing
    # upload_file("output/customer-usage-stats.csv")

    # Read CSV file into pandas
    csv_file_path = "output/customer-usage-stats.csv"
    df = pd.read_csv(csv_file_path)

    # Construct question
    question = """
        Task
        ----------
        Given the below csv data, add a "Usage Category" column based on the overall query usage of each customer, relative to the entire dataset. Treat any dashes ("-") in column values as 0s.  A Usage Category can be one of the following:

        - No Usage
        - Very Low Usage
        - Low Usage
        - Medium Usage
        - High Usage
        - Very High Usage
        - Extremely High Usage
        
        All categories should be used.  Customers with less than 10 in both "Total Queries (July)" and "Total Queries (Aug)" should be marked as "No Usage".  No other customers should be marked as No Usage. Of the remaining customers, those with less than 250 in "Total Queries (Aug)" should be marked as "Very Low Usage". The remaining customers should be categorized based on their data volume and average resource usage, relative to the overall dataset, with the following weighting:

        - **Total Queries (Aug)** should contribute 70% to the overall categorization.
        - **Avg Execution Secs (Aug)** should contribute 20% to the overall categorization.
        - **Avg MB Scanned (Aug)** should contribute 10% to the overall categorization.
        
        Provide the output as a list of tuples readable in Python where each tuple consists of exactly two elements: the customer and the usage category, formatted as:

        (Customer, Usage Category)

        Do not include any additional text aside from the list of tuples. Ensure that every category is represented in the final output.

        Examples
        ----------
        ('NYCNGSV', 'Very Low Usage')
        ('YTHHM', 'Very Low Usage')
        ('MIWLSLS', 'Low Usage')
        ('AKVOLAM', 'Low Usage')
        ('ALSWAMH', 'Low Usage')
        ('CTCJR', 'Low Usage')
        ('MOARCOA', 'Medium Usage')
        ('WAEXSYC', 'Medium Usage')
        ('AROBHAW', 'Medium Usage')
        ('VTARISS', 'High Usage')
        ('TNHEALTHCONNECT', 'High Usage')
        ('PAPYRMD', 'Very High Usage')
        ('MERIDIAN', 'Very High Usage')
        ('AKCOOK', 'Very High Usage')
        ('MLSTN', 'Extremely High Usage')

        CSV Data
        ----------
    """

    # pair 9/10s with low-usage DEMO accounts as much as possible

    # TRLDL, TrAINING, TRIAL are also internal accounts

    # Send question to LLM
    print("Asking Gemini to categorize customers based on usage...")
    response = ask_question(question, df)

    # Convert response to Python list of tuples
    response_list = ast.literal_eval(response)

    # Build new CSV with Usage Categories added
    print("Building CSV with usage categories added...")
    response_df = pd.DataFrame(
        response_list,
        columns=["Customer", "Usage Category"],
    )
    df = df.merge(response_df, on="Customer", how="left")
    filename = "output/customer-usage-stats-with-categories.csv"
    df.to_csv(filename, index=False)

    print(f"csv file {filename} saved.")
