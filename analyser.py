import pandas as pd
import json
import google.generativeai as genai
import os

# --- Configuration ---
KEYS_FILE = 'c:\\Users\\1134931\\master_report\\keys.json' # Path to your keys.json file
TRANSFORMED_DATA_FILE = 'c:\\Users\\1134931\\master_report\\driver_tree_transformed.xlsx' # Path where the transformed data is saved
CURRENT_TIME_PERIOD = 'FY2025'

# --- Load API Key ---
try:
    with open(KEYS_FILE, 'r') as f:
        keys = json.load(f)
    GEMINI_API_KEY = keys.get("GEMINI_API_KEY")

    if not GEMINI_API_KEY or GEMINI_API_KEY == "YOUR_GEMINI_API_KEY_HERE":
        raise ValueError("Please replace 'YOUR_GEMINI_API_KEY_HERE' in keys.json with your actual API key.")

except FileNotFoundError:
    print(f"Error: Keys file not found at {KEYS_FILE}")
    exit()
except json.JSONDecodeError:
    print(f"Error: Could not parse JSON from {KEYS_FILE}. Ensure it's valid JSON.")
    exit()
except ValueError as ve:
    print(f"Configuration Error: {ve}")
    exit()
except Exception as e:
    print(f"An unexpected error occurred loading keys: {e}")
    exit()

# --- Configure Gemini API ---
try:
    genai.configure(api_key=GEMINI_API_KEY)
    # Choose a model, e.g., 'gemini-1.5-flash-latest' or 'gemini-1.5-pro-latest'
    # 'gemini-1.5-pro-latest' is generally more capable for analysis tasks
    model = genai.GenerativeModel("gemini-1.5-flash-latest")
    print("Gemini API configured successfully.")
except Exception as e:
    print(f"Error configuring Gemini API: {e}")
    print("Please check your API key and network connection.")
    exit()

# --- Load Transformed Data ---
try:
    if not os.path.exists(TRANSFORMED_DATA_FILE):
         # As the previous script didn't explicitly save, let's add a note
         print(f"Error: Transformed data file not found at {TRANSFORMED_DATA_FILE}.")
         print("Please ensure the 'driver_tree_transform.py' script is run first and saves its output to this location.")
         # Add a commented out save line suggestion for the other script
         # In driver_tree_transform.py, uncomment and adjust:
         # df_unpivoted.to_excel('c:\\Users\\1134931\\master_report\\driver_tree_transformed.xlsx', index=False)
         exit()

    df_transformed = pd.read_excel(TRANSFORMED_DATA_FILE)
    print(f"Successfully loaded transformed data from {TRANSFORMED_DATA_FILE}")

except FileNotFoundError:
    # This case is handled by the os.path.exists check above, but kept for clarity
    print(f"Error: Transformed data file not found at {TRANSFORMED_DATA_FILE}.")
    exit()
except Exception as e:
    print(f"An error occurred loading the transformed data: {e}")
    exit()

# --- Find Data for Highest FiscalWeek in FY2025 with Positive Value ---

# Ensure required columns exist
required_cols = ['TimePeriod', 'FiscalWeek', 'Value', 'YearOnYearGrowth', 'YearOnYearRunrateGrowth', 'Cohort', 'Channel', 'Values']
if not all(col in df_transformed.columns for col in required_cols):
    missing = [col for col in required_cols if col not in df_transformed.columns]
    print(f"Error: Transformed data is missing required columns for analysis: {missing}")
    print("Please ensure 'driver_tree_transform.py' ran successfully and produced these columns.")
    exit()

df_fy2025_positive = df_transformed[
    (df_transformed['TimePeriod'] == CURRENT_TIME_PERIOD) &
    (df_transformed['Value'] > 0) &
    (df_transformed['FiscalWeek'].notna()) # Ensure FiscalWeek is not NaN
].copy() # Use .copy() to avoid SettingWithCopyWarning

if df_fy2025_positive.empty:
    print(f"No data found for '{CURRENT_TIME_PERIOD}' with positive 'Value'. Cannot perform analysis.")
    exit()

# Find the highest FiscalWeek
highest_fiscal_week = df_fy2025_positive['FiscalWeek'].max()
print(f"Analyzing data for the highest FiscalWeek in {CURRENT_TIME_PERIOD} with positive value: Week {int(highest_fiscal_week)}")

# Select all rows for this highest FiscalWeek
df_analysis_data = df_fy2025_positive[
    df_fy2025_positive['FiscalWeek'] == highest_fiscal_week
].copy()

if df_analysis_data.empty:
     print(f"Unexpected error: Could not select data for highest FiscalWeek {int(highest_fiscal_week)}. Exiting.")
     exit()

# Select relevant columns for the prompt
analysis_cols = ['Cohort', 'Channel', 'Values', 'FiscalWeek', 'Value', 'Runrate', 'YearOnYearGrowth', 'YearOnYearRunrateGrowth']
df_analysis_data = df_analysis_data[analysis_cols]

# Convert the selected data to a string format for the model
# Using markdown table format is often effective
data_string = df_analysis_data.to_markdown(index=False)

# --- Prepare and Send Prompt to Gemini ---

prompt = f"""
Analyze the following sales/performance data for Fiscal Year {CURRENT_TIME_PERIOD}, Week {int(highest_fiscal_week)}.
The data includes 'Value' (actual performance), 'Runrate' (4-week rolling average of previous weeks), 'YearOnYearGrowth' (% change in Value vs. same week last year), and 'YearOnYearRunrateGrowth' (% change in Runrate vs. same week last year).

Data (Cohort, Channel, Specific Value/Metric, Fiscal Week, Current Value, Current Runrate, YoY Value Growth %, YoY Runrate Growth %):
```
{data_string}
```
First look at Values 'SUM of SALES' YearOnYearGrowth and YearOnYearRunrateGrowth to identify underperforming areas. For those areas look into remaining Values to flag Cohorts and Channels that are driving the underperformance.
Identify key 'Values', 'Cohorts', and 'Channels' that appear to be underperforming based on their 'YearOnYearGrowth' and 'YearOnYearRunrateGrowth' percentages.

Consider "underperforming" to generally mean:
- Negative YearOnYearGrowth or YearOnYearRunrateGrowth.
- Significantly lower positive growth compared to others in the same week.
- Pay particular attention to combinations of Cohort, Channel, and Value that show poor growth across both metrics.

Provide a concise summary highlighting the main areas of underperformance in the tone of a major retailer retired CEO who is consulting his former company.
"""

print("\nSending data to Gemini for analysis...")

try:
    # Use generate_content for text-only input
    response = model.generate_content(prompt)

    # --- Display Analysis ---
    print("\n--- Gemini Analysis ---")
    print(response.text)
    print("-----------------------")

except Exception as e:
    print(f"\nError during Gemini API call: {e}")
    print("Could not get analysis from the model.")