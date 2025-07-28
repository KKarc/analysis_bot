import pandas as pd
import json
import google.generativeai as genai
import os
# import gradio as gr # Gradio is handled by gradio_interface.py

# --- Configuration ---
KEYS_FILE = 'c:\\Users\\1134931\\master_report\\keys.json' # Path to your keys.json file
TRANSFORMED_DATA_FILE = 'c:\\Users\\1134931\\master_report\\driver_tree_transformed.xlsx'
CURRENT_TIME_PERIOD = 'FY2025' # e.g., 'FY2025'

# --- Load API Key ---
GEMINI_API_KEY = None
try:
    with open(KEYS_FILE, 'r') as f:
        keys = json.load(f)
        GEMINI_API_KEY = keys.get("GEMINI_API_KEY")
    if not GEMINI_API_KEY or GEMINI_API_KEY == "YOUR_GEMINI_API_KEY_HERE":
        print(f"Error: GEMINI_API_KEY not found or not set in {KEYS_FILE}.")
        print("Please ensure your API key is correctly set in the keys.json file.")
        exit()
    # print("Gemini API key loaded successfully.") # Less verbose on import
except FileNotFoundError:
    print(f"Error: Keys file not found at {KEYS_FILE}")
    print("Please create the file and add your GEMINI_API_KEY.")
    exit()
except json.JSONDecodeError:
    print(f"Error: Could not decode JSON from {KEYS_FILE}")
    print("Please ensure the keys.json file is valid JSON.")
    exit()
except Exception as e:
    print(f"An unexpected error occurred while loading the API key: {e}")
    exit()

# --- Configure Gemini API ---
model = None
try:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-1.5-flash-latest')
    # print("Gemini API configured successfully.") # Less verbose on import
except Exception as e:
    print(f"Error configuring Gemini API: {e}")
    print("Please check your API key and network connection.")
    exit()

# --- Global variables for data context ---
analysis_data_markdown = ""
highest_fiscal_week_for_analysis = None
current_time_period_for_analysis = CURRENT_TIME_PERIOD
df_transformed = None

# --- Load Transformed Data (executed on import) ---
try:
    if os.path.exists(TRANSFORMED_DATA_FILE):
        df_transformed = pd.read_excel(TRANSFORMED_DATA_FILE)
        # print(f"Transformed data loaded successfully from {TRANSFORMED_DATA_FILE}") # Less verbose
        if df_transformed.empty:
            print("Warning: The loaded transformed data file is empty.")
    else:
        print(f"Error: Transformed data file not found at {TRANSFORMED_DATA_FILE}")
        print("Please ensure 'driver_tree_transform.py' has been run successfully.")
        exit()
except Exception as e:
    print(f"An error occurred loading the transformed data: {e}")
    exit()

def load_and_prepare_data_for_analysis():
    """
    Filters loaded data for the highest fiscal week in CURRENT_TIME_PERIOD
    with positive Value, and prepares a markdown string.
    Sets global variables: analysis_data_markdown, highest_fiscal_week_for_analysis.
    """
    global analysis_data_markdown, highest_fiscal_week_for_analysis, current_time_period_for_analysis, df_transformed

    if df_transformed is None:
        return "Error: Transformed data not available (was None)."
    if df_transformed.empty:
        return "Error: Transformed data is empty."

    required_cols = ['TimePeriod', 'FiscalWeek', 'Value', 'Runrate', 'YearOnYearGrowth', 'YearOnYearRunrateGrowth', 'Cohort', 'Channel', 'Values']
    if not all(col in df_transformed.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df_transformed.columns]
        return f"Error: Transformed data is missing required columns: {missing}."

    df_current_tp_positive = df_transformed[
        (df_transformed['TimePeriod'] == current_time_period_for_analysis) &
        (df_transformed['Value'] > 0) &
        (df_transformed['FiscalWeek'].notna())
    ].copy()

    if df_current_tp_positive.empty:
        return f"No data found for '{current_time_period_for_analysis}' with positive 'Value'."

    highest_fiscal_week_for_analysis = df_current_tp_positive['FiscalWeek'].max()
    # print(f"Preparing data for analysis: {current_time_period_for_analysis}, Week {int(highest_fiscal_week_for_analysis)}")

    df_analysis_subset = df_current_tp_positive[
        df_current_tp_positive['FiscalWeek'] == highest_fiscal_week_for_analysis
    ].copy()

    if df_analysis_subset.empty:
         return f"Unexpected error: Could not select data for highest FiscalWeek {int(highest_fiscal_week_for_analysis)}."

    analysis_cols_for_prompt = ['Cohort', 'Channel', 'Values', 'FiscalWeek', 'Value', 'Runrate', 'YearOnYearGrowth', 'YearOnYearRunrateGrowth']
    df_analysis_subset_for_prompt = df_analysis_subset[analysis_cols_for_prompt]
    analysis_data_markdown = df_analysis_subset_for_prompt.to_markdown(index=False)
    return None # No error

def get_initial_gemini_analysis():
    """Generates the initial summary analysis using Gemini."""
    if not model:
        return "Error: Gemini model not configured."
    if not analysis_data_markdown:
        return "Error: Analysis data not loaded or prepared."
    if highest_fiscal_week_for_analysis is None:
        return "Error: Highest fiscal week for analysis not determined."

    prompt = f"""Analyze the following sales/performance data for Fiscal Year {current_time_period_for_analysis}, Week {int(highest_fiscal_week_for_analysis)}.
The data includes 'Value' (actual performance), 'Runrate' (4-week rolling average of previous weeks), 'YearOnYearGrowth' (% change in Value vs. same week last year), and 'YearOnYearRunrateGrowth' (% change in Runrate vs. same week last year).

Data (Cohort, Channel, Specific Value/Metric, Fiscal Week, Current Value, Current Runrate, YoY Value Growth %, YoY Runrate Growth %):
```
{analysis_data_markdown}
```
First look at Values 'SUM of SALES' YearOnYearGrowth and YearOnYearRunrateGrowth to identify underperforming areas. For those areas look into remaining Values to flag Cohorts and Channels that are driving the underperformance.
Identify key 'Values', 'Cohorts', and 'Channels' that appear to be underperforming based on their 'YearOnYearGrowth' and 'YearOnYearRunrateGrowth' percentages.
Consider both negative growth and significantly lower positive growth compared to other segments.
Focus on actionable insights. What are the 2-3 most critical areas to investigate further? Look at available information on Coles and Aldi activity in the same week.

Provide a concise summary highlighting the main areas of underperformance in the tone of the retired Woolworths CEO - Brad Banducci - who is consulting his former company a little grumpy about his retirement.
"""
    try:
        # print("\nSending data to Gemini for initial analysis...") # Optional: for debugging
        response = model.generate_content(prompt)
        # print("Initial analysis received.") # Optional: for debugging
        return response.text
    except Exception as e:
        print(f"\nError during Gemini API call for initial analysis: {e}")
        # It might be helpful to see response.prompt_feedback if available and an error occurs
        # For example, if hasattr(response, 'prompt_feedback'):
        #    print(f"Prompt feedback: {response.prompt_feedback}")
        return f"Error getting initial analysis from Gemini: {e}"

def answer_user_question(user_question):
    """Answers a user's question based on the prepared data context using Gemini."""
    global model, analysis_data_markdown, highest_fiscal_week_for_analysis, current_time_period_for_analysis # Ensure globals are accessible if needed

    if not model:
        return "Error: Gemini model not configured."
    if not analysis_data_markdown: # Make sure this is populated by load_and_prepare_data_for_analysis
        return "Error: Analysis data not available for Q&A."
    if not user_question or user_question.strip() == "":
        return "Please enter a question."
    if highest_fiscal_week_for_analysis is None: # Make sure this is populated
        return "Error: Fiscal week context not available for Q&A."

    prompt = f"""
You are the retired Woolworths CEO - Brad Banducci - who is consulting his former company a little grumpy about his retirement.
Based *only* on the data provided below for Fiscal Year {current_time_period_for_analysis}, Week {int(highest_fiscal_week_for_analysis)}, answer the user's question.
If the data does not contain the answer, clearly state that the information is not available in the provided dataset.
You can check the Internet for more commercial context and share the information with the user.

Data (Cohort, Channel, Specific Value/Metric, Fiscal Week, Current Value, Current Runrate, YoY Value Growth %, YoY Runrate Growth %)
```
{analysis_data_markdown}
```

User's Question: {user_question}

Answer:
"""
    try:
        # print(f"\nSending question to Gemini: {user_question}") # Optional: for debugging
        response = model.generate_content(prompt)
        # print("Answer received from Gemini.") # Optional: for debugging
        return response.text
    except Exception as e:
        print(f"\nError during Gemini API call for Q&A: {e}")
        return f"Error getting answer from Gemini: {e}"