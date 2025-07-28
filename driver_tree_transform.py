import pandas as pd
import numpy as np # Import numpy for np.inf and np.nan if needed for specific handling, though pandas handles most cases

# --- Configuration Constants ---
EXCEL_FILE_PATH = 'driver_tree.xlsx'
OUTPUT_EXCEL_PATH = 'c:\\Users\\1134931\\master_report\\driver_tree_transformed.xlsx'
COLUMNS_TO_UNPIVOT_RANGE = range(1, 53) # Fiscal weeks 1 to 52

FISCAL_WEEK_COL_NAME = 'FiscalWeek'
VALUE_COL_NAME = 'Value'
RUNRATE_COL_NAME = 'Runrate'
TIME_PERIOD_COL_NAME = 'TimePeriod'
COHORT_COL_NAME = 'Cohort'
CHANNEL_COL_NAME = 'Channel'
VALUES_COL_NAME = 'Values' # Note: 'Values' is a common pandas method, ensure this column name is intentional

WEEKS_IN_YEAR = 52
YEAR_MAP = {
    'FY2024': 0,
    'FY2025': 1
}
RUNRATE_GROUPING_COLS = [COHORT_COL_NAME, CHANNEL_COL_NAME, VALUES_COL_NAME]
RUNRATE_WINDOW = 4
RUNRATE_MIN_PERIODS = 4

CURRENT_TIME_PERIOD_LABEL = 'FY2025'
PREVIOUS_TIME_PERIOD_LABEL = 'FY2024'
YOY_MERGE_KEYS = [COHORT_COL_NAME, CHANNEL_COL_NAME, VALUES_COL_NAME, FISCAL_WEEK_COL_NAME]
LAST_YEAR_VALUE_COL_NAME = 'LastYearValue' # Column name for the previous year's value


def load_data(file_path: str) -> pd.DataFrame:
    """Loads data from an Excel file."""
    print(f"Loading data from {file_path}...")
    return pd.read_excel(file_path)

def get_columns_to_unpivot(df_columns: pd.Index, expected_range: range) -> list:
    """
    Determines the actual weekly columns to unpivot from the DataFrame.
    Tries integer column names first, then stringified versions.
    """
    # Try integer column names first
    columns_to_unpivot_int = [i for i in expected_range]
    actual_columns = [col for col in columns_to_unpivot_int if col in df_columns]

    if not actual_columns:
        print(f"Warning: Integer columns {columns_to_unpivot_int} not found. Trying stringified column names...")
        # Attempt with stringified column names as a common fallback
        columns_to_unpivot_str = [str(i) for i in expected_range]
        actual_columns = [col for col in columns_to_unpivot_str if col in df_columns]

    if not actual_columns:
        raise ValueError(
            f"Failed to find columns to unpivot (tried integers {list(expected_range)} and strings '1'-'{expected_range.stop-1}'). "
            f"Actual DataFrame columns: {df_columns.tolist()}"
        )
    
    # Check if all expected columns were found (either as int or str)
    # This logic assumes either all are int or all are str, not a mix for the 1-52 range.
    expected_cols_found_count = len(actual_columns)
    expected_total_count = len(list(expected_range))

    if expected_cols_found_count < expected_total_count:
        # Determine which type (int or str) was being matched to report missing ones accurately
        if all(isinstance(c, int) for c in actual_columns) or (not actual_columns and all(isinstance(c, int) for c in columns_to_unpivot_int)):
            missing_cols = [col for col in columns_to_unpivot_int if col not in actual_columns]
        else:
            missing_cols = [col for col in columns_to_unpivot_str if col not in actual_columns]
        print(f"Warning: Some specified columns_to_unpivot were not found: {missing_cols}")
    
    print(f"Identified columns to unpivot: {actual_columns[:5]}... (total {len(actual_columns)})")
    return actual_columns

def unpivot_data(df: pd.DataFrame, id_vars: list, value_vars: list, 
                   fiscal_week_col_name: str, value_col_name: str) -> pd.DataFrame:
    """Performs the unpivot (melt) operation on the DataFrame."""
    print("Unpivoting data...")
    if not value_vars:
        print("Error: No columns found to unpivot. Returning original DataFrame structure.")
        # Create empty FiscalWeek and Value columns if they don't exist to prevent downstream errors
        # Or, handle this more gracefully depending on desired behavior
        if fiscal_week_col_name not in df.columns:
            df[fiscal_week_col_name] = pd.NA
        if value_col_name not in df.columns:
            df[value_col_name] = pd.NA
        return df.copy()
        
    df_unpivoted = pd.melt(df,
                           id_vars=id_vars,
                           value_vars=value_vars,
                           var_name=fiscal_week_col_name,
                           value_name=value_col_name)
    print("Data unpivoted successfully.")
    return df_unpivoted

def calculate_runrate(df: pd.DataFrame, time_period_col: str, fiscal_week_col: str, 
                        value_col: str, grouping_cols: list, year_map: dict, 
                        weeks_in_year: int, runrate_window: int, runrate_min_periods: int,
                        runrate_col_name: str) -> pd.DataFrame:
    """Calculates the continuous runrate across fiscal years."""
    print("Calculating runrate...")
    df_processed = df.copy()

    if fiscal_week_col not in df_processed.columns or value_col not in df_processed.columns:
        print(f"Warning: '{fiscal_week_col}' or '{value_col}' not in DataFrame. Skipping runrate calculation.")
        df_processed[runrate_col_name] = pd.NA
        return df_processed

    df_processed[fiscal_week_col] = pd.to_numeric(df_processed[fiscal_week_col], errors='coerce')
    df_processed['temp_YearOffset'] = df_processed[time_period_col].map(year_map)

    if df_processed['temp_YearOffset'].isnull().any():
        unmapped_periods = df_processed[df_processed['temp_YearOffset'].isnull()][time_period_col].unique()
        print(f"\nWarning: TimePeriods not in year_map for Runrate: {unmapped_periods}. Runrate may be NaN for these.")

    df_processed['temp_GlobalFiscalWeek'] = (df_processed['temp_YearOffset'] * weeks_in_year) + df_processed[fiscal_week_col]

    missing_base_group_cols = [col for col in grouping_cols if col not in df_processed.columns]
    if missing_base_group_cols:
        print(f"\nError: Grouping columns for Runrate missing: {missing_base_group_cols}. Skipping Runrate calculation.")
        df_processed[runrate_col_name] = pd.NA
    else:
        sort_order = grouping_cols + ['temp_GlobalFiscalWeek']
        actual_sort_order = [col for col in sort_order if col in df_processed.columns]
        if len(actual_sort_order) != len(sort_order):
            missing_sort_cols = [col for col in sort_order if col not in df_processed.columns]
            print(f"\nError: Sort order columns for Runrate missing: {missing_sort_cols}. Skipping Runrate calculation.")
            df_processed[runrate_col_name] = pd.NA
        else:
            df_processed = df_processed.sort_values(by=actual_sort_order)
            df_processed[runrate_col_name] = df_processed.groupby(grouping_cols)[value_col] \
                                           .transform(lambda x: x.rolling(window=runrate_window, min_periods=runrate_min_periods).mean())
    
    df_processed = df_processed.drop(columns=['temp_YearOffset', 'temp_GlobalFiscalWeek'], errors='ignore')
    print("Runrate calculated.")
    return df_processed

def calculate_yoy_growth(df: pd.DataFrame, time_period_col: str, fiscal_week_col: str, 
                           value_col: str, runrate_col: str, current_tp_label: str, 
                           previous_tp_label: str, yoy_merge_keys: list) -> pd.DataFrame:
    """Calculates Year-on-Year growth for Value and Runrate."""
    print("Calculating Year-on-Year growth...")
    df_processed = df.copy()
    yoy_growth_col = 'YearOnYearGrowth'
    yoy_runrate_growth_col = 'YearOnYearRunrateGrowth'
    last_year_val_col = LAST_YEAR_VALUE_COL_NAME # Use the constant

    # Initialize YoY columns
    df_processed[yoy_growth_col] = pd.NA
    df_processed[yoy_runrate_growth_col] = pd.NA
    df_processed[last_year_val_col] = pd.NA # Initialize the new column

    required_cols_for_yoy = yoy_merge_keys + [time_period_col, value_col, runrate_col]
    if not all(col in df_processed.columns for col in required_cols_for_yoy):
        missing = [col for col in required_cols_for_yoy if col not in df_processed.columns]
        print(f"\nWarning: Columns for YoY missing: {missing}. Skipping YoY calculation.")
        return df_processed

    df_current_tp = df_processed[df_processed[time_period_col] == current_tp_label].copy()
    df_previous_tp = df_processed[df_processed[time_period_col] == previous_tp_label].copy()

    if df_current_tp.empty or df_previous_tp.empty:
        print(f"\nWarning: Data for '{current_tp_label}' or '{previous_tp_label}' is missing. YoY calculation will result in NAs.")
        return df_processed

    df_previous_tp_metrics = df_previous_tp[yoy_merge_keys + [value_col, runrate_col]].rename(
        columns={value_col: f'{value_col}_PY', runrate_col: f'{runrate_col}_PY'}
    )

    df_merged_for_yoy = pd.merge(
        df_current_tp,
        df_previous_tp_metrics,
        on=yoy_merge_keys,
        how='inner'
    )

    # Calculate YoY growth percentages: ((Current / Previous) - 1) * 100
    df_merged_for_yoy[yoy_growth_col] = \
        ((df_merged_for_yoy[value_col] / df_merged_for_yoy[f'{value_col}_PY']) - 1) 
    df_merged_for_yoy[yoy_runrate_growth_col] = \
        ((df_merged_for_yoy[runrate_col] / df_merged_for_yoy[f'{runrate_col}_PY']) - 1)

    # --- Debugging: Check index uniqueness of df_processed ---
    if not df_processed.index.is_unique:
        print("\nCRITICAL WARNING: Index of df_processed is NOT unique before YoY assignment!")
        print(f"Number of duplicated indices in df_processed: {df_processed.index.duplicated().sum()}")
        # print("Sample of duplicated indices in df_processed:")
        # print(df_processed[df_processed.index.duplicated(keep=False)].head())
    else:
        print("\nINFO: Index of df_processed IS unique before YoY assignment.")
    # --- End Debugging ---
    # Update the main DataFrame with calculated YoY growth values for the current time period rows
    # Use .loc with the index from df_merged_for_yoy to ensure correct assignment
    # df_processed.loc[df_merged_for_yoy.index, yoy_growth_col] = df_merged_for_yoy[yoy_growth_col]
    # df_processed.loc[df_merged_for_yoy.index, yoy_runrate_growth_col] = df_merged_for_yoy[yoy_runrate_growth_col]
    # df_processed.loc[df_merged_for_yoy.index, last_year_val_col] = df_merged_for_yoy[f'{value_col}_PY'] # Assign the PY value
    # df_processed.to_excel('c:\\Users\\1134931\\master_report\\temp_processed.xlsx', index=True) # Save with index
    print("Year-on-Year growth calculated.")
    df_merged_for_yoy.drop(columns=['LastYearValue','Value_PY','Runrate_PY'], inplace=True, errors='ignore')
    return df_merged_for_yoy

def randomize_values(df: pd.DataFrame, value_col: str, percentage_variation: float) -> pd.DataFrame:
    """
    Randomizes the values in the specified column by a given percentage variation.
    New Value = Original Value * (1 +/- random_percentage_within_variation)
    """
    print(f"Randomizing '{value_col}' by +/- {percentage_variation*100:.0f}%...")
    df_processed = df.copy()

    if value_col not in df_processed.columns:
        print(f"Warning: Column '{value_col}' not found. Skipping randomization.")
        return df_processed

    # Ensure the value column is numeric, coercing errors will turn non-numeric to NaN
    df_processed[value_col] = pd.to_numeric(df_processed[value_col], errors='coerce')

    # Generate random factors between -percentage_variation and +percentage_variation
    random_factors = np.random.uniform(-percentage_variation, percentage_variation, size=len(df_processed))
    df_processed[value_col] = df_processed[value_col] * (1 + random_factors)
    print(f"'{value_col}' randomized successfully.")
    return df_processed

def save_data(df: pd.DataFrame, output_path: str):
    """Saves the DataFrame to an Excel file."""
    print(f"Saving transformed data to {output_path}...")
    df.to_excel(output_path, index=False)
    print("Data saved successfully.")

def main():
    """Main function to orchestrate the data transformation process."""
    try:
        # Load data
        df_raw = load_data(EXCEL_FILE_PATH)
        print("\nOriginal DataFrame head:")
        print(df_raw.head())

        # Determine columns to unpivot
        actual_columns_to_unpivot = get_columns_to_unpivot(df_raw.columns, COLUMNS_TO_UNPIVOT_RANGE)
        
        # Identify id_vars
        id_vars = [col for col in df_raw.columns if col not in actual_columns_to_unpivot]

        # Unpivot data
        df_unpivoted = unpivot_data(df_raw, id_vars, actual_columns_to_unpivot, 
                                    FISCAL_WEEK_COL_NAME, VALUE_COL_NAME)
        print("\nUnpivoted DataFrame head:")
        print(df_unpivoted.head())

        # Randomize 'Value' column before calculating Runrate
        # The request is to vary by +/- 30%
        df_randomized_values = randomize_values(df_unpivoted, VALUE_COL_NAME, 0.30)
        print(f"\nDataFrame with randomized '{VALUE_COL_NAME}' (head):")
        print(df_randomized_values.head())

        # Calculate Runrate
        df_with_runrate = calculate_runrate(df_randomized_values, TIME_PERIOD_COL_NAME, FISCAL_WEEK_COL_NAME,
                                            VALUE_COL_NAME, RUNRATE_GROUPING_COLS, YEAR_MAP,
                                            WEEKS_IN_YEAR, RUNRATE_WINDOW, RUNRATE_MIN_PERIODS,
                                            RUNRATE_COL_NAME)
        print(f"\nDataFrame with {RUNRATE_COL_NAME} (tail):")
        print(df_with_runrate.tail(15))

        # Calculate Year-on-Year Growth
        df_final = calculate_yoy_growth(df_with_runrate, TIME_PERIOD_COL_NAME, FISCAL_WEEK_COL_NAME,
                                        VALUE_COL_NAME, RUNRATE_COL_NAME, CURRENT_TIME_PERIOD_LABEL,
                                        PREVIOUS_TIME_PERIOD_LABEL, YOY_MERGE_KEYS)
        
        print(f"\nFinal DataFrame with YoY Growth columns (sample for {CURRENT_TIME_PERIOD_LABEL}):")
        print(df_final[df_final[TIME_PERIOD_COL_NAME] == CURRENT_TIME_PERIOD_LABEL].tail())

        yoy_growth_col_name = 'YearOnYearGrowth' # As defined inside calculate_yoy_growth
        if yoy_growth_col_name in df_final.columns:
            print(f"\nSample of rows with {yoy_growth_col_name} calculated:")
            not_na_yoy = df_final[df_final[yoy_growth_col_name].notna()]
            # Include LastYearValue in the sample print if it exists
            sample_cols_to_print = [TIME_PERIOD_COL_NAME, FISCAL_WEEK_COL_NAME, VALUE_COL_NAME, yoy_growth_col_name]
            if LAST_YEAR_VALUE_COL_NAME in not_na_yoy.columns:
                sample_cols_to_print.append(LAST_YEAR_VALUE_COL_NAME)
            print(not_na_yoy[sample_cols_to_print].sample(min(5, len(not_na_yoy))))
        
        # --- Filter df_final to keep only MAX 4 latest FiscalWeeks with Value > 0 ---
        print(f"\nFiltering final DataFrame to keep max 4 latest FiscalWeeks with {VALUE_COL_NAME} > 0...")
        if VALUE_COL_NAME in df_final.columns and FISCAL_WEEK_COL_NAME in df_final.columns:
            # Identify FiscalWeeks where Value > 0
            positive_value_weeks_df = df_final[df_final[VALUE_COL_NAME] > 0]
            if not positive_value_weeks_df.empty:
                latest_positive_weeks = sorted(positive_value_weeks_df[FISCAL_WEEK_COL_NAME].unique(), reverse=True)[:4]
                
                if latest_positive_weeks:
                    print(f"Selected latest FiscalWeeks for final output: {latest_positive_weeks}")
                    df_final = df_final[df_final[FISCAL_WEEK_COL_NAME].isin(latest_positive_weeks)].copy()
                else:
                    print(f"No FiscalWeeks found with {VALUE_COL_NAME} > 0. The DataFrame might be empty or only contain non-positive values.")
            else:
                print(f"No rows found with {VALUE_COL_NAME} > 0. DataFrame will likely be empty after this filter if not already.")
        else:
            print(f"Warning: '{VALUE_COL_NAME}' or '{FISCAL_WEEK_COL_NAME}' not in DataFrame. Skipping final FiscalWeek filtering.")
        # --- End Filtering ---

        # Save data
        save_data(df_final, OUTPUT_EXCEL_PATH)

    except FileNotFoundError:
        print(f"Error: The input file '{EXCEL_FILE_PATH}' was not found.")
    except ValueError as ve:
        print(f"A ValueError occurred during processing: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
