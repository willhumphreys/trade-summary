import os
import pandas as pd
import numpy as np
import logging
import sys
import csv

# Configure logging (ensure it's configured in the main script)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_output_directory(path):
    """Creates the output directory if it doesn't exist."""
    # Check if path is None or empty before creating
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        logging.info(f"Output directory '{path}' created or already exists.")
    elif not path:
        logging.warning("Path provided for directory creation is empty or None.")


def calculate_z_scores(df, column_name):
    """
    Calculates Z-scores for a given column in the DataFrame.
    Handles potential division by zero if standard deviation is 0.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column_name (str): The name of the column to calculate Z-scores for.

    Returns:
        pd.Series: A Series containing the Z-scores for the specified column.
                   Returns a Series of zeros if standard deviation is 0 or NaN.
    """
    # Ensure the column exists
    if column_name not in df.columns:
        logging.error(f"Column '{column_name}' not found for Z-score calculation.")
        # Return a series of NaNs or zeros, or raise an error, depending on desired handling
        return pd.Series(np.nan, index=df.index)

    # Ensure column is numeric before calculating mean/std
    if not pd.api.types.is_numeric_dtype(df[column_name]):
        logging.warning(f"Column '{column_name}' is not numeric. Attempting conversion for Z-score.")
        # Attempt conversion, keep track of NaNs introduced
        numeric_col = pd.to_numeric(df[column_name], errors='coerce')
        nan_count = numeric_col.isnull().sum() - df[column_name].isnull().sum()
        if nan_count > 0:
            logging.warning(f"Coercion introduced {nan_count} NaNs in '{column_name}'.")
    else:
        numeric_col = df[column_name]


    mean = numeric_col.mean()
    std_dev = numeric_col.std()

    # Check for zero or NaN standard deviation or mean
    if std_dev == 0 or pd.isna(std_dev) or pd.isna(mean):
        # If all values are the same, std dev is 0, or mean is NaN (e.g., all NaNs), Z-score is 0
        logging.warning(f"Standard deviation or mean for {column_name} is 0 or NaN. Z-scores set to 0.")
        return pd.Series(0.0, index=df.index)
    else:
        # Calculate Z-score using the numeric version of the column
        return (numeric_col - mean) / std_dev

def calculate_zscore_composite_score(df):
    """
    Calculates the composite score based on weighted Z-scores of selected metrics.
    Assumes input DataFrame contains the necessary raw metric columns from Athena.

    Args:
        df (pd.DataFrame): DataFrame containing raw metrics for all traders.

    Returns:
        pd.DataFrame: DataFrame with added 'CompositeScore' column.

    Raises:
        ValueError: If required columns for calculation are missing.
    """
    # --- 1. Define Columns and Weights ---
    metrics_to_standardize = {
        'sortino_ratio': 0.30,
        'recovery_factor': 0.25,
        'profit_factor': 0.20,
        'max_drawdown_duration': -0.15, # Weight is negative because lower duration is better
        'log_tradecount': 0.10
    }
    # Base columns needed from Athena output for the calculation
    required_base_columns = ['sortino_ratio', 'recovery_factor', 'profit_factor', 'max_drawdown_duration', 'tradecount']

    # --- 2. Check for Required Columns ---
    missing_cols = [col for col in required_base_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in input data for Z-score composite score: {missing_cols}. "
                         f"Please ensure the metrics file contains these columns: {required_base_columns}")

    # --- 3. Prepare Data for Z-Score Calculation ---
    # Select only the necessary columns to avoid modifying others unintentionally
    df_calc = df[required_base_columns].copy() # Work on a copy

    # Convert columns to numeric, coercing errors (preserves NaNs)
    logging.info("Converting required columns to numeric for Z-score calculation...")
    for col in required_base_columns:
        original_dtype = df_calc[col].dtype
        df_calc[col] = pd.to_numeric(df_calc[col], errors='coerce')
        if not pd.api.types.is_numeric_dtype(df_calc[col]):
            logging.warning(f"Column '{col}' could not be fully converted to numeric (original dtype: {original_dtype}). Check data quality.")


    # Handle Potential Infinite Values resulting from division by zero in ratios
    logging.info("Handling potential infinite values in ratio columns...")
    for col in ['sortino_ratio', 'recovery_factor', 'profit_factor']:
        if col in df_calc.columns:
            inf_count = np.isinf(df_calc[col]).sum()
            if inf_count > 0:
                logging.info(f"Replacing {inf_count} inf/-inf values in '{col}' with NaN.")
                df_calc[col] = df_calc[col].replace([np.inf, -np.inf], np.nan)

            # Fill NaNs resulting from coercion or infinite values.
            # Filling with 0 assumes these cases don't contribute positively or negatively.
            # Consider using median if appropriate: fill_value = df_calc[col].median()
            fill_value = 0 # Example: Fill NaN with 0
            nan_count_before = df_calc[col].isnull().sum()
            if nan_count_before > 0:
                logging.info(f"Filling {nan_count_before} NaN values in '{col}' with {fill_value}.")
                df_calc[col] = df_calc[col].fillna(fill_value)


    # Calculate log_tradecount. Add epsilon to handle tradecount=0 safely.
    # Ensure tradecount is non-negative before log. Fill NaN tradecounts with 0.
    logging.info("Calculating log_tradecount...")
    if 'tradecount' in df_calc.columns:
        nan_tradecount = df_calc['tradecount'].isnull().sum()
        if nan_tradecount > 0:
            logging.info(f"Filling {nan_tradecount} NaN values in 'tradecount' with 0.")
            df_calc['tradecount'] = df_calc['tradecount'].fillna(0)

        # Ensure tradecount is numeric and non-negative
        df_calc['tradecount'] = pd.to_numeric(df_calc['tradecount'], errors='coerce').fillna(0).clip(lower=0)
        # Add 1 before log to handle tradecount=0 correctly (log(1)=0)
        df_calc['log_tradecount'] = np.log(df_calc['tradecount'] + 1)
        logging.info("Calculated log(tradecount + 1).")
    else:
        # This case should be caught by the initial check, but added for robustness
        raise ValueError("Column 'tradecount' is missing, cannot calculate log_tradecount.")


    # --- 4. Calculate Z-Scores ---
    z_scores = pd.DataFrame(index=df_calc.index)
    logging.info("Calculating Z-scores...")
    for col in metrics_to_standardize.keys(): # Iterate through the keys needed for the score (includes log_tradecount)
        # Calculate Z-scores using the prepared df_calc DataFrame
        z_scores[f'z_{col}'] = calculate_z_scores(df_calc, col)
        # Fill any NaNs that might arise in z-scores (e.g., if std dev was 0 or input col was all NaN)
        nan_zscore = z_scores[f'z_{col}'].isnull().sum()
        if nan_zscore > 0:
            logging.warning(f"Filling {nan_zscore} NaN values in Z-scores for '{col}' with 0.")
            z_scores[f'z_{col}'] = z_scores[f'z_{col}'].fillna(0)
        logging.debug(f"Calculated Z-scores for {col}.")


    # --- 5. Calculate Weighted Composite Score ---
    logging.info("Calculating final weighted Z-score composite score...")
    # Initialize composite score Series on the original DataFrame's index
    composite_score_col = pd.Series(0.0, index=df.index)
    for col, weight in metrics_to_standardize.items():
        # Note: For max_drawdown_duration, the weight is already negative
        # Ensure the Z-score column exists before attempting to use it
        z_col_name = f'z_{col}'
        if z_col_name in z_scores.columns:
            composite_score_col += z_scores[z_col_name] * weight
            logging.debug(f"Added weighted Z-score for {col} (Weight: {weight})")
        else:
            logging.warning(f"Z-score column '{z_col_name}' not found. Skipping its contribution to composite score.")


    # Add the final score back to the original DataFrame
    df['CompositeScore'] = composite_score_col
    logging.info("Z-Score Composite Score calculation complete.")

    return df


def filter_strategies(df, composite_quantile_threshold=0.90, min_profit_factor=1.2, max_drawdown_ratio=0.5):
    """
    Filters strategies based on the Z-Score composite score and potentially other raw metrics.

    Parameters:
    - df (pd.DataFrame): DataFrame containing trader metrics including 'CompositeScore'.
    - composite_quantile_threshold: Percentile threshold for filtering by the Z-score composite score.
                                     Set to 0 or None to disable quantile filtering.
    - min_profit_factor: Minimum acceptable raw profit factor.
    - max_drawdown_ratio: Maximum allowed ratio of max drawdown to total profit.

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    df_filtered = df.copy() # Work on a copy
    initial_count = len(df_filtered)
    if initial_count == 0:
        logging.warning("Input DataFrame for filtering is empty. Returning empty DataFrame.")
        return df_filtered

    logging.info(f"Filtering strategies. Initial count: {initial_count}")

    # --- Filter by Z-Score Composite Score Quantile ---
    if composite_quantile_threshold is not None and composite_quantile_threshold > 0:
        if 'CompositeScore' in df_filtered.columns and not df_filtered['CompositeScore'].isnull().all():
            # Ensure CompositeScore is numeric before calculating quantile
            df_filtered['CompositeScore'] = pd.to_numeric(df_filtered['CompositeScore'], errors='coerce')
            if not df_filtered['CompositeScore'].isnull().all(): # Check again after coercion
                try:
                    score_threshold = df_filtered['CompositeScore'].quantile(composite_quantile_threshold)
                    if pd.isna(score_threshold):
                        logging.warning(f"Could not calculate {composite_quantile_threshold} quantile for CompositeScore (possibly too few data points). Skipping score filter.")
                    else:
                        logging.info(f"Filtering by Composite Score >= {score_threshold:.4f} ({composite_quantile_threshold*100:.1f}th percentile)")
                        df_filtered = df_filtered[df_filtered['CompositeScore'] >= score_threshold]
                        logging.info(f"Strategies after Composite Score filter: {len(df_filtered)} ({len(df_filtered)/initial_count*100:.1f}%)")
                except Exception as e:
                    logging.warning(f"Error calculating CompositeScore quantile: {e}. Skipping score filter.")
            else:
                logging.warning("CompositeScore column contains only NaNs after coercion. Skipping score filter.")
        else:
            logging.warning("CompositeScore column not found or all NaN. Skipping score filter.")
    else:
        logging.info("Quantile filtering based on Composite Score is disabled.")

    # --- Filter by Raw Profit Factor ---
    # (Assuming profit_factor column exists and is needed for filtering)
    if 'profit_factor' in df_filtered.columns:
        pf_numeric = pd.to_numeric(df_filtered['profit_factor'], errors='coerce').fillna(0)
        logging.info(f"Filtering by Profit Factor >= {min_profit_factor}")
        count_before = len(df_filtered)
        df_filtered = df_filtered[pf_numeric >= min_profit_factor]
        removed_count = count_before - len(df_filtered)
        logging.info(f"Strategies after Profit Factor filter: {len(df_filtered)} (Removed {removed_count})")
    else:
        # If profit_factor isn't in the input, this filter can't run.
        # It might be calculated within the score function but not added back explicitly,
        # or it might be expected from the input file.
        logging.warning("profit_factor column not found for filtering. Skipping this filter.")


    # --- Filter by Max Drawdown relative to Total Profit ---
    # (Assuming these columns exist in the input df)
    if 'max_drawdown' in df_filtered.columns and 'totalprofit' in df_filtered.columns:
        max_dd_numeric = pd.to_numeric(df_filtered['max_drawdown'], errors='coerce')
        total_profit_numeric = pd.to_numeric(df_filtered['totalprofit'], errors='coerce')

        valid_comparison = (total_profit_numeric > 0) & (~max_dd_numeric.isna()) & (~total_profit_numeric.isna())
        exceeds_ratio = max_dd_numeric > max_drawdown_ratio * total_profit_numeric

        rows_to_remove_mask = valid_comparison & exceeds_ratio
        num_to_remove = rows_to_remove_mask.sum()

        logging.info(f"Filtering by Max Drawdown <= {max_drawdown_ratio * 100}% of Total Profit (where Total Profit > 0)")
        df_filtered = df_filtered[~rows_to_remove_mask]
        logging.info(f"Strategies after Max Drawdown filter: {len(df_filtered)} (Removed {num_to_remove})")
    else:
        logging.warning("max_drawdown or totalprofit column not found for filtering. Skipping this filter.")


    final_count = len(df_filtered)
    logging.info(f"Filtering complete. Final count: {final_count} ({final_count/initial_count*100:.1f}% of initial)")
    return df_filtered

def sort_strategies(df, sort_by='CompositeScore', ascending=False):
    """Sort strategies DataFrame by a specific column."""
    if sort_by in df.columns:
        logging.info(f"Sorting strategies by {sort_by} {'ascending' if ascending else 'descending'}")
        df_sort = df.copy()
        df_sort[sort_by] = pd.to_numeric(df_sort[sort_by], errors='coerce')
        return df_sort.sort_values(by=sort_by, ascending=ascending, na_position='last')
    else:
        logging.warning(f"Sort column '{sort_by}' not found. Returning unsorted DataFrame.")
        return df

def save_summary(df, output_file):
    """Rounds specified numeric columns and saves the DataFrame to a CSV file."""
    if df.empty:
        logging.warning(f"DataFrame is empty. Skipping save to {output_file}")
        return

    logging.info(f"Saving summary to {output_file}")
    df_save = df.copy()
    numeric_cols = df_save.select_dtypes(include=np.number).columns
    cols_to_round = [
        col for col in numeric_cols
        if not ('id' in col.lower() or 'count' in col.lower() or 'duration' in col.lower() or 'year' in col.lower())
    ]

    try:
        df_save[cols_to_round] = df_save[cols_to_round].round(4)
        logging.info(f"Rounded numeric columns: {cols_to_round}")
    except Exception as e:
        logging.warning(f"Could not round columns: {e}")

    try:
        # Ensure parent directory exists
        parent_dir = os.path.dirname(output_file)
        if parent_dir: # Check if path has a directory part
            create_output_directory(parent_dir) # Use the helper function
        df_save.to_csv(output_file, index=False, quoting=csv.QUOTE_NONNUMERIC)
        logging.info(f"Successfully saved summary with {len(df_save)} rows to {output_file}")
    except Exception as e:
        logging.error(f"Error saving CSV file to {output_file}: {e}")


# Renamed function to reflect its purpose
def process_metrics_file(scenario, metrics_file_path, output_directory):
    """
    Main processing function for Z-Score method. Reads a metrics CSV file (e.g., from Athena),
    calculates the Z-score composite score, filters, sorts, and saves summaries.

    Args:
        scenario (str): Name of the scenario being processed (used for output file names).
        metrics_file_path (str): Path to the single CSV file containing pre-calculated metrics.
        output_directory (str): Base directory where summary and filtered folders will be created.
    """
    logging.info(f"--- Starting Z-Score processing for scenario: {scenario} ---")
    logging.info(f"Reading metrics file from: {metrics_file_path}")

    # --- Create Output Dirs ---
    # Output structure: <output_directory>/summary/ and <output_directory>/filtered/
    summary_output_dir = os.path.join(output_directory, "summary")
    filtered_output_dir = os.path.join(output_directory, "filtered")
    create_output_directory(summary_output_dir)
    create_output_directory(filtered_output_dir)

    # --- Load Data ---
    try:
        summary_df = pd.read_csv(metrics_file_path)
        logging.info(f"Loaded {len(summary_df)} records from metrics file: {metrics_file_path}")
        logging.info(f"Columns loaded: {summary_df.columns.tolist()}")
        summary_df.columns = summary_df.columns.str.strip() # Clean column names
        logging.info(f"Cleaned column names: {summary_df.columns.tolist()}")

        # Check for traderid (case-insensitive)
        traderid_col_found = False
        for col in summary_df.columns:
            if col.lower() == 'traderid':
                if col != 'traderid':
                    summary_df.rename(columns={col: 'traderid'}, inplace=True)
                traderid_col_found = True
                break
        if not traderid_col_found:
            logging.warning("Column 'traderid' (case-insensitive) not found.")

    except FileNotFoundError:
        logging.error(f"FATAL: Metrics file not found: {metrics_file_path}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"FATAL: Error loading metrics CSV from {metrics_file_path}: {e}", exc_info=True)
        sys.exit(1)

    # --- Calculate Z-Score Composite Score ---
    logging.info("Calculating Z-Score composite score...")
    try:
        # This function modifies summary_df by adding 'CompositeScore'
        summary_df = calculate_zscore_composite_score(summary_df)
    except ValueError as e: # Catch missing columns error
        logging.error(f"FATAL: Error calculating composite score: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"FATAL: Unexpected error during composite score calculation: {e}", exc_info=True)
        sys.exit(1)


    # --- Save Full Summary (with Z-score composite score) ---
    logging.info("Sorting full summary by CompositeScore...")
    summary_df_sorted = sort_strategies(summary_df.copy(), sort_by='CompositeScore', ascending=False)
    # Use suffix to indicate Z-score method
    full_summary_file_path = os.path.join(summary_output_dir, f'{scenario}_full_summary_zscore.csv')
    save_summary(summary_df_sorted, full_summary_file_path)

    # --- Filter Strategies ---
    logging.info("Filtering strategies based on Z-Score composite score and other criteria...")
    # Adjust filter parameters as needed
    filtered_df = filter_strategies(
        summary_df_sorted.copy(),
        composite_quantile_threshold=0.90, # Example: Keep top 10% by Z-score
        min_profit_factor=1.2,
        max_drawdown_ratio=0.5
    )

    # --- Sort Filtered Strategies ---
    logging.info("Sorting filtered summary by CompositeScore...")
    sorted_filtered_df = sort_strategies(filtered_df, sort_by='CompositeScore', ascending=False)

    # --- Save Filtered Summary ---
    # Use suffix to indicate Z-score method
    filtered_summary_path = os.path.join(filtered_output_dir, f'{scenario}_filtered_summary_zscore.csv')
    save_summary(sorted_filtered_df, filtered_summary_path)

    logging.info(f"--- Z-Score processing finished successfully for scenario: {scenario} ---")


# --- Example Usage (within a main script) ---
# if __name__ == "__main__":
#     scenario_name = 'YOUR_SCENARIO'
#     # This path would point to the DECOMPRESSED CSV file
#     metrics_csv_path = '/path/to/decompressed/metrics.csv'
#     # Base output directory for score files
#     score_output_dir = '/path/to/output/scores'
#
#     # Ensure the input file exists
#     if not os.path.exists(metrics_csv_path):
#          logging.error(f"FATAL: Input metrics CSV file not found: {metrics_csv_path}")
#          sys.exit(1)
#
#     # Run the processing
#     try:
#         process_metrics_file(scenario_name, metrics_csv_path, score_output_dir)
#     except Exception as main_exception:
#         logging.error(f"An unexpected error occurred during the main processing: {main_exception}", exc_info=True)
#         sys.exit(1)

