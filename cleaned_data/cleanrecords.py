import os
import pandas as pd
import ast

def parse_dict_if_string(x):
    """
    Attempts to parse a string as a dictionary if it looks like one.
    If parsing fails or if x is not a string, returns x as is.
    """
    if isinstance(x, str) and x.strip().startswith("{") and x.strip().endswith("}"):
        try:
            return ast.literal_eval(x)
        except Exception:
            return x
    return x

def flatten_dataframe(df):
    """
    For every column in the DataFrame that contains dictionaries,
    expand its keys into new columns (with names like 'col_key') and drop the original column.
    """
    df_flat = df.copy()

    # Parse string representations of dictionaries.
    for col in df_flat.columns:
        df_flat[col] = df_flat[col].apply(parse_dict_if_string)

    for col in list(df_flat.columns):
        if df_flat[col].dropna().apply(lambda x: isinstance(x, dict)).all():
            expanded = df_flat[col].apply(lambda x: pd.Series(x) if isinstance(x, dict) else pd.Series())
            expanded = expanded.rename(columns=lambda k: f"{col}_{k}")
            df_flat = df_flat.drop(col, axis=1).join(expanded)
    
    return df_flat

def save_to_csv(df, filename):
    """
    Saves the provided DataFrame to a CSV file with the given filename.
    """
    df.to_csv(filename, index=False)
    print(f"DataFrame saved to {filename}")

# === Example Usage with Corrected Paths ===
if __name__ == '__main__':
    # Define the directory containing your CSV files.
    directory = 'output_directory'
    
    # Construct full paths for the input and output files.
    input_filename = os.path.join(directory, 'records_2000_2024.csv')
    output_filename = os.path.join(directory, 'records_2000s_clean.csv')
    
    try:
        df = pd.read_csv(input_filename)
        print("Original DataFrame (first few rows):")
        print(df.head())
    except Exception as e:
        print(f"Error reading {input_filename}: {e}")
        exit(1)
    
    # Flatten dictionary columns.
    df_flattened = flatten_dataframe(df)
    print("\nFlattened DataFrame (first few rows):")
    print(df_flattened.head())
    
    # Save the flattened DataFrame to CSV.
    save_to_csv(df_flattened, output_filename)
