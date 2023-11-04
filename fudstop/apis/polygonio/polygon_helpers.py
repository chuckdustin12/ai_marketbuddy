import pandas as pd
def flatten_nested_dict(row, column_name):
    # Check if the value in the specified column is a dictionary
    if isinstance(row[column_name], dict):
        # Create new column names by combining the original column name and the nested keys
        for key, value in row[column_name].items():
            new_column_name = f"{key}"
            row[new_column_name] = value
        # Drop the original column with nested dictionary
        row = row.drop(column_name)
    return row


def flatten_dict(d, parent_key='', sep='.'):
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items
def normalize_columns(data, columns_to_normalize):
    # Convert dict to DataFrame if necessary
    if isinstance(data, dict):
        df = pd.DataFrame.from_dict(data)
    else:
        df = data
    
    # Apply the helper function to each row and each specified column
    for column in columns_to_normalize:
        if column in df.columns:
            df = df.apply(lambda row: flatten_nested_dict(row, column), axis=1)
        else:
            # Handle the case where the column does not exist
            # For example, you can print a warning message
            print(f"Warning: Column '{column}' does not exist in the dataframe and will be skipped.")
    
    return df
