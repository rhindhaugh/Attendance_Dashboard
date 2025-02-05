import pandas as pd
from pathlib import Path

def load_key_card_data(filepath: str) -> pd.DataFrame:
    """
    Load key card CSV data.
    Assumes we have columns 'Date/time' and 'User'.
    """
    return pd.read_csv(filepath)

def load_employee_info(filepath: str) -> pd.DataFrame:
    """
    Load employee info CSV data.
    The important column here is 'Employee #'.
    """
    return pd.read_csv(filepath)

if __name__ == "__main__":
    # Example usage
    base_path = Path(__file__).resolve().parent.parent  # go up one level from 'src'
    key_card_path = base_path / "data" / "raw" / "key_card_access.csv"
    employee_info_path = base_path / "data" / "raw" / "employee_info.csv"

    key_card_df = load_key_card_data(str(key_card_path))
    employee_info_df = load_employee_info(str(employee_info_path))

    print("Key card shape:", key_card_df.shape)
    print("Employee info shape:", employee_info_df.shape)
