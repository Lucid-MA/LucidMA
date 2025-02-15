import math
import platform
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import holidays
import pandas as pd


def read_skipped_files(skipped_files_tracker):
    try:
        with open(skipped_files_tracker, "r") as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_skipped(filename, skipped_files_tracker):
    with open(skipped_files_tracker, "a") as file:
        file.write(filename + "\n")


def read_processed_files(processed_files_tracker):
    try:
        with open(processed_files_tracker, "r") as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()


def mark_file_processed(filename, processed_files_tracker):
    with open(processed_files_tracker, "a") as file:
        file.write(filename + "\n")


def print_df(df):
    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        1000,
        "display.float_format",
        "{:.6f}".format,
    ):
        print(df)


def redirect_output_to_log_file(log_file_path):
    sys.stdout = open(log_file_path, "w")
    sys.stderr = sys.stdout


def get_file_path(file_path_windows):
    if platform.system() == "Darwin":  # macOS
        file_path = file_path_windows.replace("S:", "/Volumes/Sdrive$")
    elif platform.system() == "Windows":
        file_path = file_path_windows
    else:
        raise Exception("Unsupported platform")
    return file_path


def clean_and_convert_dates(df, date_columns):
    for col in date_columns:
        df[col] = pd.to_datetime(
            df[col], errors="coerce"
        )  # Convert to datetime, make errors NaT
        df[col] = df[col].fillna(pd.to_datetime("1900-01-01"))
    return df


def format_decimal(value):
    if isinstance(value, pd.Series):
        return value.apply(format_decimal)
    else:
        if pd.notnull(value):
            return f"{value:.6f}"
        else:
            return None


def get_trading_days(start_date, end_date):
    # Generate a range of dates
    all_days = pd.date_range(start=start_date, end=end_date, freq="B")

    # Get US holidays
    us_holidays = holidays.US(years=all_days.year.unique())

    # Filter out holidays
    trading_days = [day for day in all_days if day not in us_holidays]

    # Convert to the desired format
    trading_days_str = [day.strftime("%Y-%m-%d") for day in trading_days]

    return trading_days_str


def get_current_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_current_timestamp_datetime() -> datetime:
    return datetime.now()


def format_date_mm_dd_yyyy(date: datetime) -> str:
    return date.strftime("%m/%d/%y")


def format_date_YYYY_MM_DD(date: datetime) -> str:
    return date.strftime("%Y-%m-%d")


def to_YYYY_MM_DD(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def format_interest_rate(rate: float) -> str:
    return f"{rate * 100:.2f}" if not math.isnan(rate) else "n/a"


def format_interest_rate_one_decimal(rate: float) -> str:
    return f"{rate * 100:.1f}" if not math.isnan(rate) else "n/a"


def format_to_0_decimals(number: float) -> str:
    return f"{number:,.0f}"


def format_to_2_decimals(number: float) -> str:
    return f"{number:,.2f}"


def get_datetime_string():
    return datetime.now().strftime("%B-%d-%y %H:%M:%S")


def get_datetime_object():
    return pd.to_datetime(datetime.now())


def get_current_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def get_repo_root():
    """
    :return: Repo root (LucidMA folder)
    """
    try:
        repo_root = (
            subprocess.check_output(["git", "rev-parse", "--show-toplevel"])
            .decode("utf-8")
            .strip()
        )
        return Path(repo_root)
    except subprocess.CalledProcessError:
        print("Not a Git repository. Using the current directory as the root.")
        return Path.cwd()


def get_previous_business_day(
    current_date: datetime, holidays_df: pd.DataFrame
) -> datetime:
    """
    Calculate the previous business day given a date, excluding weekends and holidays.

    :param current_date: The date (datetime object) to calculate the previous business day from.
    :param holidays_df: DataFrame containing a 'date' column with holiday dates.
    :return: The previous business day as a datetime object.
    """
    if not isinstance(current_date, datetime):
        raise TypeError("current_date must be a datetime object.")
    if not isinstance(holidays_df, pd.DataFrame) or "date" not in holidays_df.columns:
        raise TypeError("holidays_df must be a pandas DataFrame with a 'date' column.")

    # Ensure holidays_df["date"] is of datetime.date type
    holidays = holidays_df["date"].dt.date

    previous_day = current_date.date() - timedelta(days=1)

    # Keep checking until we find a non-holiday, non-weekend day
    while previous_day.weekday() >= 5 or previous_day in holidays.values:
        previous_day -= timedelta(days=1)

    return previous_day
