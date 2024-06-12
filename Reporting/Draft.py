from Utils.database_utils import read_table_from_db

#
# file_path = "/Volumes/Sdrive$/Users/THoang/Data/Calculating lag rate.xlsx"
#
# df = pd.read_excel(file_path)
#
# Convert 'Start Date' and 'End Date' columns to datetime format
df["Start Date"] = pd.to_datetime(df["start_date"])
df["End Date"] = pd.to_datetime(df["end_date"])

# Calculate the actual number of days between start and end dates
df["Day count"] = (df["End Date"] - df["Start Date"]).dt.days

# Calculate the accrual period return
df["Accrual period return"] = df["annualized_return"].astype("float") * (
    df["Day count"] / 360
)

# Calculate the growth of $1
df["Growth of $1"] = (1 + df["Accrual period return"]).cumprod()

# Initialize the '3-month growth rate' and 'Annualized 3-month growth rate' columns
df["3-month"] = pd.Series([None] * len(df))
df["Annualized 3-month"] = pd.Series([None] * len(df))

df["12-month"] = pd.Series([None] * len(df))
df["Annualized 12-month"] = pd.Series([None] * len(df))

n = 3
# Calculate the 3-month growth rate and annualized 3-month growth rate
for i in range(len(df)):
    if i >= n:
        start_date = df.iloc[i - n]["End Date"]
        end_date = df.iloc[i]["End Date"]
        growth_rate = df.iloc[i]["Growth of $1"] / df.iloc[i - n]["Growth of $1"] - 1
        df.at[i, "3-month"] = growth_rate

        # Calculate the actual number of days in the 3-month period
        days_in_period = (end_date - start_date).days

        # Calculate the annualized 3-month growth rate
        annualized_growth_rate = growth_rate * (360 / days_in_period)
        df.at[i, "Annualized 3-month"] = annualized_growth_rate

    elif i == (n - 1):
        # Calculate the growth rate and annualized growth rate from the start of the data
        start_date = df.iloc[0]["Start Date"]
        end_date = df.iloc[i]["End Date"]
        growth_rate = df.iloc[i]["Growth of $1"] - 1
        df.at[i, "3-month"] = growth_rate

        # Calculate the actual number of days from the start of the data
        days_in_period = (end_date - start_date).days

        # Calculate the annualized growth rate from the start of the data
        annualized_growth_rate = growth_rate * (360 / days_in_period)
        df.at[i, "Annualized 3-month"] = annualized_growth_rate


n = 12
for i in range(len(df)):
    if i >= n:
        start_date = df.iloc[i - n]["End Date"]
        end_date = df.iloc[i]["End Date"]
        growth_rate = df.iloc[i]["Growth of $1"] / df.iloc[i - n]["Growth of $1"] - 1
        df.at[i, "12-month"] = growth_rate

        # Calculate the actual number of days in the 3-month period
        days_in_period = (end_date - start_date).days

        # Calculate the annualized 3-month growth rate
        annualized_growth_rate = growth_rate * (360 / days_in_period)
        df.at[i, "Annualized 12-month"] = annualized_growth_rate
    elif i == (n - 1):
        # Calculate the growth rate and annualized growth rate from the start of the data
        start_date = df.iloc[0]["Start Date"]
        end_date = df.iloc[i]["End Date"]
        growth_rate = df.iloc[i]["Growth of $1"] - 1
        df.at[i, "12-month"] = growth_rate

        # Calculate the actual number of days from the start of the data
        days_in_period = (end_date - start_date).days

        # Calculate the annualized growth rate from the start of the data
        annualized_growth_rate = growth_rate * (360 / days_in_period)
        df.at[i, "Annualized 12-month"] = annualized_growth_rate
# # Print the resulting DataFrame
# print_df(df[:60])


# Read the CSV file into a DataFrame
df = read_table_from_db("historical_returns", "postgres")


import pandas as pd

# Convert 'start_date' and 'end_date' columns to datetime
df["start_date"] = pd.to_datetime(df["start_date"])
df["end_date"] = pd.to_datetime(df["end_date"])

# Sort the DataFrame by 'start_date' and 'end_date'
df = df.sort_values(["start_date", "end_date"])
