# from datetime import datetime
#
# from Utils.database_utils import read_table_from_db, get_database_engine
#
# # Table names
# db_type = "postgres"
# attributes_table_name = "bronze_series_attributes"
# historical_returns_table_name = "historical_returns"
# target_return_table_name = "target_returns"
# benchmark_table_name = "bronze_benchmark"
# oc_rate_table_name = "oc_rates"
# daily_nav_table_name = "bronze_daily_nav"
# roll_schedule_table_name = "roll_schedule"
# cash_balance_table_name = "bronze_cash_balance"
# benchmark_comparison_table_name = "silver_return_by_series"
#
# # Connect to the PostgreSQL database
# engine = get_database_engine("postgres")
#
# # Read the table into a pandas DataFrame
# df_attributes = read_table_from_db(attributes_table_name, db_type)
#
# df_historical_returns = read_table_from_db(historical_returns_table_name, db_type)
#
# df_target_return = read_table_from_db(target_return_table_name, db_type)
#
# df_benchmark = read_table_from_db(benchmark_table_name, db_type)
#
# df_oc_rates = read_table_from_db(oc_rate_table_name, db_type)
#
# df_daily_nav = read_table_from_db(daily_nav_table_name, db_type)
#
# df_roll_schedule = read_table_from_db(roll_schedule_table_name, db_type)
#
# df_cash_balance = read_table_from_db(cash_balance_table_name, db_type)
#
# df_benchmark_comparison = read_table_from_db(benchmark_comparison_table_name, db_type)
#
# df_historical_returns_plot = read_table_from_db(historical_returns_table_name, db_type)
#
#
# def calculate_oc_metrics(data):
#     global cash_balance
#
#     total_investment = data["investment_amount"].sum() + cash_balance
#
#     def get_values(rating):
#         if rating in data["rating_buckets"].values:
#             row = data[data["rating_buckets"] == rating].iloc[0]
#             return row["collateral_mv_allocated"], row["investment_amount"]
#         return 0, 0
#
#     col_mv_allocated_aaa, inv_aaa = get_values("AAA")
#     col_mv_allocated_aa, inv_aa = get_values("AA")
#     col_mv_allocated_a, inv_a = get_values("A")
#     col_mv_allocated_bbb, inv_bbb = get_values("BBB")
#     col_mv_allocated_usg, inv_usg = get_values("USG")
#     col_mv_allocated_usgcmo, inv_usgcmo = get_values("USGCMO")
#
#     oc_total = data["collateral_mv_allocated"].sum() / data["investment_amount"].sum()
#
#     oc_usg_aaa = (
#         (col_mv_allocated_aaa + col_mv_allocated_usg + col_mv_allocated_usgcmo)
#         / (inv_aaa + inv_usg + inv_usgcmo)
#         if (inv_aaa + inv_usg + inv_usgcmo) != 0
#         else 0
#     )
#     oc_aa_a = (
#         (col_mv_allocated_aa + col_mv_allocated_a) / (inv_aa + inv_a)
#         if (inv_aa + inv_a) != 0
#         else 0
#     )
#     oc_bbb = col_mv_allocated_bbb / inv_bbb if inv_bbb != 0 else 0
#     oc_tbills = 0
#
#     aloc_usg_aaa = (inv_aaa + inv_usg + inv_usgcmo) / total_investment
#     aloc_aa_a = (inv_aa + inv_a) / total_investment
#     aloc_bbb = inv_bbb / total_investment
#     aloc_tbills = cash_balance / total_investment
#
#     return (
#         oc_total,
#         oc_usg_aaa,
#         oc_aa_a,
#         oc_bbb,
#         oc_tbills,
#         aloc_usg_aaa,
#         aloc_aa_a,
#         aloc_bbb,
#         aloc_tbills,
#     )
#
#
# def form_as_percent(val, rnd):
#     try:
#         if float(val) == 0:
#             return "-"
#         return ("{:." + str(rnd) + "f}").format(100 * val) + "\\%"
#     except:
#         return "n/a"
#
#
# reporting_series = [
#     "PRIME-C10",
#     "PRIME-M00",
#     "PRIME-MIG",
#     # "PRIME-Q10",
#     # "PRIME-Q36",
#     # "PRIME-QX0",
#     "USGFD-M00",
# ]
#
# current_date = datetime.strptime("2024-06-12", "%Y-%m-%d")
# report_date_formal = current_date.strftime("%B %d, %Y")
# report_date = current_date.strftime("%Y-%m-%d")
#
# for reporting_series_id in reporting_series:
#     fund_attribute_condition = df_attributes["security_id"] == reporting_series_id
#     print(reporting_series_id)
#     df_attributes_tmp = df_attributes[fund_attribute_condition]
#     fund_name = df_attributes_tmp["fund_name"].iloc[0]
#     series_name = df_attributes_tmp["series_name"].iloc[0]
#
#     cash_balance_condition = (
#         (df_cash_balance["Fund"] == fund_name.upper())
#         & (df_cash_balance["Series"] == series_name.upper().replace(" ", ""))
#         & (df_cash_balance["Balance_date"] == report_date)
#         & (df_cash_balance["Account"] == "MAIN")
#     )
#
#     df_cash_balance_tmp = df_cash_balance[cash_balance_condition]
#     cash_balance = df_cash_balance_tmp["Sweep_Balance"].iloc[0]
#
#     oc_rate_condition = (
#         (df_oc_rates["fund"] == fund_name.upper())
#         & (df_oc_rates["series"] == series_name.upper().replace(" ", ""))
#         & (df_oc_rates["report_date"] == report_date)
#     )
#     df_oc_rates_tmp = df_oc_rates[oc_rate_condition]
#
#     (
#         oc_total,
#         oc_usg_aaa,
#         oc_aa_a,
#         oc_bbb,
#         oc_tbills,
#         aloc_usg_aaa,
#         aloc_aa_a,
#         aloc_bbb,
#         aloc_tbills,
#     ) = calculate_oc_metrics(df_oc_rates_tmp)
#
#     print(f"oc_total = {form_as_percent(oc_total,2)}")
#     print(f"oc_usg_aaa = {form_as_percent(oc_usg_aaa,2)}")
#     print(f"oc_aa_a = {form_as_percent(oc_aa_a,2)}")
#     print(f"oc_bbb = {form_as_percent(oc_bbb,2)}")
#     print(f"oc_tbills = {form_as_percent(oc_tbills,2)}")
#     print(f"aloc_usg_aaa = {form_as_percent(aloc_usg_aaa,2)}")
#     print(f"aloc_aa_a = {form_as_percent(aloc_aa_a,2)}")
#     print(f"aloc_bbb = {form_as_percent(aloc_bbb,2)}")
#     print(f"aloc_tbills = {form_as_percent(aloc_tbills,2)}")


import requests

# GitHub username and repository
username = "your-username"
repository = "your-repository"

# GitHub API endpoint for repository commits
url = f"https://api.github.com/repos/tonylucid/LucidMA/commits"
headers = {"Accept": "application/vnd.github.v3+json"}

# Calculate the date from the beginning of 2024
since_date = "2024-01-01T00:00:00Z"

# Initialize counters
total_added = 0
total_removed = 0

# Loop to handle pagination and fetch all commits
# Loop to handle pagination and fetch all commits
while url:
    response = requests.get(url, headers=headers, params={"since": since_date})
    commits = response.json()

    # Check if the response is a list (which it should be)
    if isinstance(commits, list):
        for commit in commits:
            if isinstance(commit, dict) and "url" in commit:
                commit_url = commit["url"]
                commit_response = requests.get(commit_url, headers=headers)
                commit_data = commit_response.json()

                if "files" in commit_data:
                    for file in commit_data["files"]:
                        if "additions" in file:
                            total_added += file["additions"]
                        if "deletions" in file:
                            total_removed += file["deletions"]
            else:
                print(f"Unexpected commit format: {commit}")
    else:
        print(f"Unexpected response format: {commits}")
        break

    # Check for pagination
    if "next" in response.links:
        url = response.links["next"]["url"]
    else:
        break

print(f"Total lines added: {total_added}")
print(f"Total lines removed: {total_removed}")
print(f"Net lines of code: {total_added - total_removed}")
