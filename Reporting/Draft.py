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


report_data_fund = {
    "report_date": report_date,  # done
    "fundname": fund_name,  # done
    "toptableextraspace": "5.5em",
    "series_abbrev": series_abbrev,
    "port_limit": ("Quarterly" if "Q" in series_abbrev else "Monthly"),
    "seriesname": series_name,
    "fund_description": fund_description,
    "series_description": series_description,
    "benchmark": benchmark_name,  # done
    "tgt_outperform": target_outperform_range,  # done
    "exp_rat_footnote": expense_ratio_footnote_text,
    "prev_pd_start": pd.to_datetime(curr_start).strftime("%B %d, %Y"),  # done
    "this_pd_start": pd.to_datetime(next_start).strftime("%B %d, %Y"),  # done
    "prev_pd_return": prev_return,  # done
    "prev_pd_benchmark": benchmark_short,  # done
    "prev_pd_outperform": prev_target_outperform,  # done
    "this_pd_end": pd.to_datetime(next_end).strftime("%B %d, %Y"),  # done
    "this_pd_est_return": current_target_return,  # done
    "this_pd_est_outperform": target_outperform_net,  # done
    "benchmark_short": benchmark_short,  # done
    "interval1": month_wordify(interval_tuple[0]),  # TODO: review this
    "interval2": month_wordify(interval_tuple[1]),  # TODO: review this
    "descstretch": stretches(
        df_attributes["fund_description"].iloc[0]
        + df_attributes["series_description"].iloc[0]
    )[0],
    "pcompstretch": stretches(
        df_attributes["fund_description"].iloc[0]
        + df_attributes["series_description"].iloc[0]
    )[1],
    "addl_coll_breakdown": addl_coll_breakdown(
        form_as_percent(aloc_aa_a, 1) if fund_name != "USG" else "n/a",
        form_as_percent(oc_aa_a, 1) if fund_name != "USG" else "n/a",
        form_as_percent(aloc_bbb, 1) if fund_name != "USG" else "n/a",
        form_as_percent(oc_bbb, 1) if fund_name != "USG" else "n/a",
        form_as_percent(0, 1) if fund_name != "USG" else "n/a",
        form_as_percent(0, 1),
    ),
    "oc_aaa": form_as_percent(oc_usg_aaa, 2),  # TODO: review
    "oc_tbills": "-",
    "oc_total": form_as_percent(oc_total, 2),  # TODO: review
    "usg_aaa_cat": (
        "US Govt Repo" if fund_name == "USG" else "US Govt/AAA Repo"
    ),  # done
    "alloc_aaa": form_as_percent(aloc_usg_aaa, 2),  # TODO: review
    "alloc_tbills": form_as_percent(aloc_tbills, 2),  # TODO: review
    "alloc_total": form_as_percent(1, 1),  # TODO: review
    "tablevstretch": tablevstretch(fund_name),  # done
    # "return_table_plot": "\n\t\\textbf{Lucid USG - Series M}                    & \\textbf{5.55\\%}                              & \\textbf{-}                                  & \\textbf{5.55\\%}                               & \\textbf{-}                           & \\textbf{5.55\\%}                             & \\textbf{-}                          \\\\\n1m T-Bills                       & 5.55\\%                                       & \\textbf{+16 bps}                            & 5.55\\%                               & \\textbf{+17 bps}                     & 5.55\\%                              & \\textbf{+16 bps}                    \\\\\nCrane Govt MM Index                       & 5.55\\%                                       & \\textbf{+43 bps}                           & 5.55\\%                               & \\textbf{+43 bps}                     & 5.55\\%                              & \\textbf{+40 bps}                    \\\\ \\arrayrulecolor{light_grey}\\hline\n\t",
    "return_table_plot": return_table_plot(
        fund_name=fund_name,  # done
        prev_pd_return=prev_return,
        series_abbrev=series_abbrev,
        r_this_1=r_this_1,
        r_this_2=r_this_2,
        comp_a=benchmark_to_use[0],
        comp_b=benchmark_to_use[1],
        comp_c=benchmark_to_use[2],
        r_a=r_a,
        r_b=r_b,
        r_c=r_c,
        s_a_0=s_a_0,
        s_a_1=s_a_1,
        s_a_2=s_a_2,
        s_b_0=s_b_0,
        s_b_1=s_b_1,
        s_b_2=s_b_2,
        s_c_0=s_c_0,
        s_c_1=s_c_1,
        s_c_2=s_c_2,
    ),
    "fund_size": get_fund_size(fund_name.upper(), report_date),  # TODO: update database
    "series_size": get_series_size(
        reporting_series_id, report_date
    ),  # TODO: update database
    "lucid_aum": wordify_aum(lucid_aum),  # TODO: update database
    "rating": df_attributes["rating"].iloc[0],  # done
    "rating_org": df_attributes["rating_org"].iloc[0],  # done
    "calc_frequency": "Monthly at par",  # done
    "next_withdrawal_date": pd.to_datetime(next_withdrawal).strftime(
        "%B %d, %Y"
    ),  # done
    "next_notice_date": pd.to_datetime(next_notice).strftime("%B %d, %Y"),  # done
    "min_invest": wordify(df_attributes["minimum_investment"].iloc[0]),  # done
    "wal": wal,
    "legal_fundname": df_attributes["legal_fund_name"].iloc[0],  # done
    "fund_inception": df_attributes["fund_inception"]
    .iloc[0]
    .strftime("%B %d, %Y"),  # done
    "series_inception": df_attributes["series_inception"]
    .iloc[0]
    .strftime("%B %d, %Y"),  # done
    "performance_graph": performance_graph_data,
}
