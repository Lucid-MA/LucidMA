### TRANSACTION MAP ###
transaction_map = {
    "ASSIGN (BEG)": "Beginning Cap Acct Bal",
    "BAL FWD": "Beginning Cap Acct Bal",
    "CE AD COST": "Expense",
    "CE ADMIN": "Expense",
    "CE AUDIT": "Expense",
    "CE CUST FE": "Expense",
    "CE FCE": "Expense",
    "CE HED FEE": "Expense",
    "CE IF": "Mgmt Fee Waiver",
    "CE IIEA": "Income",
    "CE IIEM": "Income",
    "CE IIF": "Income",
    "CE IIM": "Income",
    "CE IIMM": "Income",
    "CE IIR": "Income",
    "CE MF": "Mgmt Fee",
    "CE MFW": "Mgmt Fee Waiver",
    "CE MREAL": "Mark to Market",
    "CE MUNREAL": "Mark to Market",
    "CE ORG EXP": "Expense",
    "CE OTH FEE": "Expense",
    "CE RADJEXP": "Expense",
    "CE RADJINC": "Income",
    "CONT": "Contribution",
    "Total": "Ending Cap Acct Bal",
    "WITH (BEG)": "Withdrawal - BOP",
    "WITH (END)": "Withdrawal - EOP",
}

# Define the columns needed for the Bronze returns table
needed_columns = [
    "SK",
    "VehicleCode",
    "VehicleDescription",
    "PoolCode",
    "PoolDescription",
    "Period",
    "PeriodDescription",
    "InvestorCode",
    "InvestorDescription",
    "Head1",
    "Amt1",
]

master_data_return_column_order = [
    "Start Date", "End Date", "Starting Cap Accounts", "End Cap Accounts",
    "Admin NAV Strike Date", "Subscriptions", "Withdrawals", "Gross Return",
    "Net Return (Act/360)", "Net Return (Act/365)",
    "Principal Outstanding", "Interest Payment Date", "Interest Paid", "Benchmark",
    "Benchmark return", "Outperform", "Target Outperform", "1m SOFR",
    "1m T-Bills", "US Gov't & AAA (%)", "AA to A (%)",
    "BBB (%)", "HY (%)", "A1/P1 CP", "T-Bills /Govt MMF", "Total OC Rate", "US Gov't & AAA",
    "AA to A", "BBB", "HY", "Benchmark File", "Comments"
]
