
import pandas as pd
import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine


# excel_file_path = r'C:\Users\Tony.Hoang\OneDrive - Lucid Management and Capital Partne\Desktop\Demo.xlsx'
#
# df = pd.read_excel(excel_file_path)  # Read the Excel file

# Database connection parameters
db_endpoint = 'luciddb1.czojmxqfrx7k.us-east-1.rds.amazonaws.com'
db_port = '5432'
db_user = 'dbmasteruser'
db_password = 'lnRz*(N_7aOf~7Hx6oRo8;,<vYp|~#PC'
db_name = 'reporting'


# Create the database URL
database_url = f"postgresql://{db_user}:{db_password}@{db_endpoint}:{db_port}/{db_name}"

# Create a database engine
engine = create_engine(database_url)

# Specify your table name and schema
table_name = 'transactions_raw_v2'

# Read the table into a pandas DataFrame
df = pd.read_sql_table(table_name, con=engine)

### DATE CONVERSION ###
# Splitting the 'Period' column into 'Start_date' and 'End_date'
df[['Start_date', 'End_date']] = df['PeriodDescription'].str.extract(
    r'From (\d{1,2}/\d{1,2}/\d{4}) To (\d{1,2}/\d{1,2}/\d{4})')

# Converting to date format
df['Start_date'] = pd.to_datetime(df['Start_date'], format='%m/%d/%Y')
df['End_date'] = pd.to_datetime(df['End_date'], format='%m/%d/%Y')

### GROUP BY ###

### TRANSACTION MAP ###
transaction_map = {
    'ASSIGN (BEG)': 'Beginning Cap Acct Bal',
    'BAL FWD':'Beginning Cap Acct Bal',
    'CE AD COST':'Expense',
    'CE ADMIN': 'Expense',
    'CE AUDIT':'Expense',
    'CE CUST FE': 'Expense',
    'CE FCE':'Expense',
    'CE HED FEE': 'Expense',
    'CE IF':'Mgmt Fee Waiver',
    'CE IIEA': 'Income',
    'CE IIEM':'Income',
    'CE IIF': 'Income',
    'CE IIM':'Income',
    'CE IIMM': 'Income',
    'CE IIR':'Income',
    'CE MF': 'Mgmt Fee',
    'CE MFW':'Mgmt Fee Waiver',
    'CE MREAL': 'Mark to Market',
    'CE MUNREAL':'Mark to Market',
    'CE ORG EXP': 'Expense',
    'CE OTH FEE':'Expense',
    'CE RADJEXP': 'Expense',
    'CE RADJINC':'Income',
    'CONT': 'Beginning Cap Acct Bal',
    'Total':'Ending Cap Acct Bal',
    'WITH (BEG)':'Beginning Cap Acct Bal',
    'WITH (END)': 'Ending Cap Acct Bal',
}

df['Transaction_category'] = df['Head1'].apply(lambda x: transaction_map.get(x, 'Unmapped / Others'))
df['Amount'] = df['Amt1'].astype(float)

# Step 1: Filter the DataFrame for the specified date range
df = df[(df['Start_date'] >= pd.Timestamp('2023-01-01')) & (df['End_date'] <= pd.Timestamp('2023-12-31')) & (df['PoolCode'] == 'GEN-LUCIDII') & (df['InvestorCode'] == '1000068425')]
subset_cols = ['PoolDescription', 'PeriodDescription', 'Start_date', 'End_date', 'InvestorDescription', 'Head1', 'Transaction_category', 'Amount']
deduplicated_df = df.drop_duplicates(subset=subset_cols)
deduplicated_df = deduplicated_df.groupby(subset_cols[:-1])['Amount'].sum().reset_index()

# Pivot the DataFrame
pivot_df = deduplicated_df.pivot_table(index=['PoolDescription', 'InvestorDescription', 'Start_date', 'End_date'], columns='Transaction_category', values='Amount', fill_value=0)

print(pivot_df.columns)
# Rename the columns if necessary, e.g., pivot_df.rename(columns={'BAL FWD': 'Balance Forwarded'})

# Export the DataFrame to Excel
file_path = r"C:\Users\Tony.Hoang\OneDrive - Lucid Management and Capital Partne\Desktop\Test.xlsx"
pivot_df.to_excel(file_path, index=False, engine='openpyxl')

# Display the first few rows of the resulting DataFrame
with pd.option_context('display.max_columns', None, 'display.width', 1000, 'display.float_format', '{:.2f}'.format):
    print(pivot_df)