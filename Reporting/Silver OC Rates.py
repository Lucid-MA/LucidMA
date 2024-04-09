from Reporting.Utils.database_utils import read_table_from_db
from Utils.Common import print_df
from Utils.Hash import hash_string

db_type = "postgres"
table_name = "Bronze_OC_Rates_mini"

df_bronze = read_table_from_db(table_name, db_type)

print_df(df_bronze[df_bronze['Trade ID'].isin([
    153434,
    154130,
    153888,
    153894,
    153882,
    153876,
])])

# select price from table where price_date = '2024-04-01' and is_AM = 0
df_price = read_table_from_db("daily_price", "postgres")
df_price = df_price[(df_price['Price_date'] == '2024-04-01')]
df_price['Price_ID_AM'] = df_price.apply(
    lambda row: hash_string(f"{row['Bond_ID']}{row['Price_date'].strftime('%Y-%m-%d')}" + "1"), axis=1)

# Filter df_price where 'Price_ID' equals 'Price_ID_PM'
df_price = df_price[df_price['Price_ID'].astype(float) != df_price['Price_ID_AM'].astype(float)]

df_bronze = df_bronze.merge(df_price[['Bond_ID', 'Final_price']], left_on='BondID', right_on='Bond_ID', how='left')

# Rename 'Final_price' column to 'Price'
df_bronze.rename(columns={'Final_price': 'Price'}, inplace=True)

# Replace missing values in 'Price' column with 100
df_bronze['Price'].fillna(100, inplace=True)

# Drop the 'Bond_ID' column as it's no longer needed
df_bronze.drop(columns='Bond_ID', inplace=True)

df_factor = read_table_from_db('bronze_price_factor', 'postgres')
df_factor = df_factor[(df_factor['Factor_date'] == '2024-04-01')]

df_bronze = df_bronze.merge(df_factor[['Bond_ID', 'Factor']], left_on='BondID', right_on='Bond_ID', how='left')


def calculate_collateral_mv(row):
    """
    This function calculates the 'Collateral_MV' column.
    TODO: Review the formula when Factor == 0
    """
    if row['Factor'] == 0:
        return (row['Par/Quantity'] * row['Price'] * row['Factor'] / 100) + 0.001
    else:
        return row['Par/Quantity'] * row['Price'] * row['Factor'] / 100


df_bronze['Collateral_MV'] = df_bronze.apply(calculate_collateral_mv, axis=1)


print_df(df_bronze[df_bronze['Trade ID'].isin([
    153434,
    154130,
    153888,
    153894,
    153882,
    153876,
])])

# Group by 'Comments' and calculate the sum of 'Money' and sum of 'Collateral_MV'
df_result = df_bronze.groupby('Comments').agg({
    'Money': 'sum',
    'Collateral_MV': 'sum',

}).reset_index()
# Rename the 'Money' column to 'Investment Amount'
df_result = df_result.rename(columns={'Money': 'Investment Amount'})

print_df(df_result.head())
