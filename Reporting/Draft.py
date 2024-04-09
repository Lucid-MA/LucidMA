from Utils.Common import print_df
from Utils.Hash import hash_string
from Utils.database_utils import read_table_from_db

print(hash_string("78637EAD82024-04-022"), hash_string("78637EAD82024-04-021"))

df_price = read_table_from_db("daily_price","postgres")
df_price = df_price[(df_price['Price_date'] == '2024-04-01')]

# 252122726095011
# 221905004109302

# Create 'Price_ID_PM' column
df_price['Price_ID_AM'] = df_price.apply(lambda row: hash_string(f"{row['Bond_ID']}{row['Price_date'].strftime('%Y-%m-%d')}" + "1"), axis=1)

# Filter df_price where 'Price_ID' equals 'Price_ID_PM'
df_price_pm = df_price[df_price['Price_ID'].astype(float) != df_price['Price_ID_AM'].astype(float)]




