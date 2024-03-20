import pandas as pd

excel_file_path = r'C:\Users\Tony.Hoang\OneDrive - Lucid Management and Capital Partne\Desktop\Demo.xlsx'

df = pd.read_excel(excel_file_path)  # Read the Excel file
print(df.dtypes)