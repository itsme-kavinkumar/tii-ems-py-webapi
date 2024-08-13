#
import pandas as pd


file_path = 'D:\\thowffic\\TI-EMS\\src\\endpoints\\attachments\\availability_report.xlsx'

df = pd.read_excel(file_path)

# Perform operations on the DataFrame if needed
# For example, print the first few rows
print(df.head())