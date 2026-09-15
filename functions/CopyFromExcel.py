import pandas as pd
from sqlalchemy import create_engine

# Load the Excel file
excel_file = 'C:\\Users\\user\\Desktop\\Справочники\\tab_formula_koef_d816_4.xlsx'
df = pd.read_excel(excel_file)

# Create a connection to PostgreSQL
engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5433/msb_zuv_input_data')

# Insert the data into the PostgreSQL table
df.to_sql('tab_formula_koef_d816_4', engine, if_exists='replace', index=False)