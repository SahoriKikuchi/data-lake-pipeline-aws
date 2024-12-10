import sys
sys.path.append('s3://aws-glue-assets-538763555827-sa-east-1/dependencies.zip')
import numpy as np
import pandas as pd

def clean_data(x):
    if isinstance(x, str):
        if x.replace(',', '').replace('.', '').replace('-', '').isdigit(): 
            parts = x.split(',')
            if len(parts) > 1:
                x = parts[0] + '.' + ''.join(parts[1:])
    return x

#CEPEA ANUAL - BATATA
read_file = pd.read_excel("s3://raw-data-harvestlake/CEPEA_anual_batata-1994-2023.xlsx")
read_file.to_csv("s3://processed-data-harvestlake/CEPEA_anual_batata-1994-2023/CEPEA_anual_batata-1994-2023.csv", index = None, header = True)
df = pd.DataFrame(pd.read_csv("s3://processed-data-harvestlake/CEPEA_anual_batata-1994-2023/CEPEA_anual_batata-1994-2023.csv"))

#CEPEA MENSAL - BATATA
read_file = pd.read_excel("s3://raw-data-harvestlake/CEPEA_mensal_batata-1994-2023.xlsx")
read_file.to_csv("s3://processed-data-harvestlake/CEPEA_mensal_batata-1994-2023/CEPEA_mensal_batata-1994-2023.csv", index = None, header = True)
df = pd.DataFrame(pd.read_csv("s3://processed-data-harvestlake/CEPEA_mensal_batata-1994-2023/CEPEA_mensal_batata-1994-2023.csv"))

#COBAB CUSTO PRODUÇÃO - BATATA INGLESA
read_file = pd.read_excel("s3://raw-data-harvestlake/CONAB_custoProd_batataInglesa-2012-2023.xlsx")
read_file.to_csv("s3://processed-data-harvestlake/CONAB_custoProd_batataInglesa-2012-2023/CONAB_custoProd_batataInglesa-2012-2023.csv", index = None, header = True)
df = pd.DataFrame(pd.read_csv("s3://processed-data-harvestlake/CONAB_custoProd_batataInglesa-2012-2023/CONAB_custoProd_batataInglesa-2012-2023.csv"))

#IBGE SIDRA TABELA 1001 - BATATA INGLESA
def is_table_1001(sheet_name, excel_file): 
    first_row = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=1, header=None)
    return first_row.iloc[0, 0].startswith("Tabela 1001")

excel_file = pd.ExcelFile("s3://raw-data-harvestlake/IBGE_tabela1001.xlsx")
filtered_dfs = {}

for sheet_name in excel_file.sheet_names:
    if is_table_1001(sheet_name, excel_file):
        df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=4)

        if 'Unnamed: 0' in df.columns:
            df = df.rename(columns={'Unnamed: 0': 'Região'})

        years_row = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=3, nrows=1, header=None)
        years = years_row.iloc[0, 1:].values

        years = [int(year) for year in years if not pd.isna(year)]
        
        new_columns = ['Região']
        for i, year in enumerate(years):
            new_columns.extend([
                f'Total ({year})', 
                f'Batata-inglesa - 1ª safra ({year})', 
                f'Batata-inglesa - 2ª safra ({year})', 
                f'Batata-inglesa - 3ª safra ({year})'
            ])

        df.columns = new_columns[:len(df.columns)]

        df.replace(['-', '...', '..'], np.nan, inplace=True)

        for col in df.columns[1:]:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        filtered_dfs[sheet_name] = df

if filtered_dfs:
    for sheet_name, df in filtered_dfs.items():

        csv_filename = f"s3://processed-data-harvestlake/{sheet_name}_IBGE_tabela1001/IBGE_tabela1001_{sheet_name}.csv"
        
        df.to_csv(csv_filename, index=False, header=True, float_format='%.6f')
