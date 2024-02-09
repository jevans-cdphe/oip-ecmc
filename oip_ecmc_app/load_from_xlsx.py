import os

import polars as pl

############################################ CONSTANTS ############################################
OIP_DRIVE = 'X:/Shared drives/Office of Innovation Shared Drive/'
#SAMPLE_DATA_DIR = OIP_DRIVE + 'ECMC Data and Projects/Production Data/Sample Data'
SAMPLE_DATA_DIR = './Sample Data/'

XLSX_FILE = SAMPLE_DATA_DIR + '/2020-2022 Annual Data compiled from Access DBs.xlsx'
YEARS_XLSX = [2020, 2021, 2022]
XLSX_SHEETS = {
    'production': {year: f'{year} Prod' for year in YEARS_XLSX},
    'completions': '2022 Completions',
    'data filter': 'data_filter',
}

PRODUCTION_FILES = {year: SAMPLE_DATA_DIR + f'/production_{year}.parquet' for year in YEARS_XLSX}
COMPLETIONS_FILE = SAMPLE_DATA_DIR + '/completions.parquet'
DATA_FILTER_FILE = SAMPLE_DATA_DIR + '/data filter.parquet'
########################################### /CONSTANTS ############################################

# check for parquet files
parquets_exist = False
for year in YEARS_XLSX:
    if not os.path.exists(PRODUCTION_FILES[year]):
        break
else:
    if os.path.exists(COMPLETIONS_FILE) and os.path.exists(DATA_FILTER_FILE):
        parquets_exist = True

if not parquets_exist:
    # calamine is VERY fast, but requires defined datatypes. openpyxl is what
    # Pandas uses; it's slow but it allows for automatic type inference, which
    # is necessary for some sheets.
    print('loading data from xlsx file...', end='', flush=True)
    xlsx_sample_data = {
        'production': {
            year: pl.read_excel(
                XLSX_FILE, sheet_name=XLSX_SHEETS['production'][year], engine='calamine')
            for year in YEARS_XLSX
        },
        'completions': pl.read_excel(
            XLSX_FILE, sheet_name=XLSX_SHEETS['completions'], engine='openpyxl'),
        'data filter': pl.read_excel(
            XLSX_FILE, sheet_name=XLSX_SHEETS['data filter'], engine='calamine'),
    }
    print('done')

    # write parquet files
    print('writing data to parquet files...', end='', flush=True)
    for year, df in xlsx_sample_data['production'].items():
        df.write_parquet(PRODUCTION_FILES[year])
    xlsx_sample_data['completions'].write_parquet(COMPLETIONS_FILE)
    xlsx_sample_data['data filter'].write_parquet(DATA_FILTER_FILE)
    print('done')