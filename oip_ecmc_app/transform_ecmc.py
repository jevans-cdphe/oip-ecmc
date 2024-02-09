import pathlib
from typing import List

import polars as pl
import spock

import oip_ecmc.logger as lgr
import oip_ecmc.utils as utils


DESCRIPTION = '''
This script accepts parquet files made using the included convert_access_to_parquet.py script and transforms them into the data format that Ben Hmiel used at the start of this project.
'''


class OutputType(utils.StrEnum):
    '''
    Currently only csv is supported
    '''
    csv = 'csv'
    # ipc = 'ipc'
    # parquet = 'parquet'
    # avro = 'avro'
    # excel = 'excel'


@spock.spock
class Config:
    log_level: utils.LogLevel = utils.LogLevel.INFO
    ecmc_data_path: str = '~/Documents/ecmc/'
    parquet_directory: str = 'ECMC parquet'
    log_directory: str = 'logs'
    output_type: OutputType = OutputType.csv
    production_columns_to_keep: List[str] = [
        'name',
        'operator_num',
        'API_num',
        'Prod_days',
        'gas_btu_sales',
        'gas_sales',
        'gas_shrinkage',
        'gas_used_on_lease',
        'flared_vented',
        'oil_adjustment',
        'oil_gravity',
        'oil_sales',
        'gas_prod',
        'oil_prod',
        'water_prod',
    ]
    completions_columns_to_keep: List[str] = [
        'name',
        'operator_num',
        'facility_name',
        'facility_num',
        'well_name',
        'API_num',
        'well_bore_status',
        'county',
        'lat',
        'long',
        'first_prod_date',
        'gas_type',
    ]
    production_columns_to_fill_null_with_zero: List[str] = [
        'gas_btu_sales',
        'gas_sales',
        'gas_shrinkage',
        'gas_used_on_lease',
        'flared_vented',
        'oil_adjustment',
        'oil_gravity',
        'oil_sales',
        'gas_prod',
        'oil_prod',
        'water_prod',
    ]
    completions_columns_to_fill_null_with_zero: List[str] = [
        'gas_type',
    ]


def main() -> None:
    config = spock.SpockBuilder(Config, desc=DESCRIPTION).generate()

    log_path = utils.get_dir_path(config.Config.ecmc_data_path, config.Config.log_directory)
    logger = lgr.get_logger(
        'transform_ecmc', config.Config.log_level, log_path + 'transform_ecmc.jsonl')

    parquet_dir = utils.get_dir_path(config.Config.ecmc_data_path, config.Config.parquet_directory)
    parquet_files = list(pathlib.Path(parquet_dir).glob('*.parquet'))
    data = parse_filenames(parquet_files)

    production_keep = config.Config.production_columns_to_keep
    production_fillnull = config.Config.production_columns_to_fill_null_with_zero
    completions_keep = config.Config.completions_columns_to_keep
    completions_fillnull = config.Config.completions_columns_to_fill_null_with_zero

    for year in data['production']:
        query = (
            pl.scan_parquet(data['production'][year]['filename'])
            # build API_num column
            .with_columns(
                pl.concat_str(
                    [
                        pl.lit('05'),
                        pl.col('api_county_code').str.zfill(3),
                        pl.col('api_seq_num').str.zfill(5),
                        pl.col('sidetrack_num').str.zfill(2),
                    ],
                    separator='-',
                ).alias('API_num')
            )
            # keep only wanted columns
            .select(pl.col(*production_keep))
            # drop duplicates
            .unique()
            # replace null with 0
            .with_columns(*[
                pl.col(col).fill_null(strategy='zero')
                for col in production_fillnull
            ])
            # Calculate BOE from gas and oil production, assming 1BOE = 6MCF.  We can refine this later
            .with_columns(
                (pl.col('oil_prod') + pl.col('gas_prod') / 6).alias('boe_prod')
            )
            # Calculate BOEd using daily stats
            .with_columns(
                (pl.col('boe_prod') / pl.col('Prod_days')).alias('BOEd')
            )

            #################################################################################################
            # Ben's group_by
            .group_by('API_num')
            .agg([
                *[pl.col(c).sum() for c in [
                    *production_fillnull,
                    'Prod_days',
                    'boe_prod',
                    'BOEd',
                ]],
                *[pl.col(c).first() for c in [
                    'name',
                    'operator_num',
                ]],
            ])
            # Calculate GOR (MCF/bbl)
            # https://en.wikipedia.org/wiki/Gas/oil_ratio
            ## RECHECK this to make sure flared/vented is appropriately considered in
            ## calculating GOR
            .with_columns(
                (pl.col('gas_prod') / pl.col('oil_prod')).alias('GOR')
            )
            # calculate well type
            .with_columns(
                pl.when(pl.col('boe_prod') == 0)
                .then(pl.lit('Inactive'))
                .when(pl.col('oil_prod') == 0, pl.col('gas_prod') > 0)
                .then(pl.lit('Coal Bed Methane'))
                .when(pl.col('GOR') <= 0.3)
                .then(pl.lit('Heavy Oil'))
                .when(pl.col('GOR') <= 100)
                .then(pl.lit('Light Oil'))
                .when(pl.col('GOR') <= 1000)
                .then(pl.lit('Wet Gas'))
                .otherwise(pl.lit('Dry Gas'))
                .alias('well_type')
            )
        )

        data['production'][year]['dataframe'] = query.collect()

    for year in data['completions']:
        query = (
            pl.scan_parquet(data['completions'][year]['filename'])
            # remove unneeded columns
            .select(pl.col(*completions_keep))
            # drop duplicates
            .unique()
            # replace null with 0
            .with_columns(*[
                pl.col(col).fill_null(strategy='zero')
                for col in completions_fillnull
            ])
        )

        data['completions'][year]['dataframe'] = query.collect()

    for _, year_dict in data['production'].items():
        (
            year_dict['dataframe']
            .join(data['completions'][max(data['completions'])]['dataframe'], on='API_num')
            .write_csv(str(year_dict['filename'])[:-8] + '.csv' )
        )


def parse_filenames(parquet_files: list[str]) -> dict[str, dict]:
    to_return = {}

    for f in parquet_files:
        without_extension = str(f)[:-8]
        parts = without_extension.split('_')
        table = parts[0]
        year = int(parts[1])
        key = table.split(' ')[-1].lower()

        if key not in to_return:
            to_return[key] = {}
        
        to_return[key][year] = {'table': table, 'filename': f}

    return to_return


if __name__ == '__main__':
    main()