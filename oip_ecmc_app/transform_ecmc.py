import json
import logging
import pathlib
from typing import List

import polars as pl
import spock

import oip_ecmc.logger as lgr
import oip_ecmc.utils as utils


DESCRIPTION = '''
This script accepts parquet files made using the included
convert_access_to_parquet.py script and transforms them into the data format
that Ben Hmiel used at the start of this project.
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
    log_level: lgr.LogLevel = lgr.LogLevel.INFO
    ecmc_data_path: str = '~/Documents/ecmc/'
    parquet_directory: str = 'ECMC parquet'
    output_directory: str = 'ECMC transformed data'
    log_directory: str = 'logs'
    output_type: OutputType = OutputType.csv
    remove_CO2_wells: bool = True
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

    ecmc_data_path = utils.str_to_path(config.Config.ecmc_data_path)

    logger = lgr.get_logger(
        'transform_ecmc',
        config.Config.log_level,
        ecmc_data_path / config.Config.log_directory,
    )

    parquet_dir = ecmc_data_path / config.Config.parquet_directory

    with (parquet_dir / 'metadata.json').open('r') as f:
        parquet_metadata = json.load(f)

    output_path = ecmc_data_path / config.Config.output_directory
    output_previous_versions_path = output_path / 'previous_versions'
    output_previous_versions_path.mkdir(parents=True, exist_ok=True)
        
    output_metadata = get_output_metadata(parquet_metadata, output_path, logger)
    output_metadata_path = output_path / 'metadata.json'

    if utils.new_hashes(output_metadata, output_metadata_path, logger=logger):
        utils.backup(
            output_path, output_previous_versions_path, 'csv', logger=logger)

        with output_metadata_path.open('w') as f:
            json.dump(utils.to_json(output_metadata, logger=logger), f)

        data = {'production': {}, 'completions': {}}
        for _, hash_dict in parquet_metadata.items():
            data['production'][hash_dict['year']] = transform_production(
                pathlib.Path(hash_dict['production_path']),
                config.Config.production_columns_to_keep,
                config.Config.production_columns_to_fill_null_with_zero,
                logger,
            )
            data['completions'][hash_dict['year']] = transform_completions(
                pathlib.Path(hash_dict['completions_path']),
                config.Config.completions_columns_to_keep,
                config.Config.completions_columns_to_fill_null_with_zero,
                logger,
            )

        write_output_data(data, output_path, logger)


def get_output_metadata(
    parquet_metadata: dict,
    output_path: pathlib.Path,
    logger: logging.Logger,
) -> dict:
    return {
        sha_hash: {
            'year': hash_dict['year'],
            'path': output_path / f'{hash_dict["year"]}.csv',
            'timestamp': hash_dict['timestamp'],
        }
        for sha_hash, hash_dict in parquet_metadata.items()
    }


def write_output_data(
    data: dict[int, pl.DataFrame],
    output_path: pathlib.Path,
    remove_co2_wells: bool,
    logger: logging.Logger,
) -> None:
    for year, df in data['production'].items():
        df_out = df.join(
            data['completions'][max(data['completions'])], on='API_num')
        if remove_co2_wells:
            df_out = df_out.filter(pl.col('Prod_days') == 0)
        df_out.write_csv(output_path / f'{year}.csv')


def transform_production(
    parquet_path: pathlib.Path,
    production_keep: list[str],
    production_fillnull: list[str],
    logger: logging.Logger,
) -> pl.DataFrame:
    return (
        pl.scan_parquet(parquet_path)
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
        # Calculate BOE from gas and oil production, assming 1BOE = 6MCF.
        # We can refine this later
        .with_columns(
            (pl.col('oil_prod') + pl.col('gas_prod') / 6).alias('boe_prod')
        )
        # Calculate BOEd using daily stats
        .with_columns(
            (pl.col('boe_prod') / pl.col('Prod_days')).alias('BOEd')
        )

        ####################################################################
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
        ## RECHECK this to make sure flared/vented is appropriately
        ## considered in calculating GOR
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
    ).collect()


def transform_completions(
    parquet_path: pathlib.Path,
    completions_keep: list[str],
    completions_fillnull: list[str],
    logger: logging.Logger,
) -> pl.DataFrame:
    return (
        pl.scan_parquet(parquet_path)
        # remove unneeded columns
        .select(pl.col(*completions_keep))
        # drop duplicates
        .unique()
        # replace null with 0
        .with_columns(*[
            pl.col(col).fill_null(strategy='zero')
            for col in completions_fillnull
        ])
    ).collect()


if __name__ == '__main__':
    main()