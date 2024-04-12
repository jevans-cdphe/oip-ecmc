'''
This script accepts parquet files made using the included
convert_access_to_parquet.py script and transforms them into the data format
that Ben Hmiel used at the start of this project.
'''


import json
import logging
import pathlib

import polars as pl

from . import config as cfg
from . import utils


def transform(
    config: cfg.ProductionSummariesConfig,
    logger: logging.Logger,
) -> None:
    with (config.parquet_dir / 'metadata.json').open('r') as f:
        parquet_metadata = json.load(f)

    output_previous_versions_path = config.export_dir / 'previous_versions'
    output_previous_versions_path.mkdir(parents=True, exist_ok=True)
        
    output_metadata = _get_output_metadata(parquet_metadata, config.export_dir, logger)
    output_metadata_path = config.export_dir / 'metadata.json'

    if utils.new_hashes(output_metadata, output_metadata_path, logger=logger):
        utils.backup(
            config.export_dir, output_previous_versions_path, 'csv', logger=logger)

        with output_metadata_path.open('w') as f:
            json.dump(utils.to_json(output_metadata, logger=logger), f)

        data = {'production': {}, 'completions': {}}
        for _, hash_dict in parquet_metadata.items():
            data['production'][hash_dict['year']] = _transform_production(
                pathlib.Path(hash_dict['production_path']),
                config.transform_config.production_columns_to_keep,
                config.transform_config.production_columns_to_fill_null_with_zero,
                logger,
            )
            data['completions'][hash_dict['year']] = _transform_completions(
                pathlib.Path(hash_dict['completions_path']),
                config.transform_config.completions_columns_to_keep,
                config.transform_config.completions_columns_to_fill_null_with_zero,
                logger,
            )

        _write_output_data(
            data,
            config.export_dir,
            config.transform_config.remove_CO2_wells,
            logger,
        )


def _get_output_metadata(
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


def _write_output_data(
    data: dict[str, dict[int, pl.DataFrame]],
    output_path: pathlib.Path,
    remove_co2_wells: bool,
    logger: logging.Logger,
) -> None:
    for year, df in data['production'].items():
        df_out = df.join(
            data['completions'][max(data['completions'])],
            on='API_num',
            how='outer',
        )
        if remove_co2_wells:
            df_out = df_out.filter(pl.col('Prod_days') != 0)
        df_out.write_csv(output_path / f'{year}.csv')


def _transform_production(
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
                'boe_prod',
                'BOEd',
            ]],
            *[pl.col(c).first() for c in [
                'name',
                'operator_num',
            ]],
            *[pl.col(c).max() for c in [
                'Prod_days',
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


def _transform_completions(
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