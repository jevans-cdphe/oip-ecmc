'''
This script accepts Microsoft Access Database files pulled from the Colorado
ECMC website using the included scrape_from_ecmc.py script.

This will take those Access files and convert them into parquet files for better
compatibility with automated tools such as the included transform_ecmc.py
script.

Because this script relies on the Microsoft Access driver, it only runs on
Microsoft Windows operating systems.
'''


import json
import logging
import pathlib

import polars as pl

from . import config as cfg
from . import enum
from . import utils


access_driver_map = {
    enum.MsAccessDriver.x64: r'{Microsoft Access Driver (*.mdb, *.accdb)}',
    enum.MsAccessDriver.x32: r'{Microsoft Access Driver (*.mdb)}',
}


def convert(
    config: cfg.ProductionSummariesConfig,
    logger: logging.Logger,
) -> None:
    parquet_previous_versions_path = config.parquet_dir / 'previous_versions'
    parquet_previous_versions_path.mkdir(parents=True, exist_ok=True)

    with (config.access_db_dir / 'metadata.json').open('r') as f:
        access_db_metadata = json.load(f)

    parquet_metadata = _get_parquet_metadata(
        access_db_metadata, config.parquet_dir, logger)
    parquet_metadata_path = config.parquet_dir / 'metadata.json'

    if utils.new_hashes(parquet_metadata, parquet_metadata_path, logger=logger):
        utils.backup(
            config.parquet_dir,
            parquet_previous_versions_path,
            'parquet',
            path_keys=['production_path', 'completions_path'],
            keys_to_delete=['db_path'],
            logger=logger,
        )

        with parquet_metadata_path.open('w') as f:
            json.dump(utils.to_json(parquet_metadata, logger=logger), f)

        data = _mdb_import(
            access_db_metadata, logger, driver=config.access_driver)
        _write_parquet(config.parquet_dir, data, logger)


def _odbc_connection_str(
        connection: dict[enum.ODBCKey, str], logger: logging.Logger) -> str:
    return ''.join([k + '=' + v + ';' for k, v in connection.items()])


def _read_odbc_table(
    table: enum.MsAccessTable,
    connection: dict[enum.ODBCKey, str],
    logger: logging.Logger,
) -> pl.DataFrame:
    logger.info(f'loading data from {table} in {connection[enum.ODBCKey.dbq]}')
    query = f'SELECT * FROM \"{table}\"'
    return pl.read_database(
        query, connection=_odbc_connection_str(connection, logger))


def _get_parquet_metadata(
    db_metadata: dict[str, dict],
    parquet_path: pathlib.Path,
    logger: logging.Logger,
) -> dict[str, dict]:
    
    return {
        sha_hash: {
            'year': hash_dict['year'],
            'db_path': pathlib.Path(hash_dict['path']),
            'production_path': parquet_path \
                / f'{enum.MsAccessTable.production}_{hash_dict["year"]}.parquet',
            'completions_path': parquet_path \
                / f'{enum.MsAccessTable.completions}_{hash_dict["year"]}.parquet',
            'timestamp': hash_dict['timestamp']
        }
        for sha_hash, hash_dict in db_metadata.items()
    }


def _mdb_import(
    metadata: dict[int, str],
    logger: logging.Logger,
    driver: enum.MsAccessDriver = enum.MsAccessDriver.x64,
    tables: list[enum.MsAccessTable] = [
        enum.MsAccessTable.production,
        enum.MsAccessTable.completions,
    ],
) -> dict[enum.MsAccessTable, dict[int, pl.DataFrame]]:
    db_data = {table: {} for table in tables}

    connection = {
        enum.ODBCKey.driver: access_driver_map[driver],
        enum.ODBCKey.dbq: '',
    }

    for _, hash_dict in metadata.items():
        connection[enum.ODBCKey.dbq] = hash_dict['path'] # type: ignore
        for table in db_data:
            db_data[table][hash_dict['year']] = _read_odbc_table( # type: ignore
                table, connection, logger)

    return db_data


def _write_parquet(
    out_dir: pathlib.Path,
    data: dict[enum.MsAccessTable, dict[int, pl.DataFrame]],
    logger: logging.Logger,
) -> None:
    for table, year_dfs in data.items():
        for year, df in year_dfs.items():
            df.write_parquet(out_dir / f'{table}_{year}.parquet')