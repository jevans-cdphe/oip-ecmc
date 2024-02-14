import json
import logging
import pathlib

import polars as pl
import spock

import oip_ecmc.logger as lgr
import oip_ecmc.utils as utils


DESCRIPTION = '''
This script accepts Microsoft Access Database files pulled from the Colorado
ECMC website using the included scrape_from_ecmc.py script.

This will take those Access files and convert them into parquet files for better
compatibility with automated tools such as the included transform_ecmc.py
script.

Because this script relies on the Microsoft Access driver, it only runs on
Microsoft Windows operating systems.
'''


class _MsAccessDriver(utils.StrEnum):
    x64 = r'{Microsoft Access Driver (*.mdb, *.accdb)}'
    x32 = r'{Microsoft Access Driver (*.mdb)}'


class MsAccessDriver(utils.StrEnum):
    '''
    Most modern Windows installations will have/use the x64 driver.
    '''
    x64 = 'x64'
    x32 = 'x32'


def get_access_driver(access_driver: MsAccessDriver) -> _MsAccessDriver:
    if access_driver == MsAccessDriver.x32:
        return _MsAccessDriver.x32
    return _MsAccessDriver.x64


class MsAccessTable(utils.StrEnum):
    production = 'Colorado Annual Production'
    completions = 'Colorado Well Completions'


class ODBCKey(utils.StrEnum):
    driver = 'Driver'
    max_buffer_size = 'MAXBUFFERSIZE'
    dsn = 'DSN'
    dbq = 'DBQ'
    description = 'DESCRIPTION'
    exclusive = 'EXCLUSIVE'
    page_timeout = 'PAGETIMEOUT'
    read_only = 'READONLY'
    system_database = 'SYSTEMDB'
    threads = 'THREADS'
    user_commit_sync = 'USERCOMMITSYNC'


@spock.spock
class Config:
    log_level: lgr.LogLevel = lgr.LogLevel.INFO
    ecmc_data_path: str = '~/Documents/ecmc/'
    access_db_directory: str = 'ECMC db'
    parquet_directory: str = 'ECMC parquet'
    log_directory: str = 'logs'
    microsoft_access_driver: MsAccessDriver = MsAccessDriver.x64


def main() -> None:
    config = spock.SpockBuilder(Config, desc=DESCRIPTION).generate()

    ecmc_data_path = utils.str_to_path(config.Config.ecmc_data_path)

    logger = lgr.get_logger(
        'convert_access_to_parquet',
        config.Config.log_level,
        ecmc_data_path / config.Config.log_directory,
    )

    access_db_path = ecmc_data_path / config.Config.access_db_directory
    parquet_path = ecmc_data_path / config.Config.parquet_directory
    parquet_previous_versions_path = parquet_path / 'previous_versions'
    parquet_previous_versions_path.mkdir(parents=True, exist_ok=True)

    with (access_db_path / 'metadata.json').open('r') as f:
        access_db_metadata = json.load(f)

    parquet_metadata = get_parquet_metadata(access_db_metadata, parquet_path)
    parquet_metadata_path = parquet_path / 'metadata.json'

    if utils.new_hashes(parquet_metadata, parquet_metadata_path):
        utils.backup(
            parquet_path,
            parquet_previous_versions_path,
            'parquet',
            path_keys=['production_path', 'completions_path'],
            keys_to_delete=['db_path'],
        )

        with parquet_metadata_path.open('w') as f:
            json.dump(utils.to_json(parquet_metadata), f)

        driver = get_access_driver(config.Config.microsoft_access_driver)
        data = mdb_import(access_db_metadata, logger, driver=driver)
        write_parquet(parquet_path, data)


def odbc_connection_str(connection: dict[ODBCKey, str]) -> str:
    return ''.join([k + '=' + v + ';' for k, v in connection.items()])


def read_odbc_table(
    table: MsAccessTable,
    connection: dict[ODBCKey, str],
    logger: logging.Logger,
) -> pl.DataFrame:
    logger.info(f'loading data from {table} in {connection[ODBCKey.dbq]}')
    query = f'SELECT * FROM \"{table}\"'
    return pl.read_database(query, connection=odbc_connection_str(connection))


def get_parquet_metadata(
    db_metadata: dict[str, dict],
    parquet_path: pathlib.Path,
) -> dict[str, dict]:
    
    return {
        sha_hash: {
            'year': hash_dict['year'],
            'db_path': pathlib.Path(hash_dict['path']),
            'production_path': parquet_path \
                / f'{MsAccessTable.production}_{hash_dict["year"]}.parquet',
            'completions_path': parquet_path \
                / f'{MsAccessTable.completions}_{hash_dict["year"]}.parquet',
            'timestamp': hash_dict['timestamp']
        }
        for sha_hash, hash_dict in db_metadata.items()
    }


def mdb_import(
    metadata: dict[int, str],
    logger: logging.Logger,
    driver: MsAccessDriver = MsAccessDriver.x64,
    tables: list[MsAccessTable] = [
        MsAccessTable.production,
        MsAccessTable.completions,
    ],
) -> dict[MsAccessTable, dict[int, pl.DataFrame]]:
    db_data = {table: {} for table in tables}
    connection = {ODBCKey.driver: driver, ODBCKey.dbq: ''}

    for _, hash_dict in metadata.items():
        connection[ODBCKey.dbq] = hash_dict['path']
        for table in db_data:
            db_data[table][hash_dict['year']] = read_odbc_table(
                table, connection, logger)

    return db_data


def write_parquet(
    out_dir: pathlib.Path,
    data: dict[MsAccessTable, dict[int, pl.DataFrame]],
) -> None:
    for table, year_dfs in data.items():
        for year, df in year_dfs.items():
            df.write_parquet(out_dir / f'{table}_{year}.parquet')


if __name__ == '__main__':
    main()