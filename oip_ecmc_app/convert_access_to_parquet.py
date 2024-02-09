import json
import logging
import pathlib

import polars as pl
import spock

import oip_ecmc.logger as lgr
import oip_ecmc.utils as utils


DESCRIPTION = '''
This script accepts Microsoft Access Database files pulled from the Colorado ECMC website using the included scrape_from_ecmc.py script.

This will take those Access files and convert them into parquet files for better compatibility with automated tools such as the included transform_ecmc.py script.

Because this script relies on the Microsoft Access driver, it only runs on Microsoft Windows operating systems.
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
    log_level: utils.LogLevel = utils.LogLevel.INFO
    ecmc_data_path: str = '~/Documents/ecmc/'
    access_db_directory: str = 'ECMC db'
    parquet_directory: str = 'ECMC parquet'
    log_directory: str = 'logs'
    microsoft_access_driver: MsAccessDriver = MsAccessDriver.x64


def main() -> None:
    config = spock.SpockBuilder(Config, desc=DESCRIPTION).generate()

    log_path = utils.get_dir_path(config.Config.ecmc_data_path, config.Config.log_directory)
    logger = lgr.get_logger(
        'convert_access_to_parquet', config.Config.log_level, log_path + 'convert_access_to_parquet.jsonl')

    access_db_path = utils.get_dir_path(config.Config.ecmc_data_path, config.Config.access_db_directory)

    parquet_path = utils.get_dir_path(config.Config.ecmc_data_path, config.Config.parquet_directory)
    parquet_previous_versions_path = parquet_path + 'previous_versions\\'
    pathlib.Path(parquet_previous_versions_path).mkdir(parents=True, exist_ok=True)

    # check if files are the same
    # if not, update each file that needs updating. update metadata json file
    # metadata includes: file name, year, hash, download timestamp

    access_db_metadata = json.load(access_db_path + 'metadata.json')
    filenames = get_filenames(access_db_metadata)
    driver = get_access_driver(config.Config.microsoft_access_driver)
    data = mdb_import(
        access_db_path, filenames, logger, driver=driver)
    write_parquet(parquet_path, data)


def get_filenames(metadata: dict) -> dict[int, str]:
    '''
    '''


def odbc_connection_str(connection: dict[ODBCKey, str]) -> str:
    return ''.join([k + '=' + v + ';' for k, v in connection.items()])


def read_odbc_table(
        table: MsAccessTable, connection: dict[ODBCKey, str], logger: logging.Logger) -> pl.DataFrame:
    logger.info(f'loading data from {table} in {connection[ODBCKey.dbq]}')
    query = f'SELECT * FROM \"{table}\"'
    return pl.read_database(query, connection=odbc_connection_str(connection))


def mdb_import(
    db_dir: str,
    files: dict[int, str],
    logger: logging.Logger,
    driver: MsAccessDriver = MsAccessDriver.x64,
    tables: list[MsAccessTable] = [
        MsAccessTable.production,
        MsAccessTable.completions,
    ],
) -> dict[MsAccessTable, dict[int, pl.DataFrame]]:
    db_data = {table: {} for table in tables}
    connection = {ODBCKey.driver: driver, ODBCKey.dbq: ''}

    for year, f in files.items():
        connection[ODBCKey.dbq] = db_dir + f + '.mdb'
        for table in db_data:
            db_data[table][year] = read_odbc_table(table, connection, logger)

    return db_data


def write_parquet(
        out_dir: str, data: dict[MsAccessTable, dict[int, pl.DataFrame]]) -> None:
    for table, year_dfs in data.items():
        for year, df in year_dfs.items():
            filename = f'{table}_{year}.parquet'
            df.write_parquet(f'{out_dir}\\{filename}')


if __name__ == '__main__':
    main()