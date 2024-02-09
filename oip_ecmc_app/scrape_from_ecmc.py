import datetime
import os
import pathlib
from typing import List
import zipfile

import spock
import wget

import oip_ecmc.logger as lgr
import oip_ecmc.utils as utils


DESCRIPTION = '''
This script pulls Annual Production Summaries from the Colorado ECMC website and extracts them for use with the included convert_access_to_parquet.py script.

filename_template must include "YYYY", which will be converted to the year for each Production Summary file.
'''


@spock.spock
class Config:
    log_level: utils.LogLevel = utils.LogLevel.INFO
    years: List[int] = [2020, 2021, 2022, 2023]
    production_summary_base_url: str = 'https://ecmc.state.co.us/documents/data/downloads/production/'
    ecmc_data_path: str = '~/Documents/ecmc/'
    zip_directory: str = 'ECMC pull'
    access_db_directory: str = 'ECMC db'
    log_directory: str = 'logs'
    filename_template: str = 'co YYYY Annual Production Summary-xp'

    def __post_hook__(self):
        for year in self.years:
            spock.utils.within(
                year,
                low_bound=1999,
                upper_bound=datetime.datetime.now().year,
                inclusive_lower=True,
                inclusive_upper=True,
            )


def main() -> None:
    # generate config
    config = spock.SpockBuilder(Config, desc=DESCRIPTION).generate()

    # set up logger
    log_path = utils.get_dir_path(config.Config.ecmc_data_path, config.Config.log_directory)
    logger = lgr.get_logger(
        'scrape_from_ecmc', config.Config.log_level, log_path + 'scrape_from_ecmc.jsonl')

    # 
    filenames = get_filenames(config.Config.filename_template, config.Config.years)

    zip_path = utils.get_dir_path(config.Config.ecmc_data_path, config.Config.zip_directory)
    zip_temp_path = zip_path + 'temp\\'
    pathlib.Path(zip_temp_path).mkdir(parents=True, exist_ok=True)

    access_db_path = utils.get_dir_path(config.Config.ecmc_data_path, config.Config.access_db_directory)
    access_db_previous_versions_path = access_db_path + 'previous_versions\\'
    pathlib.Path(access_db_previous_versions_path).mkdir(parents=True, exist_ok=True)

    pull_from_ecmc(zip_temp_path, filenames, config.Config.production_summary_base_url)

    # check if files are the same
    # if not, update each file that needs updating. update metadata json file
    # metadata includes: file name, year, hash, download timestamp

    unzip_pulled_files(zip_path, access_db_path)


def get_filenames(filename_template: str, years: tuple[int]) -> dict[int, str]:
    return {year: filename_template.replace('YYYY', str(year)) for year in years}


def pull_from_ecmc(pull_dir: str, files: dict[int: str], base_url: str) -> dict[int, str]:
    return {year: wget.download(base_url + f + '.zip', out=pull_dir) for year, f in files.items()}


def unzip_pulled_files(pull_dir: str, db_dir: str) -> None:
    zip_files = [f for f in os.listdir(pull_dir) if f.endswith('.zip')]

    for f in zip_files:
        if pathlib.Path(db_dir + f[:-4] + '.mdb').exists():
            continue
        with zipfile.ZipFile(pull_dir + f, 'r') as z:
            z.extractall(db_dir)


if __name__ == '__main__':
    main()