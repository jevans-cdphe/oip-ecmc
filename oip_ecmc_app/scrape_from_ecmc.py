import datetime
import hashlib
import json
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

    # generate zip file URLs
    filenames = {
        year: config.Config.filename_template.replace('YYYY', str(year)) for year in config.Config.years}

    # get/make paths for zip files
    zip_path = utils.get_dir_path(config.Config.ecmc_data_path, config.Config.zip_directory)
    zip_temp_path = zip_path + 'temp\\'
    pathlib.Path(zip_temp_path).mkdir(parents=True, exist_ok=True)

    # get/make paths for access db files
    access_db_path = utils.get_dir_path(config.Config.ecmc_data_path, config.Config.access_db_directory)
    access_db_previous_versions_path = access_db_path + 'previous_versions\\'
    pathlib.Path(access_db_previous_versions_path).mkdir(parents=True, exist_ok=True)

    # remove files from temp zip directory
    for f in pathlib.Path(zip_temp_path).glob('*'):
        os.remove(f)

    # use wget to grab zip files from ECMC
    downloaded_files = {
        year: wget.download(config.Config.production_summary_base_url + f + '.zip', out=zip_temp_path)
        for year, f in filenames.items()
    }

    # hash zip files
    file_sha256_hashes = hash_files(downloaded_files)

    # build metadata for zip files
    zip_metadata = {
        file_sha256_hashes[year]: {
            'path': filename,
            'year': year,
            'timestamp': datetime.datetime.now().isoformat(),
        }
        for year, filename in downloaded_files.items()
    }

    # write out zip metadata
    with open(zip_temp_path + 'metadata.json', 'w') as f:
        json.dump(zip_metadata, f)

    # check if files are the same.
    new_files = False
    if pathlib.Path(zip_path + 'metadata.json').exists():
        with open(zip_path + 'metadata.json', 'r') as f:
            previous_zip_metadata = json.load(f)
        for prev_sha256 in previous_zip_metadata:
            if prev_sha256 not in zip_metadata:
                new_files = True
    else:
        os.rename(zip_temp_path + 'metadata.json', zip_path + 'metadata.json')
        for _, file_dict in zip_metadata.items():
            os.rename(file_dict['path'], zip_path + file_dict['path'][file_dict['path'].rfind('\\'):])
    
    if new_files:
        # if new files, remove old files and move new files to main zip directory
        os.remove(zip_path + 'metadata.json')
        os.rename(zip_temp_path + 'metadata.json', zip_path + 'metadata.json')
        for _, file_dict in zip_metadata.items():
            os.remove(zip_path + file_dict['path'][file_dict['path'].rfind('\\'):])
            os.rename(file_dict['path'], zip_path + file_dict['path'][file_dict['path'].rfind('\\'):])

        # backup any access db files
        previous_access_db_files = list(pathlib.Path(access_db_path).glob('*.[mdb json]*'))
        if len(previous_access_db_files) > 0:
            previous_access_db_dir = access_db_previous_versions_path + datetime.datetime.now().strftime('%Y%m%d-%H%M%S') + '\\'
            pathlib.Path(previous_access_db_dir).mkdir()
            for f in previous_access_db_files:
                os.rename(f, previous_access_db_dir + str(f)[str(f).rfind('\\'):])

        # unzip new files
        unzip_pulled_files(zip_path, access_db_path)

        # make access db metadata
        with open(access_db_path + 'metadata.json', 'w') as f:
            json.dump({
                metadata['year']: {
                    'timestamp': metadata['timestamp'],
                    'path': zip_path + metadata['path'][metadata['path'].rfind('\\') + 1:-3] + 'mdb',
                }
                for _, metadata in zip_metadata.items()
            }, f)


def hash_files(files_dict: dict[int, str]) -> dict[int, str]:
    file_hashes = {}

    for year, file_path in files_dict.items():
        with open(file_path, 'rb') as f:
            f_bytes = f.read()
            file_hashes[year] = hashlib.sha256(f_bytes).hexdigest()
    return file_hashes


def unzip_pulled_files(pull_dir: str, db_dir: str) -> None:
    zip_files = [f for f in os.listdir(pull_dir) if f.endswith('.zip')]

    for f in zip_files:
        if pathlib.Path(db_dir + f[:-4] + '.mdb').exists():
            continue
        with zipfile.ZipFile(pull_dir + f, 'r') as z:
            z.extractall(db_dir)


if __name__ == '__main__':
    main()