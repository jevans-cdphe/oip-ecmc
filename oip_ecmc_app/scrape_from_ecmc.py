import datetime
import json
import pathlib
from typing import List
import zipfile

import spock
import wget

import oip_ecmc.logger as lgr
import oip_ecmc.utils as utils


DESCRIPTION = '''
This script pulls Annual Production Summaries from the Colorado ECMC website and
extracts them for use with the included convert_access_to_parquet.py script.

filename_template must include "YYYY", which will be converted to the year for
each Production Summary file.
'''


@spock.spock
class Config:
    log_level: lgr.LogLevel = lgr.LogLevel.INFO
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
    config = spock.SpockBuilder(Config, desc=DESCRIPTION).generate()

    ecmc_data_path = utils.str_to_path(config.Config.ecmc_data_path)

    logger = lgr.get_logger(
        'scrape_from_ecmc',
        config.Config.log_level,
        ecmc_data_path / config.Config.log_directory,
    )

    base_filenames = {
        year: config.Config.filename_template.replace('YYYY', str(year))
        for year in config.Config.years
    }

    zip_path = ecmc_data_path / config.Config.zip_directory
    zip_temp_path = zip_path / 'temp'
    zip_temp_path.mkdir(parents=True, exist_ok=True)

    access_db_path = ecmc_data_path / config.Config.access_db_directory
    access_db_previous_versions_path = access_db_path / 'previous_versions'
    access_db_previous_versions_path.mkdir(parents=True, exist_ok=True)

    utils.remove_files(zip_temp_path, ['zip', 'json'])

    downloaded_files = download_files(
        config.Config.production_summary_base_url,
        base_filenames,
        zip_temp_path,
    )

    zip_metadata = get_zip_metadata(downloaded_files, zip_path)

    with (zip_temp_path / 'metadata.json').open('w') as f:
        json.dump(utils.to_json(zip_metadata), f)

    if utils.new_hashes(zip_metadata, zip_path / 'metadata.json'):
        utils.remove_files(zip_path, ['zip', 'json'])
        utils.move_files(zip_temp_path, zip_path, ['zip', 'json'])

        utils.backup(access_db_path, access_db_previous_versions_path, 'mdb')
        unzip_pulled_files(zip_path, access_db_path)

        with (access_db_path / 'metadata.json').open('w') as f:
            json.dump(
                utils.to_json(get_db_metadata(access_db_path, zip_metadata)), f)


def download_files(
    base_url: str,
    filenames: list[str],
    out_dir: pathlib.Path,
) -> dict[int, str]:
    return {
        year: pathlib.Path(
            wget.download(
                f'{base_url}/{f}.zip',
                out=str(out_dir),
            )
        )
        for year, f in filenames.items()
    }


def get_zip_metadata(
    downloaded_files: dict[int, str],
    zip_dir:pathlib.Path,
) -> dict[str, dict]:
    return {
        utils.hash_file(f): {
            'path': zip_dir / f.name,
            'year': year,
            'timestamp': datetime.datetime.now().isoformat(),
        }
        for year, f in downloaded_files.items()
    }


def get_db_metadata(
    access_db_path: pathlib.Path,
    zip_metadata: dict[str, dict],
) -> dict[int, dict]:
    to_return = {}
    for _, metadata in zip_metadata.items():
        f = access_db_path / f'{metadata["path"].stem}.mdb'
        to_return[utils.hash_file(f)] = {
            'year': metadata['year'],
            'timestamp': metadata['timestamp'],
            'path': f,
        }
    return to_return


def unzip_pulled_files(pull_dir: pathlib.Path, db_dir: pathlib.Path) -> None:
    zip_files = [f for f in pull_dir.iterdir() if f.suffix == '.zip']

    for f in zip_files:
        if (db_dir / f'{f.stem}.mdb').exists():
            continue
        with zipfile.ZipFile(pull_dir / f.name, 'r') as z:
            z.extractall(db_dir)


if __name__ == '__main__':
    main()