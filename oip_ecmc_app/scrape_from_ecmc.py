import datetime
import json
import logging
import pathlib
import zipfile

import requests
from spock.backend.wrappers import Spockspace

import oip_ecmc.config as cfg
import oip_ecmc.logger as lgr
import oip_ecmc.setup as setup
import oip_ecmc.utils as utils


DESCRIPTION = '''
This script pulls Annual Production Summaries from the Colorado ECMC website and
extracts them for use with the included convert_access_to_parquet.py script.

filename_template must include "YYYY", which will be converted to the year for
each Production Summary file.
'''


def main() -> None:
    config, ecmc_data_path, logger = setup.setup_individual_script(
        cfg.ScrapeConfig,
        DESCRIPTION,
        'scrape_from_ecmc',
    )

    scrape_from_ecmc(config.ScrapeConfig, ecmc_data_path, logger)


def scrape_from_ecmc(
    config: Spockspace,
    ecmc_data_path: pathlib.Path,
    logger: logging.Logger,
) -> None:
    base_filenames = {
        year: config.filename_template.replace('YYYY', str(year))
        for year in config.years
    }

    zip_path = ecmc_data_path / config.zip_directory
    zip_temp_path = zip_path / 'temp'
    zip_temp_path.mkdir(parents=True, exist_ok=True)

    access_db_path = ecmc_data_path / config.access_db_directory
    access_db_previous_versions_path = access_db_path / 'previous_versions'
    access_db_previous_versions_path.mkdir(parents=True, exist_ok=True)

    utils.remove_files(zip_temp_path, ['zip', 'json'], logger=logger)

    downloaded_files = download_files(
        config.production_summary_base_url,
        base_filenames,
        zip_temp_path,
        logger,
    )

    zip_metadata = get_zip_metadata(downloaded_files, zip_path, logger)

    with (zip_temp_path / 'metadata.json').open('w') as f:
        json.dump(utils.to_json(zip_metadata, logger=logger), f)

    if utils.new_hashes(zip_metadata, zip_path / 'metadata.json', logger=logger):
        utils.remove_files(zip_path, ['zip', 'json'], logger=logger)
        utils.move_files(
            zip_temp_path, zip_path, ['zip', 'json'], logger=logger)

        utils.backup(
            access_db_path,
            access_db_previous_versions_path,
            'mdb',
            logger=logger,
        )

        unzip_pulled_files(zip_path, access_db_path, logger)

        with (access_db_path / 'metadata.json').open('w') as f:
            json.dump(
                utils.to_json(
                    get_db_metadata(access_db_path, zip_metadata, logger)),
                f,
            )


def download_files(
    base_url: str,
    filenames: list[str],
    out_dir: pathlib.Path,
    logger: logging.Logger,
) -> dict[int, pathlib.Path]:
    to_return = {}
    for year, f in filenames.items():
        try:
            url = f'{base_url.strip("/")}/{f}.zip'.replace(" ", "%20")
            response = requests.get(url)
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            logger.error(e)
            raise SystemExit(e)

        to_return[year] = out_dir / f'{f}.zip'
        to_return[year].write_bytes(response.content)
        logger.info(f'downloaded {url} to {to_return[year]}')

    return to_return


def get_zip_metadata(
    downloaded_files: dict[int, str],
    zip_dir:pathlib.Path,
    logger: logging.Logger,
) -> dict[str, dict]:
    return {
        utils.hash_file(f, logger=logger): {
            'path': zip_dir / f.name,
            'year': year,
            'timestamp': datetime.datetime.now().isoformat(),
        }
        for year, f in downloaded_files.items()
    }


def get_db_metadata(
    access_db_path: pathlib.Path,
    zip_metadata: dict[str, dict],
    logger: logging.Logger,
) -> dict[int, dict]:
    to_return = {}
    for _, metadata in zip_metadata.items():
        f = access_db_path / f'{metadata["path"].stem}.mdb'
        to_return[utils.hash_file(f, logger=logger)] = {
            'year': metadata['year'],
            'timestamp': metadata['timestamp'],
            'path': f,
        }
    return to_return


def unzip_pulled_files(
    pull_dir: pathlib.Path,
    db_dir: pathlib.Path,
    logger: logging.Logger,
) -> None:
    zip_files = [f for f in pull_dir.iterdir() if f.suffix == '.zip']

    for f in zip_files:
        if (db_dir / f'{f.stem}.mdb').exists():
            continue
        with zipfile.ZipFile(pull_dir / f.name, 'r') as z:
            z.extractall(db_dir)


if __name__ == '__main__':
    main()