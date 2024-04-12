'''
This script pulls Annual Production Summaries from the Colorado ECMC website and
extracts them for use with the included convert_access_to_parquet.py script.

filename_template must include "YYYY", which will be converted to the year for
each Production Summary file.
'''


import datetime
import json
import logging
import pathlib
import zipfile

import requests

from . import config as cfg
from . import utils


def scrape(
    config: cfg.ProductionSummariesConfig,
    logger: logging.Logger,
) -> None:
    zip_temp_path = config.zip_dir / 'temp'
    zip_temp_path.mkdir(parents=True, exist_ok=True)

    access_db_path = config.access_db_dir
    access_db_previous_versions_path = access_db_path / 'previous_versions'
    access_db_previous_versions_path.mkdir(parents=True, exist_ok=True)

    utils.remove_files(zip_temp_path, ['zip', 'json'], logger=logger)

    downloaded_files = _download_files(
        config.years,
        config.url_config,
        zip_temp_path,
        logger,
    )

    zip_metadata = _get_zip_metadata(downloaded_files, config.zip_dir, logger)

    with (zip_temp_path / 'metadata.json').open('w') as f:
        json.dump(utils.to_json(zip_metadata, logger=logger), f)

    if utils.new_hashes(
            zip_metadata, config.zip_dir / 'metadata.json', logger=logger):
        utils.remove_files(config.zip_dir, ['zip', 'json'], logger=logger)
        utils.move_files(
            zip_temp_path, config.zip_dir, ['zip', 'json'], logger=logger)

        utils.backup(
            access_db_path,
            access_db_previous_versions_path,
            'mdb',
            logger=logger,
        )

        _unzip_pulled_files(config.zip_dir, access_db_path, logger)

        with (access_db_path / 'metadata.json').open('w') as f:
            json.dump(
                utils.to_json(
                    _get_db_metadata(access_db_path, zip_metadata, logger)),
                f,
            )


def _download_files(
    years: list[int],
    url_config: cfg.ProductionSummariesUrlConfig,
    out_dir: pathlib.Path,
    logger: logging.Logger,
) -> dict[int, pathlib.Path]:
    to_return = {}
    for year in years:
        try:
            url = url_config.url(year)
            response = requests.get(url)
            response.raise_for_status()

        except requests.exceptions.HTTPError as e:
            logger.error(e)
            raise SystemExit(e)

        to_return[year] = out_dir / url_config.zip_file_name(year)
        to_return[year].write_bytes(response.content)
        logger.info(f'downloaded {url} to {to_return[year]}')

    return to_return


def _get_zip_metadata(
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


def _get_db_metadata(
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


def _unzip_pulled_files(
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