import logging
import pathlib

from spock.backend.wrappers import Spockspace

from . import config as cfg
from . import logger as lgr
from . import utils


def setup_individual_script(
    config: Spockspace,
    description: str,
    script_name: str,
) -> tuple[Spockspace, pathlib.Path, logging.Logger]:
    return setup_script([config], description, script_name)


def setup_script(
    configs: list[Spockspace],
    description: str,
    script_name: str,
) -> tuple[Spockspace, pathlib.Path, logging.Logger]:
    config = cfg.get_config(configs, description)

    ecmc_data_path = utils.str_to_path(config.ECMCConfig.ecmc_data_path)

    logger = lgr.get_logger(
        script_name,
        config.ECMCConfig.log_level,
        ecmc_data_path / config.ECMCConfig.log_directory,
    )

    return (config, ecmc_data_path, logger)