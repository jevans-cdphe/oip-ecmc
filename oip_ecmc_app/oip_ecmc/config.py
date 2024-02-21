import datetime
from typing import List

import spock
from spock.backend.wrappers import Spockspace

from . import logger as lgr
from . import utils


def get_individial_config(config: Spockspace, description: str) -> Spockspace:
    return get_config([config], description)


def get_config(configs: List[Spockspace], description: str) -> Spockspace:
    return spock.SpockBuilder(ECMCConfig, *configs, desc=description).generate()


class MsAccessDriver(utils.StrEnum):
    '''
    Most modern Windows installations will have/use the x64 driver.
    '''
    x64 = 'x64'
    x32 = 'x32'


class OutputType(utils.StrEnum):
    '''
    Currently only csv is supported
    '''
    csv = 'csv'
    # ipc = 'ipc'
    # parquet = 'parquet'
    # avro = 'avro'
    # excel = 'excel'


@spock.spock
class ECMCConfig:
    log_level: lgr.LogLevel = lgr.LogLevel.INFO
    ecmc_data_path: str = '~/Documents/ecmc/'
    log_directory: str = 'logs'


@spock.spock
class ScrapeConfig:
    years: List[int] = [2020, 2021, 2022, 2023]
    production_summary_base_url: str = 'https://ecmc.state.co.us/documents/data/downloads/production/'
    zip_directory: str = 'ECMC pull'
    access_db_directory: str = 'ECMC db'
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


@spock.spock
class ConvertDBConfig:
    access_db_directory: str = 'ECMC db'
    parquet_directory: str = 'ECMC parquet'
    microsoft_access_driver: MsAccessDriver = MsAccessDriver.x64


@spock.spock
class TransformConfig:
    parquet_directory: str = 'ECMC parquet'
    output_directory: str = 'ECMC transformed data'
    output_type: OutputType = OutputType.csv
    remove_CO2_wells: bool = True

    production_columns_to_keep: List[str] = [
        'name',
        'operator_num',
        'API_num',
        'Prod_days',
        'gas_btu_sales',
        'gas_sales',
        'gas_shrinkage',
        'gas_used_on_lease',
        'flared_vented',
        'oil_adjustment',
        'oil_gravity',
        'oil_sales',
        'gas_prod',
        'oil_prod',
        'water_prod',
    ]

    completions_columns_to_keep: List[str] = [
        'facility_name',
        'facility_num',
        'well_name',
        'API_num',
        'well_bore_status',
        'county',
        'lat',
        'long',
        'first_prod_date',
        'gas_type',
    ]

    production_columns_to_fill_null_with_zero: List[str] = [
        'gas_btu_sales',
        'gas_sales',
        'gas_shrinkage',
        'gas_used_on_lease',
        'flared_vented',
        'oil_adjustment',
        'oil_gravity',
        'oil_sales',
        'gas_prod',
        'oil_prod',
        'water_prod',
    ]

    completions_columns_to_fill_null_with_zero: List[str] = [
        'gas_type',
    ]