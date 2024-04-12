from dataclasses import dataclass
import pathlib
from typing import Optional

from . import logger
from . import utils


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


@dataclass(frozen=True)
class ProductionSummariesTransformConfig:
    completions_columns_to_fill_null_with_zero: list[str]
    completions_columns_to_keep: list[str]
    production_columns_to_fill_null_with_zero: list[str]
    production_columns_to_keep: list[str]
    remove_CO2_wells: bool

    @classmethod
    def from_dict(cls, args_dict: dict) -> 'ProductionSummariesTransformConfig':
        return cls(**args_dict)


@dataclass(frozen=True)
class ProductionSummariesUrlConfig:
    base_url: str
    default_zip_file_template: str
    yearly_zip_file_templates: dict[int, str]

    @classmethod
    def from_dict(cls, args_dict: dict) -> 'ProductionSummariesUrlConfig':
        return cls(
            base_url = args_dict['base_url'],
            default_zip_file_template = args_dict['zip_file_template']['default'],
            yearly_zip_file_templates = {
                year: template
                for year, template in args_dict['zip_file_template'].items()
                if year != 'default'
            },
        )

    def zip_file_name(self, year: int) -> str:
        if year not in self.yearly_zip_file_templates:
            return f'{self.default_zip_file_template.replace("YYYY", str(year))}.zip'
        
        return f'{self.zip_file_templates[year].replace("YYYY", str(year))}.zip'

    def url(self, year: int) -> str:
        return f'{self.base_url.strip("/")}/{self.zip_file_name(year)}'.replace(" ", "%20")


@dataclass(frozen=True)
class ProductionSummariesConfig:
    access_db_dir: pathlib.Path
    access_driver: MsAccessDriver
    export_type: OutputType
    export_dir: pathlib.Path
    log_dir: pathlib.Path
    log_level: logger.LogLevel
    parquet_dir: pathlib.Path
    quiet: bool
    show_config: bool
    transform: bool
    transform_config: ProductionSummariesTransformConfig
    url_config: ProductionSummariesUrlConfig
    write_config_to_file: Optional[pathlib.Path]
    years: list[int]
    zip_dir: pathlib.Path

    @classmethod
    def from_dict(cls, args_dict: dict) -> 'ProductionSummariesConfig':
        if args_dict['transform_config'] is not None:
            args_dict['transform_config'] = ProductionSummariesTransformConfig.from_dict(
                args_dict['transform_config'])

        if args_dict['url_config'] is not None:    
            args_dict['url_config'] = ProductionSummariesUrlConfig.from_dict(
                args_dict['url_config'])

        return cls(**args_dict)