import datetime
import pathlib
from typing import List, Optional
from typing_extensions import Annotated

import yaml
from rich import print
from rich.console import Console
from rich.syntax import Syntax
import typer

from . import config as cfg
from . import convert_production_summaries_access_to_parquet as convert_prod
from . import logger as lgr
from . import package_info
from . import scrape_production_summaries as scrape_prod
from . import transform_production_summaries as transform_prod
from . import utils


DEFAULT_DIR = pathlib.Path.home() / 'Documents/ecmc-data'

DEFAULT_URL_CONFIG = {
    'base_url': 'https://ecmc.state.co.us/documents/data/downloads/production/',
    'zip_file_template': {
        'default': 'co YYYY Annual Production Summary-xp',
        1999: 'co YYYY Annual Production Summary-XP',
        2000: 'co YYYY Annual Production Summary-XP',
    },
}

DEFAULT_TRANSFORM_CONFIG = {
    'remove_CO2_wells': True,
    'production_columns_to_keep': [
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
    ],
    'completions_columns_to_keep': [
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
    ],
    'production_columns_to_fill_null_with_zero': [
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
    ],
    'completions_columns_to_fill_null_with_zero': ['gas_type'],
}

app = typer.Typer(no_args_is_help=True)
url_config_from_global_config_file = None
transform_config_from_global_config_file = None


def version_callback(ctx: typer.Context, value: bool):
    if ctx.resilient_parsing:
        return value

    if not value:
        return value

    print(f'ECMC Scraper version {package_info.__version__}')
    print(package_info.__copyright__)
    print(f'\nMaintainer:\n{package_info.__maintainer__}')
    print(package_info.__email__)

    raise typer.Exit()


def show_default_config_callback(ctx: typer.Context, value: bool):
    if ctx.resilient_parsing:
        return value

    if not value:
        return value
    
    config_dict = get_default_config(ctx)

    Console().print(Syntax(yaml.dump(config_dict, indent=4),'yaml'))
    
    raise typer.Exit()


def write_default_config_callback(ctx: typer.Context, value: Optional[pathlib.Path]):
    if ctx.resilient_parsing:
        return value

    if value is None:
        return value
    
    config_dict = get_default_config(ctx)
    yaml.dump(config_dict, value.open('w'), indent=4)
    
    raise typer.Exit()


def get_default_config(ctx: typer.Context) -> dict:
    config_dict = {
        p.name: p.default
        for p in ctx.command.params
        if p.name not in (
            'config_option',
            'transform_config',
            'url_config',
            'version',
            'show_default_config',
            'write_default_config_to_file'
        )
    }

    config_dict['transform_config'] = DEFAULT_TRANSFORM_CONFIG
    config_dict['url_config'] = DEFAULT_URL_CONFIG
    config_dict['years'] = [2020, 2021, 2022, 2023]

    return utils.to_json(config_dict)


def config_callback(ctx: typer.Context, value):
    if value is None:
        return value

    conf = yaml.safe_load(value)

    if 'url_config' in conf:
        url_config_from_global_config_file = yaml.safe_dump(conf['url_config'])

    if 'transform_config' in conf:
        transform_config_from_global_config_file = yaml.safe_dump(
            conf['transform_config'])

    for k in conf.keys():
        if k.endswith('_dir'):
            conf[k] = pathlib.Path(conf[k])

    ctx.default_map = ctx.default_map or {}
    ctx.default_map.update({
        k: v
        for k, v in conf.items()
        if k not in ('transform_config', 'url_config')
    })

    return value


@app.callback()
def callback():
    """
    Scrapes the ECMC download page.
    """


@app.command()
def production_summaries(
    ctx: typer.Context,
    years: Annotated[
        List[int],
        typer.Option(
            min=1999,
            max=datetime.datetime.now().year,
            help='a list of the report years desired, e.g. 2020 2021 2022 2023',
        ),
    ] = [2020, 2021, 2022, 2023],
    version: Annotated[
        bool,
        typer.Option(
            '--version', '-v',
            callback=version_callback,
            help='Print the current version.',
            is_eager=True,
        ),
    ] = False,
    quiet: Annotated[
        bool,
        typer.Option(
            '--quiet', '-q',
            help='Hide all standard output.',
        ),
    ] = False,
    show_config: Annotated[
        bool,
        typer.Option(
            '--show-config',
            help='Show the resulting configuration for the current command to copy it or to pipe it to a file.',
        ),
    ] = False,
    write_config_to_file: Annotated[
        Optional[pathlib.Path],
        typer.Option(show_default=False),
    ] = None,
    show_default_config: Annotated[
        bool,
        typer.Option(
            '--show-default-config',
            callback=show_default_config_callback,
            help='Show the default configuration to copy it or to pipe it to a file.',
            is_eager=True,
        ),
    ] = False,
    write_default_config_to_file: Annotated[
        Optional[pathlib.Path],
        typer.Option(
            callback=write_default_config_callback,
            is_eager=True,
            show_default=False,
        ),
    ] = None,
    config_option: Annotated[
        Optional[typer.FileText],
        typer.Option(
            '--config', '-c',
            help='Run script using a YAML configuration file.',
            callback=config_callback,
            is_eager=True,
            show_default=False,
        ),
    ] = None,
    log_level: lgr.LogLevel = lgr.LogLevel.INFO,
    zip_dir: pathlib.Path = DEFAULT_DIR / 'production-summaries/zip',
    access_db_dir: pathlib.Path = DEFAULT_DIR / 'production-summaries/access-db',
    parquet_dir: pathlib.Path = DEFAULT_DIR / 'production-summaries/parquet',
    log_dir: pathlib.Path = DEFAULT_DIR / 'production-summaries/logs',
    export_dir: pathlib.Path = DEFAULT_DIR / 'production-summaries/export',
    access_driver: cfg.MsAccessDriver = cfg.MsAccessDriver.x64,
    export_type: cfg.OutputType = cfg.OutputType.csv,
    transform: Annotated[
        bool,
        typer.Option(
            '--transform', '-t',
            help='Transform data before export (read documentation for more details).',
        ),
    ] = False,
    url_config: Annotated[
        Optional[typer.FileText],
        typer.Option(
            help='YAML Configuration file used to change url arguments.',
            show_default=False,
        ),
    ] = None,
    transform_config: Annotated[
        Optional[typer.FileText],
        typer.Option(
            help='YAML Configuration file used to change transform arguments.',
            show_default=False,
        ),
    ] = None,
):
    """
    Scrapes ECMC Production Summaries.
    """
    if not transform:
        typer.confirm(
            'Exporting data without transforming is not yet supported. Do you want to continue?',
            abort = True,
        )

    url_config_data = DEFAULT_URL_CONFIG
    transform_config_data = DEFAULT_TRANSFORM_CONFIG

    if url_config_from_global_config_file is not None:
        url_config_data.update(url_config_from_global_config_file)

    if transform_config_from_global_config_file is not None:
        transform_config_data.update(transform_config_from_global_config_file)

    if url_config is not None:
        url_config_data.update(yaml.safe_load(url_config))

    if transform_config is not None:
        transform_config_data.update(yaml.safe_load(transform_config))

    config_dict = {
        k: v
        for k, v in ctx.params.items()
        if k not in (
            'config_option',
            'transform_config',
            'url_config',
            'version',
            'show_default_config',
            'write_default_config_to_file'
        )
    }

    config_dict['transform_config'] = transform_config_data
    config_dict['url_config'] = url_config_data
    config = cfg.ProductionSummariesConfig.from_dict(config_dict)

    logger = lgr.get_logger(
        'production_summaries',
        config.log_level,
        config.log_dir,
    )

    scrape_prod.scrape(config, logger)
    convert_prod.convert(config, logger)

    if transform:
        transform_prod.transform(config, logger)   

    if show_config or write_config_to_file is not None:
        config_dict = utils.to_json(config_dict)

        if write_config_to_file is not None:
            yaml.dump(config_dict, write_config_to_file.open('w'), indent=4)

        if show_config and not quiet:
            Console().print(Syntax(yaml.dump(config_dict, indent=4),'yaml'))