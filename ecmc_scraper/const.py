LOG_RECORD_BUILTIN_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}

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

NON_CONFIG_OPTIONS = [
    'config_option',
    'transform_config',
    'url_config',
    'version',
    'show_default_config',
    'write_default_config_to_file',
]

DEFAULT_YEARS = [2020, 2021, 2022, 2023]