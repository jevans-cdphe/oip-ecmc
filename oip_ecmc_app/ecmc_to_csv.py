
import oip_ecmc.config as cfg
import oip_ecmc.setup as setup

from . import convert_access_to_parquet
from . import transform_ecmc
from . import scrape_from_ecmc


DESCRIPTION = '\n'.join(
    scrape_from_ecmc.DESCRIPTION,
    convert_access_to_parquet.DESCRIPTION,
    transform_ecmc.DESCRIPTION,
)


def main() -> None:
    config, ecmc_data_path, logger = setup.setup_script(
        [
            cfg.ScrapeConfig,
            cfg.ConvertDBConfig,
            cfg.TransformConfig,
        ],
        DESCRIPTION,
        'ecmc_to_csv',
    )

    scrape_from_ecmc.scrape_from_ecmc(
        config.ScrapeConfig, ecmc_data_path, logger)
    convert_access_to_parquet.convert_access_to_parquet(
        config.ConvertDBConfig, ecmc_data_path, logger)
    transform_ecmc.transform_ecmc(
        config.TransformConfig, ecmc_data_path, logger)


if __name__ == '__main__':
    main()