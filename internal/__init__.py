from configparser import ConfigParser, ExtendedInterpolation
import os
from pathlib import Path

from dotenv import dotenv_values
from rich import pretty
from rich import traceback

env_file = 'secret.env' if Path('secret.env').exists() else 'shared.env'

env = {
    **dotenv_values(env_file),
    **os.environ,
}
TOKEN = env["TOKEN"]
CREDS = env["CREDS"]

pretty.install()
traceback.install()


def load_config():
    config_folder = Path.cwd().absolute()
    config_file = config_folder / 'appconfig.ini'
    config = ConfigParser(
        interpolation=ExtendedInterpolation(),
        converters={'path': Path}
    )
    config.read(config_file)
    return config


config = load_config()
