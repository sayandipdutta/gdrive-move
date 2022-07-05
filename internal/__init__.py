import os

from dotenv import dotenv_values
from rich import pretty
from rich import traceback

config = {
    **dotenv_values('secret.env'),
    **os.environ,
}
TOKEN = config["TOKEN"]
CREDS = config["CREDS"]

pretty.install()
traceback.install()
