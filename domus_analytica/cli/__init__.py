import logging

import click

from domus_analytica.cli.gis_import.station_passengers import import_station_passengers
from domus_analytica.cli.suumo import download_from_suumo

log = logging.getLogger(__name__)


@click.group()
@click.option("--debug", "-v", is_flag=True, help="Print debug logs.")
def app(debug: bool):
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)


app.command("suumo", help="Download data from SUUMO")(download_from_suumo)


@app.group()
def gis_import():
    pass


gis_import.command("station_passengers")(import_station_passengers)
