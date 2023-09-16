#!/usr/bin/env python3

import json
import os
import random
import requests
import time

import click
from influxdb import InfluxDBClient
from loguru import logger

# Sault Ste Marie
# "location": "Allens Sideroad, Sault Ste. Marie, Algoma, Ontario, P6C 5P7, Canada"
#
# At some point, may want to switch to using nominatim:
# curl 'https://nominatim.openstreetmap.org/reverse?lat=46.59215&lon=-84.402466&format=json' | jq .
#
# Also: think about a class for this
SSM_URL = "https://api.waqi.info/feed/A202660"
LOCATION = "Sault Ste Marie"
STATION = "Allens Sideroad"
# my name, api name
MEASUREMENTS = {"pm10": "pm10", "pm25": "pm25", "temp": "t", "humidity": "h"}


DEFAULT_BATCH_SIZE = 10_000


@click.group("aqicn")
def aqicn():
    """
    A wrapper for aqicn stuff
    """


def build_current_influxdb_data(data: dict):
    """
    Build current conditions influx data
    """
    influx_data = []
    station = STATION
    for my_name, their_name in MEASUREMENTS.items():
        # Coerce to a float in case it comes back as an int
        val = float(data["data"]["iaqi"][their_name]["v"])
        # They appear to record time in epoch seconds.  That works for
        # me; the call in write_influx_data specifies "seconds" as the
        # precision.
        tstamp = data["data"]["time"]["v"]
        measurement = {
            "measurement": "aqicn",
            "fields": {my_name: val},
            "tags": {"location": LOCATION, "station": STATION},
            "time": tstamp,
        }
        influx_data.append(measurement)

    logger.info("Made it here")
    return influx_data


def build_forecast_influxdb_data(data: dict):
    """
    Build influxdb data and return it
    """
    # logger = logging.getLogger(__name__)
    # logger.info("Building influxdb data...")

    influx_data = []
    location = data["Location"]["City"]
    forecast_date = data["ForecastDate"]
    for period in data["Location"]["periods"]:
        if period["Type"] != "Today":
            continue

        print(period)
        measurement = {
            "measurement": "aqicn_index",
            "fields": {"aqicn_index": period["Index"]},
            "tags": {"station_location": location},
            "time": forecast_date,
        }
        influx_data.append(measurement)

    return influx_data


def write_influx_data(influx_data, influx_client):
    """
    Write influx_data to database
    """
    # logger = logging.getLogger(__name__)
    logger.info("Writing data to influxdb...")
    logger.debug("Number of data points: {}".format(len(influx_data)))
    print(
        influx_client.write_points(
            influx_data, time_precision="s", batch_size=DEFAULT_BATCH_SIZE
        )
    )


def build_influxdb_client():
    """
    Build and return InfluxDB client
    """
    # Setup influx client
    # logger = logging.getLogger(__name__)

    db = os.getenv("INFLUX_DB", "You forgot to set INFLUX_DB in .secret.sh!")
    host = os.getenv("INFLUX_HOST", "You forgot to set INFLUX_HOST in .secret.sh!")
    port = os.getenv("INFLUX_PORT", "You forgot to set INFLUX_PORT in .secret.sh!")
    influx_user = os.getenv(
        "INFLUX_USER", "You forgot to set INFLUX_USER in .secret.sh!"
    )
    influx_pass = os.getenv(
        "INFLUX_PASS", "You forgot to set INFLUX_PASS in .secret.sh!"
    )

    influx_client = InfluxDBClient(
        host=host,
        port=port,
        username=influx_user,
        password=influx_pass,
        database=db,
        ssl=True,
        verify_ssl=True,
    )
    # logger.info("Connected to InfluxDB version {}".format(influx_client.ping()))
    print("Connected to InfluxDB version {}".format(influx_client.ping()))
    return influx_client


def fetch_forecast_data(session, url=SSM_URL):
    """
    Fetch forecast data
    """
    # TODO: Dedupe this code
    url = f"{url}/?token={os.getenv('AQICN_TOKEN')}"
    print(url)
    data = session.get(url).json()
    return data


def fetch_current_data(session, url=SSM_URL):
    """
    Fetch current data
    """
    # TODO: Dedupe this code
    url = f"{url}/?token={os.getenv('AQICN_TOKEN')}"
    print(url)
    data = session.get(url).json()
    return data


@click.command("current", short_help="Fetch current data")
@click.option(
    "--random-sleep",
    default=300,
    help="Sleep for random number of seconds, up to default.  Set to 0 to disable.",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    help="Don't push to Influxdb, just dump data",
)
def current(random_sleep, dry_run):
    """
    Fetch current data
    """
    if bool(random_sleep) and dry_run is False:
        time.sleep(random.randrange(0, random_sleep))
    session = requests.Session()
    logger.debug("Here we go, fetching data")
    data = fetch_current_data(session)
    if dry_run is True:
        logger.debug("Raw data:")
        logger.debug(json.dumps(data, indent=2))
        logger.debug("=-=-=-=-=-=-=-=-")

    influxdb_data = build_current_influxdb_data(data)
    if dry_run is True:
        logger.debug("InfluxDB data:")
        logger.debug(json.dumps(influxdb_data, indent=2))
        logger.debug("=-=-=-=-=-=-=-=-")
        return

    influx_clientdb = build_influxdb_client()
    write_influx_data(influxdb_data, influx_clientdb)


@click.command("forecast", short_help="Fetch forecast data")
@click.option(
    "--random-sleep",
    default=300,
    help="Sleep for random number of seconds, up to default.  Set to 0 to disable.",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    help="Don't push to Influxdb, just dump data",
)
def forecast(random_sleep, dry_run):
    """
    Forecast data
    """
    if bool(random_sleep) and dry_run is False:
        time.sleep(random.randrange(0, random_sleep))
    session = requests.Session()
    data = fetch_forecast_data(session)
    if dry_run is True:
        print("Raw data:")
        print(json.dumps(data, indent=2))
        print("=-=-=-=-=-=-=-=-")

    influxdb_data = build_forecast_influxdb_data(data)
    if dry_run is True:
        print("InfluxDB data:")
        print(json.dumps(influxdb_data, indent=2))
        print("=-=-=-=-=-=-=-=-")
        return
    influxdb_client = build_influxdb_client()
    write_influx_data(influxdb_data, influxdb_client)


aqicn.add_command(forecast)
aqicn.add_command(current)

if __name__ == "__main__":
    aqicn()
