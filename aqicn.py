#!/usr/bin/env python3

import json
import os
import random
import requests
import time

import click
from influxdb import InfluxDBClient


# Sault Ste Marie
# "location": "Allens Sideroad, Sault Ste. Marie, Algoma, Ontario, P6C 5P7, Canada"

ssm_url = "https://api.waqi.info/feed/A202660"

# DEFAULT_BATCH_SIZE = 10_000


@click.group("aqicn")
def aqicn():
    """
    A wrapper for aqicn stuff
    """


def build_historical_influxdb_data(data: dict):
    """
    build historical influx data
    """
    influx_data = []
    location = data["Location"]["City"]
    for period in data["Location"]["periods"]:
        measurement = {
            "measurement": "aqicn_index_historical",
            "fields": {"aqicn_index": period["Index"]},
            "tags": {"station_location": location},
            "time": period["Period"],
        }
        influx_data.append(measurement)

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
    # logger.info("Writing data to influxdb...")
    # logger.info("Number of data points: ".format(len(influx_data)))
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


def fetch_forecast_data(session, url=ssm_url):
    """
    Fetch forecast data
    """
    # TODO: Dedupe this code
    url = f"{url}/?token={os.getenv('AQICN_TOKEN')}"
    print(url)
    data = session.get(url).json()
    return data


def fetch_historical_data(session, url=ssm_url):
    """
    Do historical stuff
    """
    # TODO: Dedupe this code
    url = f"{url}/?token={os.getenv('AQICN_TOKEN')}"
    print(url)
    data = session.get(url).json()
    return data


@click.command("historical", short_help="Fetch historical data")
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
def historical(random_sleep, dry_run):
    """
    Historical data
    """
    if bool(random_sleep) and dry_run is False:
        time.sleep(random.randrange(0, random_sleep))
    session = requests.Session()
    data = fetch_historical_data(session)
    if dry_run is True:
        print(json.dumps(data, indent=2))
        return

    influxdb_data = build_historical_influxdb_data(data)
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
        print(json.dumps(data, indent=2))
        return

    influxdb_data = build_forecast_influxdb_data(data)
    influxdb_client = build_influxdb_client()
    write_influx_data(influxdb_data, influxdb_client)


aqicn.add_command(forecast)
aqicn.add_command(historical)

if __name__ == "__main__":
    aqicn()
