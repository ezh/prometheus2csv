#!/usr/bin/python
# -*- coding: utf-8 -*-
# Filename: metrics2csv.py

import requests
import sys
import getopt
import time
import logging

from datetime import datetime
from influxdb import InfluxDBClient

PROMETHEUS_URL = ''
QUERY_API = '/api/v1/query'
RANGE_QUERY_API = '/api/v1/query_range'
RESOLUTION = '' # default: 10s
START = '' # rfc3339 | unix_timestamp
END = '' # rfc3339 | unix_timestamp
PERIOD = '' # unit: second
SELECTOR = '__name__=~".+"'
INFUXDB_HOST = ''
INFUXDB_PORT = 8086
INFUXDB_DATABASE = 'telegraf'
INFUXDB_MEASUREMENT = ''


def add_time(d, time):
    return d[time] if time in d else {}


def add_tags(d, tags):
    return d[tags] if tags in d else {}


def add_fields(d, metric, value):
    d[metric] = value
    return d


def main():
    handle_args(sys.argv[1:])

    metricnames = query_metric_names()
    logging.info("Querying metric names succeeded, metric number: %s", len(metricnames))

    logging.info("Connect to InfluxDB {}:{}, database {}".format(INFUXDB_HOST, INFUXDB_PORT, INFUXDB_DATABASE))
    client = InfluxDBClient(INFUXDB_HOST, INFUXDB_PORT)
    client.switch_database(INFUXDB_DATABASE)
    push_metric_values(client, pull_metric_values(metricnames))


def handle_args(argv):
    global PROMETHEUS_URL
    global RESOLUTION
    global START
    global END
    global PERIOD
    global SELECTOR
    global INFUXDB_HOST
    global INFUXDB_PORT
    global INFUXDB_DATABASE
    global INFUXDB_MEASUREMENT

    try:
        opts, args = getopt.getopt(argv, "h:c:s:p:i:j:d:m:",
                ["host=", "container=", "step=", "period=",
                    "ihost", "iport", "database", "measurement",
                    "help", "start=", "end="])
    except getopt.GetoptError as error:
        logging.error(error)
        print_help_info()
        sys.exit(2)

    for opt, arg in opts:
        if opt == "--help":
            print_help_info()
            sys.exit()
        elif opt in ("-h", "--host"):
            PROMETHEUS_URL = arg
        elif opt in ("-s", "--step"):
            RESOLUTION = arg
        elif opt == "--start":
            START = arg
        elif opt == "--end":
            END = arg
        elif opt == "--period":
            PERIOD = int(arg)
        elif opt in ("-i", "--ihost"):
            INFUXDB_HOST = arg
        elif opt in ("-j", "--iport"):
            INFUXDB_PORT = arg
        elif opt in ("-d", "--database"):
            INFUXDB_DATABASE = arg
        elif opt in ("-m", "--measurement"):
            INFUXDB_MEASUREMENT = arg

    if PROMETHEUS_URL == '':
        logging.error("You should use -h or --host to specify your prometheus server's url, e.g. http://prometheus:9090")
        print_help_info()
        sys.exit(2)
    if INFUXDB_HOST == '':
        logging.error("You should use -ih or --influxdb_host to specify your influxdb server's url, e.g. influxdb")
        print_help_info()
        sys.exit(2)
    if INFUXDB_MEASUREMENT == '':
        logging.error("You should use -im or --influxdb_measurement to specify your influxdb measurement's name, e.g. my_measurement")
        print_help_info()
        sys.exit(2)

    if RESOLUTION == '':
        RESOLUTION = '10s'
        logging.warning("You didn't specify query resolution step width, will use default value %s", RESOLUTION)
    if PERIOD == '' and START == '' and END == '':
        PERIOD = 10
        logging.warning("You didn't specify query period or start&end time, will query the latest %s seconds' data as a test", PERIOD)
    if args:
        SELECTOR = ",".join(args)


def print_help_info():
    print('')
    print('Metrics2infuxdb Help Info')
    print('    Metrics2influxdb.py -h <prometheus_url> label_selector')
    print('or: metrics2influxdb.py --host=<prometheus_url> label_selector')
    print('---')
    print('Additional options: --start=<start_timestamp_or_rfc3339> --end=<end_timestamp_or_rfc3339> --period=<get_for_most_recent_period(int seconds)>')
    print('                    use start&end or only use period')


def query_metric_names():
    response = requests.get(PROMETHEUS_URL + QUERY_API, params={'query': 'sum by(__name__)({{{}}})'.format(SELECTOR)})
    logging.info("Request {}".format(response.request.url))
    status = response.json()['status']

    if status == "error":
        logging.error(response.json())
        sys.exit(2)

    results = response.json()['data']['result']
    metricnames = list()
    for result in results:
        metricnames.append(result['metric'].get('__name__', ''))
    metricnames.sort()

    return metricnames


def pull_metric_values(metricnames):
    values = {}
    if PERIOD != '':
        end_time = int(time.time())
        start_time = end_time - PERIOD
    else:
        end_time = END
        start_time = START

    for metric in metricnames:
        response = requests.get(PROMETHEUS_URL + RANGE_QUERY_API, params={'query': '{0}'.format(metric), 'start': start_time, 'end': end_time, 'step': RESOLUTION})
        logging.info(response.request.url)
        results = response.json()['data']['result']
        for element in results:
            metric = element['metric']['__name__']
            element['metric'].pop('job')
            element['metric'].pop('__name__')
            tags = tuple(sorted(element['metric'].items()))
            for value in element['values']:
                isotime = datetime.fromtimestamp(value[0]).isoformat()
                value = value[1]
                values[tags] = add_tags(values, tags)
                values[tags][isotime] = add_time(values[tags], isotime)
                values[tags][isotime] = add_fields(values[tags][isotime], metric, value)
    return values


def push_metric_values(client, values):
    points=[]
    for (tags, n) in values.items():
        for (isotime, fields) in n.items():
            points.append({
                "measurement": INFUXDB_MEASUREMENT,
                "fields": fields,
                "tags": dict(tags),
                "time": isotime})
    logging.info("Write {} {} measurements to InfluxDB".format(len(points), INFUXDB_MEASUREMENT))
    client.write_points(points)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
