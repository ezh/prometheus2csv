#!/usr/bin/python
# -*- coding: utf-8 -*-
# Filename: metrics2csv.py

import requests
import sys
import getopt
import time
import logging

PROMETHEUS_URL = ''
QUERY_API = '/api/v1/query'
RANGE_QUERY_API = '/api/v1/query_range'
RESOLUTION = '' # default: 10s
START = '' # rfc3339 | unix_timestamp
END = '' # rfc3339 | unix_timestamp
PERIOD = '' # unit: second
SELECTOR = '__name__=~".+"'

def main():
    handle_args(sys.argv[1:])

    metricnames = query_metric_names()
    logging.info("Querying metric names succeeded, metric number: %s", len(metricnames))

    forward_metric_values(metricnames=metricnames)

def handle_args(argv):
    global PROMETHEUS_URL
    global RESOLUTION
    global START
    global END
    global PERIOD
    global SELECTOR

    try:
        opts, args = getopt.getopt(argv, "h:c:s:p:", ["host=", "container=", "step=", "period=", "help", "start=", "end="])
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

    if PROMETHEUS_URL == '':
        logging.error("You should use -h or --host to specify your prometheus server's url, e.g. http://prometheus:9090")
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

    import ptpdb
    ptpdb.set_trace()

def print_help_info():
    print('')
    print('Metrics2StatsD Help Info')
    print('    metrics2statsd.py -h <prometheus_url> label_selector')
    print('or: metrics2statsd.py --host=<prometheus_url> label_selector')
    print('---')
    print('Additional options: --start=<start_timestamp_or_rfc3339> --end=<end_timestamp_or_rfc3339> --period=<get_for_most_recent_period(int seconds)>')
    print('                    use start&end or only use period')

def query_metric_names():
    print('sum by(__name__)({{{}}})'.format(SELECTOR))
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


def forward_metric_values(metricnames):
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
            print(element)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
