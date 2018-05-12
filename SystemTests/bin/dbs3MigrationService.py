#!/usr/bin/env python
from __future__ import print_function
from LifeCycleTests.LifeCycleTools.APIFactory import create_api
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler, increase_interval
from LifeCycleTests.LifeCycleTools.Timing import TimingStat
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options
from LifeCycleTests.LifeCycleTools.StatsClient import StatsPipeClient

import os
import sys
import time

options = get_command_line_options(__name__, sys.argv)

config = {'url': os.environ.get("DBS_MIGRATE_URL", "https://cmsweb.cern.ch:8443/dbs/int/global/DBSMigrate/")}

api = create_api('DbsApi', config=config)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

named_pipe = payload_handler.payload['workflow']['NamedPipe']

stat_client = StatsPipeClient(named_pipe)

initial = payload_handler.payload['workflow']['dataset']

migration_timing = {'migration_stats': {'data': str(initial)}}

migration_url = os.environ.get("DBS_READER_URL", "https://cmsweb.cern.ch:8443/dbs/int/global/DBSReader/")

with TimingStat(migration_timing, stat_client, stats_name="migration_stats") as migration_timer:
    request_timing = {'stats': {'api': 'submit', 'query': str(initial)}}

    migration_input = dict(migration_url=migration_url, migration_input=initial)
    print("Putting migration_request: %s" % (migration_input))

    with TimingStat(request_timing, stat_client) as request_timer:
        migration_task = api.submitMigration(migration_input)

    request_processing_time, request_time = api.requestTimingInfo
    request_timer.update_stats({'server_request_timing': float(request_processing_time)/1000000.0,
                                'server_request_timestamp': float(request_time)/1000000.0,
                                'request_content_length': int(api.requestContentLength)})

    request_timer.stat_to_server()

    request_timing = {'stats': {'api': 'status', 'query': str(initial)}}

    while True:
        with TimingStat(request_timing, stat_client) as request_timer:
            migration_status = api.statusMigration(dataset=initial)

        request_processing_time, request_time = api.requestTimingInfo
        request_timer.update_stats({'server_request_timing': float(request_processing_time)/1000000.0,
                                    'server_request_timestamp': float(request_time)/1000000.0,
                                    'request_content_length': int(api.requestContentLength)})

        request_timer.stat_to_server()

        migration_status = migration_status[0]['migration_status']

        if migration_status in (2, 3):
            migration_timer.update_stats({'status': migration_status})
            break
        time.sleep(5)

migration_timer.stat_to_server()