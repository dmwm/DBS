#!/usr/bin/env python
from __future__ import print_function
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler, increase_interval
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options

import sys

options = get_command_line_options(__name__, sys.argv)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

initial_payload = payload_handler.get_payload()

number_of_cycles = initial_payload['workflow']['NumberOfCycles']
number_of_workflows = initial_payload['workflow']['NumberOfWorkflows']

print("Create initial %s workflows and %s cycles"  % (number_of_workflows, number_of_cycles))

events = []

for cycle in xrange(number_of_cycles):
    events.extend(('payload_provider', 'dbs3BulkInsert'))

for workflow in xrange(number_of_workflows):
    p = payload_handler.clone_payload()
    p['workflow']['Events'] = events
    payload_handler.append_payload(p)
    payload_handler.save_payload(options.output)
