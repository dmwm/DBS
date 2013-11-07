#!/usr/bin/env python
from LifeCycleTests.LifeCycleTools.PayloadHandler import PayloadHandler
from LifeCycleTests.LifeCycleTools.OptParser import get_command_line_options

import os, sys

options = get_command_line_options(__name__, sys.argv)

payload_handler = PayloadHandler()

payload_handler.load_payload(options.input)

block_dump = payload_handler.payload['workflow']['DBS']
das_info = payload_handler.payload['workflow']['DAS']

for block in block_dump:
    ### to be implemented
    pass

p = payload_handler.clone_payload()
payload_handler.append_payload(p)

payload_handler.save_payload(options.output)
