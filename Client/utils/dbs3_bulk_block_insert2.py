#!/usr/bin/env python
"""
Script to insert bulk blocks into DBS 3
"""
from __future__ import print_function
from optparse import OptionParser
import glob
import json
import os

from dbs.apis.dbsClient import DbsApi


def get_command_line_options():
    parser = OptionParser(usage='%prog --in MyBlock.txt --url=<DBS_Instance_URL>')
    parser.add_option("-i", "--in", dest="input", help="Input file containing the block dump. Wildcard support.",
                      metavar="MyBlock*.txt")
    parser.add_option("-u", "--url", dest="url", help="DBS Instance url", metavar="DBS_Instance_URL")

    (options, args) = parser.parse_args()

    if not (options.input and options.url):
        parser.print_help()
        parser.error('Mandatory options are --input and --url')

    return options, args

if __name__ == '__main__':
    options, args = get_command_line_options()

    input_files = glob.glob(options.input)
    cmd = "date"
    api = DbsApi(url=options.url)
    for input_file in input_files:
        with open(input_file, 'r') as f:
            block_dump = json.loads(f.read())
            try:
                print("Time before DBS insertation.", os.system(cmd))
                api.insertBulkBlock(block_dump)
                print("Time after DBS insertation.", os.system(cmd))
            except:
                print("failed to load json.", os.system(cmd))
                t = api.listDataTiers(data_tier_name='RAW')
                print("*****data tier*****" , t)
                print("Time after calll listDataTiers: ", os.system(cmd))
                raise
            else:
                print("Successfully inserted block. ", os.system(cmd))
    print("Time before call listDataTiers. ", os.system(cmd))
    api.listDataTiers(data_tier_name='RAW')
    print("Time after calll listDataTiers. ", os.system(cmd))
