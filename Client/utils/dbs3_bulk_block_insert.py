#!/usr/bin/env python
"""
Script to insert bulk blocks into DBS 3
"""

from optparse import OptionParser
from ast import literal_eval
import glob

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

    for input_file in input_files:
        with open(input_file, 'r') as f:
            block_dump = literal_eval(f.read())

            api = DbsApi(url=options.url)

            try:
                api.insertBulkBlock(block_dump)
            except:
                raise
            else:
                print("Successfully inserted block!")
