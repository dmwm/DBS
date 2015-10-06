#!/usr/bin/env python
"""
Script to validate the input of bulk block insert API using WMCore block dump
"""
from optparse import OptionParser
from ast import literal_eval
import glob

from dbs.utils.DBSInputValidation import validateJSONInputNoCopy

def get_command_line_options():
    parser = OptionParser(usage='%prog --in')
    parser.add_option("-i", "--in", dest="input", help="Input file containing the block dump. Wildcards are supported", metavar="MyBlocks*.txt")

    (options, args) = parser.parse_args()

    if not options.input:
        parser.print_help()
        parser.error('Mandatory options are --input')

    return options, args

if __name__ == '__main__':
    options, args = get_command_line_options()

    input_files = glob.glob(options.input)

    for input_file in input_files:
        with open(input_file, 'r') as f:
            block_dump = literal_eval(f.read())

            try:
                validateJSONInputNoCopy('blockBulk', block_dump)
            except:
                raise
            else:
                print "Successfully validated!"
