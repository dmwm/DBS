#!/usr/bin/env python
from __future__ import print_function
from optparse import OptionParser

from dbs.apis.dbsClient import DbsApi

def get_command_line_options():
    parser = OptionParser(usage='%prog --src_url=<Source_DBS_Instance_URL> --dst_url=<Destination_DBS_Instance_URL> --blocks=<block_list>')
    parser.add_option("-s", "--surl", dest="url_src", help="Source DBS Instance url", metavar="Source_DBS_Instance_URL")
    parser.add_option("-d", "--durl", dest="url_dst", help="Destination DBS Instance url", metavar="Destination_DBS_Instance_URL")
    parser.add_option("-b", "--blocks", dest="blocks", help="File containing block list", metavar="BlockList.txt")

    (options, args) = parser.parse_args()

    if not (options.url_src and options.url_dst and options.blocks):
        parser.print_help()
        parser.error('Mandatory options are --url_src, --url_dst and --blocks')

    return options, args

if __name__ == '__main__':
    options, args = get_command_line_options()
    api_src = DbsApi(url=options.url_src)
    api_dst = DbsApi(url=options.url_dst)

    with open(options.blocks, 'r') as f:
        for block_name in f:
            print("Checking block %s: " % block_name)
            block_dump_src = sorted(api_src.blockDump(block_name=block_name))
            block_dump_dst = sorted(api_dst.blockDump(block_name=block_name))
            if block_dump_src == block_dump_dst:
                print("Ok")
            else:
                print("Failure")
