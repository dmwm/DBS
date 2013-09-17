#!/usr/bin/env python
"""
_listRunLumiInfo_

Give the dataset path and DBS Instance url (reader), it will
list all the runs and lumi section in the dataset.

"""

from optparse import OptionParser
from optparse import OptionGroup
import logging
import pprint

from dbs.apis.dbsClient import DbsApi

def get_command_line_options():
    parser = OptionParser(usage='%prog --dataset=</specify/dataset/path> --url=<DBS_Instance_URL> + optional arguments')
    parser.add_option("-u", "--url", dest="url", help="DBS Instance url", metavar="DBS_Instance_URL")
    parser.add_option("-d", "--dataset", dest="dataset", help="Dataset to consider",
                      metavar="/specify/dataset/path")

    group = OptionGroup(parser, "optional arguments")
    group.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Increase verbosity")
    group.add_option("-p", "--proxy", dest="proxy", help="Use Socks5 proxy to connect to server",
                      metavar="socks5://127.0.0.1:1234")

    parser.add_option_group(group)
    (options, args) = parser.parse_args()

    if not (options.url and options.dataset):
        parser.print_help()
        parser.error('Mandatory options are --dataset and --url')

    return options, args

def list_blocks(dataset):
    return (block['block_name'] for block in api.listBlocks(dataset=dataset))

if __name__ == "__main__":
    options, args = get_command_line_options()

    log_level = logging.DEBUG if options.verbose else logging.INFO
    logging.basicConfig(format='%(message)s', level=log_level)

    api = DbsApi(url=options.url, proxy=options.proxy)

    result = list()

    for block in list_blocks(options.dataset):
        logging.debug("Checking block %s" % block)
        result.append(api.listFileLumis(block_name=block))

    pprint.pprint(result)

    logging.debug("Done")
