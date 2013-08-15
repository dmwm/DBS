#!/usr/bin/env python
"""
_setDatasetStatus_

Give the dataset path, the new status and DBS Instance url (writer), it will
set the new status. If the children option is used, the status of all its
children will be changed as well

"""
from optparse import OptionParser
import logging
import sys
import os
from dbs.apis.dbsClient import DbsApi

def get_command_line_options():
    parser = OptionParser(usage='%prog --dataset=</specify/dataset/path> --status=<newStatus> --url=<DBS_Instance_URL> + optional options')
    parser.add_option("-u", "--url", dest="url", help="DBS Instance url", metavar="DBS_Instance_URL")
    parser.add_option("-r", "--recursive", dest="recursive", help="Invalidate all children datasets,too?", metavar="True/False")
    parser.add_option("-d", "--dataset", dest="dataset", help="Dataset to change status", metavar="/specify/dataset/path")
    parser.add_option("-s", "--status", dest="new_status", help="New status of the dataset", metavar="newStatus")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="Increase verbosity")
    parser.add_option("-p", "--proxy", dest="proxy", help="Use Socks5 proxy to connect to server", metavar="socks5://127.0.0.1:1234")
    (options, args) = parser.parse_args()

    if not (options.url and options.dataset and options.new_status and options.recursive):
        parser.print_help()
        parser.error('Mandatory options are --dataset, --status, --url and --recursive')

    return options, args

def list_dataset_children(dataset):
    for child_dataset in api.listDatasetChildren(dataset=dataset):
        logging.debug('Found children dataset %s' % (child_dataset['child_dataset']))
        for grand_child in list_dataset_children(dataset=child_dataset['child_dataset']):
            yield grand_child
        yield child_dataset['child_dataset']

def update_dataset_type(dataset, new_status):
    logging.debug('Update dataset type for dataset %s to %s' % (dataset, new_status))
    api.updateDatasetType(dataset=dataset, dataset_access_type=new_status)

def update_file_status(dataset, new_status):
    files = api.listFiles(dataset=dataset)

    file_status = (1,0)[options.new_status in ['DELETED', 'DEPRECATED', 'INVALID']]

    for this_file in files:
        logging.debug('Update file status for file %s to status %s' % (this_file['logical_file_name'], file_status))
        api.updateFileStatus(logical_file_name=this_file['logical_file_name'],
                             is_file_valid=file_status)

if __name__ == "__main__":
    options, args = get_command_line_options()

    log_level = logging.DEBUG if options.verbose else logging.INFO
    logging.basicConfig(format='%(message)s', level=log_level)

    api = DbsApi(url=options.url, proxy=options.proxy)

    new_status = options.new_status.upper()

    ###update file status
    update_file_status(dataset=options.dataset, new_status=new_status)

    ###update status of the dataset
    update_dataset_type(dataset=options.dataset, new_status=new_status)

    if options.recursive in ['True','true', '1', 'y', 'yes', 'yeah', 'yup', 'certainly']:
        ###update status of children datasets as well
        for child_dataset in list_dataset_children(options.dataset):
            update_file_status(dataset=child_dataset, new_status=new_status)
            update_dataset_type(dataset=child_dataset, new_status=new_status)

    logging.info("Done")
