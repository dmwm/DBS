"""
Regular expression for DBS input validation
"""
import re

###validation of strings
string_validation_rx = re.compile(r"^[-A-Za-z0-9_]+$")

###validation of datasets
dataset_validation_rx = re.compile(r"^/(\*|[a-zA-Z\*][a-zA-Z0-9_\*\-]{0,100})(/(\*|[a-zA-Z0-9_\.\-\*]{1,100})){0,1}(/(\*|[A-Z\-\*]{1,50})){0,1}$")
