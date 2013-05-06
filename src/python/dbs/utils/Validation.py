"""
Regular expression for DBS input validation
"""
import re

###validation of strings
string_validation_rx = re.compile(r"^[-A-Za-z0-9_]+$")
