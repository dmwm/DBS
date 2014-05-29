"""
Tools for the dao layer
"""

def create_lfn_generator(logical_file_name):
    """SQL LFN Generator to select from list of values in Oracle"""
    ###Generator trick from http://betteratoracle.com/posts/20-how-do-i-bind-a-variable-in-list
    ###The maximum length of the comma separated list is 4000 characters, therefore we need to split the list
    ###ORA-01460: unimplemented or unreasonable conversion requested will thrown if list is larger
    oracle_limit = 4000
    lfn_list = []
    lfns = ','.join(logical_file_name)
    if len(lfns) >= oracle_limit:
        index = 0
        while True:
            begin, end = index, index+oracle_limit
            index = lfns.rfind(',', begin, end)
            if index == -1:
                break
            lfn_list.append(lfns[begin:index])
            index += 1 #to remove the leading comma
    else:
        lfn_list.append(lfns)

    lfn_generator = """
    WITH LFN_GENERATOR AS (
    """
    binds = {}
    for index, chunk in enumerate(lfn_list):
        if index:
            lfn_generator += """
            UNION ALL
            """
        bind = "logical_file_name_%s" % index
        lfn_generator += """SELECT REGEXP_SUBSTR(:{bind}, '[^,]+', 1, LEVEL) LOGICAL_FILE_NAME
        FROM DUAL
        CONNECT BY LEVEL <= LENGTH(:{bind}) - LENGTH(REPLACE(:{bind}, ',', '')) + 1
        """.format(bind=bind)
        binds.update({bind: chunk})
    lfn_generator += ")"

    return lfn_generator, binds


def create_token_generator(input_list):
    """SQL Generator to select from list of values in Oracle"""
    ###Generator trick from http://betteratoracle.com/posts/20-how-do-i-bind-a-variable-in-list
    ###The maximum length of the comma separated list is 4000 characters, therefore we need to split the list
    ###ORA-01460: unimplemented or unreasonable conversion requested will thrown if list is larger
    oracle_limit = 4000
    grp_list = []
    input_str = ','.join(input_list)

    if len(input_str) >= oracle_limit:
        index = 0
        while True:
            begin, end = index, index+oracle_limit
	    if end > len(input_str):
                end = len(input_str)
                grp_list.append(input_str[begin:end])
                break
	    else: 	
		index = input_str.rfind(',', begin, end)
		if index == -1:
			break
		grp_list.append(input_str[begin:index])
		index += 1 #to remove the leading comma
    else:
        grp_list.append(input_str)

    token_generator = """
    WITH TOKEN_GENERATOR AS (
    """
    binds = {}
    for index, chunk in enumerate(grp_list):
        if index:
            token_generator += """
            UNION ALL
            """
        bind = "token_%s" % index
        token_generator += """SELECT REGEXP_SUBSTR(:{bind}, '[^,]+', 1, LEVEL) token
        FROM DUAL
        CONNECT BY LEVEL <= LENGTH(:{bind}) - LENGTH(REPLACE(:{bind}, ',', '')) + 1
        """.format(bind=bind)
        binds.update({bind: chunk})
    token_generator += ")"

    return token_generator, binds
