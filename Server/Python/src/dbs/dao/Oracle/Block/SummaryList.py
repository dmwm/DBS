"""
This module provides Block.SummaryList data access object
"""

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSDaoTools import create_token_generator

class SummaryList(DBFormatter):
    def __init__(self, logger, dbi, owner=""):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.block_join = "JOIN {owner}BLOCKS BS ON BS.BLOCK_ID=FS.BLOCK_ID".format(owner=self.owner)
        self.dataset_join = "JOIN {owner}DATASETS DS ON BS.DATASET_ID=DS.DATASET_ID".format(owner=self.owner)

    def execute(self, conn, block_name="", dataset="", detail=0, transaction=False):
        binds = {}
        generatedsql=''
        if dataset:
            where_clause = "WHERE DS.dataset=:dataset"
            if detail:
                sql = """
                    with t1 as(
                        SELECT 
                            BS.BLOCK_NAME as BLOCK_NAME, 
                            NVL(SUM(FS.EVENT_COUNT),0) as NUM_EVENT
                        FROM 
                            {owner}FILES FS 
                        {block_join}
                        {dataset_join}
                        {where_clause}
                        group by BS.BLOCK_NAME
                    ) 
                    select 
                        b.block_name as block_name, 
                        b.file_count as num_file, 
                        b.block_size as file_size,
                        t1.num_event as num_event, 
                        b.open_for_writing as open_for_writing
                   from     
                        {owner}blocks b, t1
                   where 
                        t1.block_name = b.block_name
                """.format(owner=self.owner, where_clause=where_clause, dataset_join=self.dataset_join,
                                block_join=self.block_join)
            else:
                sql = """SELECT (
                SELECT NVL(SUM(BS.BLOCK_SIZE), 0)
                FROM {owner}BLOCKS BS {dataset_join} {where_clause}
                ) AS FILE_SIZE,
                (
                SELECT NVL(SUM(BS.FILE_COUNT),0)
                FROM {owner} BLOCKS BS {dataset_join} {where_clause}
                ) AS NUM_FILE,
                (
                    SELECT NVL(SUM(FS.EVENT_COUNT),0)
                    FROM {owner}FILES FS {block_join} {dataset_join} {where_clause}
                ) AS NUM_EVENT
                FROM DUAL""".format(owner=self.owner, where_clause=where_clause, dataset_join=self.dataset_join,
                                block_join=self.block_join)
            binds.update(dataset=dataset)

        else:
            # Oracle IN only supports a maximum of 1,000 values
            # (ORA-01795: maximum number of expressions in a list is 1000)
            if isinstance(block_name, str):
                block_name=[block_name]
            block_clause = "BS.BLOCK_NAME IN (SELECT TOKEN FROM TOKEN_GENERATOR) "
            generatedsql, run_binds = create_token_generator(block_name)
            binds.update(run_binds)
            where_clause = "WHERE {block_clause}".format(block_clause=block_clause)
            if detail: 
                sql = generatedsql + \
                    """  
                    select 
                        b.block_name as block_name, 
                        b.file_count as num_file, 
                        b.block_size as file_size,
                        t1.num_event as num_event, 
                        b.open_for_writing as open_for_writing
                    from 
                        {owner}blocks b, 
                        (select 
                            bs.block_name as block_name, 
                            NVL(sum(fs.event_count),0) as num_event 
                        from
                            {owner}files fs 
                        {block_join} 
                        {where_clause} 
                        group by bs.block_name )t1
                    where 
                        t1.block_name = b.block_name
                    """.format(owner=self.owner, block_join=self.block_join, where_clause=where_clause)
            else:
                sql = generatedsql + \
                """SELECT (
                SELECT NVL(SUM(BS.BLOCK_SIZE),0)
                FROM {owner}BLOCKS BS {where_clause}
                ) AS FILE_SIZE,
                (
                SELECT NVL(SUM(BS.FILE_COUNT),0)
                FROM {owner} BLOCKS BS {where_clause}
                ) AS NUM_FILE,
                (
                SELECT NVL(SUM(FS.EVENT_COUNT),0)
                FROM {owner}FILES FS {block_join} {where_clause}
                ) AS NUM_EVENT
                FROM DUAL""".format(owner=self.owner, where_clause=where_clause, block_join=self.block_join)
        cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)

        result = []
        #self.logger.debug(sql)
        #self.logger.debug(binds)
        for cursor in cursors:
            result.extend(self.formatCursor(cursor))
        return result
