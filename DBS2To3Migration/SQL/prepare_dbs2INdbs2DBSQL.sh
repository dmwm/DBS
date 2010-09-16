cat create-oracle-schema.sql | sed -e "s%CMS_DBS3_READ_ROLE%CMS_DBS_PRODG_READER_ROLE%g" > create-dbs3-schema-in-dbs2-db.sql.1
cat create-dbs3-schema-in-dbs2-db.sql.1 | sed -e "s%CMS_DBS3_WRITE_ROLE%CMS_DBS_PRODG_WRITER_ROLE%g" > create-dbs3-schema-in-dbs2-db.sql.2
cat create-dbs3-schema-in-dbs2-db.sql.2 | sed -e "s%CMS_DBS3_ADMIN_ROLE%CMS_DBS_PRODG_ADMIN_ROLE%g" > create-dbs3-schema-in-dbs2-db.sql
    
rm create-dbs3-schema-in-dbs2-db.sql.1  create-dbs3-schema-in-dbs2-db.sql.2  

