DBS release notes
====================
DBS_0_3_12
Below are the changes in DBS_0_3_12
1. Insert file api now can insert files in different datasets or blocks.
2. No longer using IS_DATASET_VALID attribute in dataset. The defined dataset_access_types are: PRODUCTION, DEPRECATED, DELETED,
   VALID and INVALID.
3. PREP_ID is added to dataset .
4. Auto generated DOC when building rpm.
6. Deployment tests are updated with pre-loaded test data.
7. Client authorization is improved by adding more ways to authorize a client. 
8. Client APIs are updated to what the server APIs are.
