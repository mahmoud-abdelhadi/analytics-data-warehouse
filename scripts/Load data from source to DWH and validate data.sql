/*
================================
Load data from the source
================================
Script Purpose:
    use bulk insert to load the data from the source to the DWH.
    bulk insert: load massive amount of data very quickly.
    delete all data from the table before loading to aviod dublication if the script runs more than one time.
*/

truncate table bronze.crm_cust_info
bulk insert bronze.crm_cust_info
from 'D:\Cources\CRM_source\cust_info.csv'
with(
    firstrow = 2,
    fieldterminator = ','
);
------------------------------------

truncate table bronze.crm_prd_info
bulk insert bronze.crm_prd_info
from 'D:\Cources\CRM_source\prd_info.csv'
with(
    firstrow = 2,
    fieldterminator = ','
);
------------------------------------

truncate table bronze.crm_sales_details
bulk insert bronze.crm_sales_details
from 'D:\Cources\CRM_source\sales_details.csv'
with(
    firstrow = 2,
    fieldterminator = ','
);
---------------------------------------

truncate table bronze.erp_cust_az12
bulk insert bronze.erp_cust_az12
from 'D:\Cources\ERP_source\CUST_AZ12.csv'
with(
    firstrow = 2,
    fieldterminator = ','
);
----------------------------------------

truncate table bronze.erp_loc_a101
bulk insert bronze.erp_loc_a101
from 'D:\Cources\ERP_source\LOC_A101.csv'
with(
    firstrow = 2,
    fieldterminator = ','
);
----------------------------------------

truncate table bronze.erp_px_cat_g1v2
bulk insert bronze.erp_px_cat_g1v2
from 'D:\Cources\ERP_source\PX_CAT_G1V2.csv'
with(
    firstrow = 2,
    fieldterminator = ','
);


-- Check the tables data
select * from bronze.crm_cust_info
select * from bronze.crm_prd_info
select * from bronze.crm_sales_details
select * from bronze.erp_cust_az12
select * from bronze.erp_loc_a101
select * from bronze.erp_px_cat_g1v2
