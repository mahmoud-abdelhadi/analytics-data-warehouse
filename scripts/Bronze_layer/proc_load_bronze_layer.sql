/*
================================
Stored Procedure: Load data from the source
================================
Script Purpose:
    use bulk insert to load the data from the source to the DWH.
    bulk insert: load massive amount of data very quickly.
    delete all data from the table before loading to aviod dublication if the script runs more than one time.
Usage Example:
    exec bronze.load_bronze_layer;
*/

create or alter procedure bronze.load_bronze_layer as
begin
    declare @start_time datetime, @end_time datetime
    begin try
        set @start_time = GETDATE()
        print'Loading Bronze Layer'
        print'===================='
        print'Loading CRM tables'
        print'--------------------'
        print'truncating table: bronze.crm_cust_info'
        truncate table bronze.crm_cust_info
        print'Loading table: bronze.crm_cust_info'
        bulk insert bronze.crm_cust_info
        from 'D:\Cources\CRM_source\cust_info.csv'
        with(
            firstrow = 2,
            fieldterminator = ','
        );
        ------------------------------------
        print'truncating table: bronze.crm_prd_info'
        truncate table bronze.crm_prd_info
        print'Loading table: bronze.crm_prd_info'
        bulk insert bronze.crm_prd_info
        from 'D:\Cources\CRM_source\prd_info.csv'
        with(
            firstrow = 2,
            fieldterminator = ','
        );
        ------------------------------------
        print'truncating table: bronze.crm_sales_details'
        truncate table bronze.crm_sales_details
        print'truncating table: bronze.crm_sales_details'
        bulk insert bronze.crm_sales_details
        from 'D:\Cources\CRM_source\sales_details.csv'
        with(
            firstrow = 2,
            fieldterminator = ','
        );
        ---------------------------------------
        print'Loading ERP tables'
        print'--------------------'
        print'truncating table: bronze.erp_cust_az12'
        truncate table bronze.erp_cust_az12
        print'Loading table: bronze.erp_cust_az12'
        bulk insert bronze.erp_cust_az12
        from 'D:\Cources\ERP_source\CUST_AZ12.csv'
        with(
            firstrow = 2,
            fieldterminator = ','
        );
        ----------------------------------------
        print'truncating table: bronze.erp_loc_a101'
        truncate table bronze.erp_loc_a101
        print'truncating table: bronze.erp_loc_a101'
        bulk insert bronze.erp_loc_a101
        from 'D:\Cources\ERP_source\LOC_A101.csv'
        with(
            firstrow = 2,
            fieldterminator = ','
        );
        ----------------------------------------
        print'truncating table: bronze.erp_px_cat_g1v2'
        truncate table bronze.erp_px_cat_g1v2
        print'truncating table: bronze.erp_px_cat_g1v2'
        bulk insert bronze.erp_px_cat_g1v2
        from 'D:\Cources\ERP_source\PX_CAT_G1V2.csv'
        with(
            firstrow = 2,
            fieldterminator = ','
        );
        set @end_time = GETDATE()
        print'the load duration ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' Seconds'
    end try
    begin catch
        print'--------------------------------------------'
        print'Error occured during loading the bronze layer'
        print'--------------------------------------------'
        print'Error message:' + cast(error_message() as nvarchar)
    end catch
end
