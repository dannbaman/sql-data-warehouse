
--Create stored procedure to load data from csv files into bronze tables
CREATE OR ALTER  PROCEDURE [bronze].[load_bronze] AS
BEGIN
    DECLARE @START_TIME DATETIME;
    DECLARE @END_TIME DATETIME; 
BEGIN TRY
    PRINT '================================';
    PRINT 'Loading data into bronze tables...';
    PRINT '================================';

    PRINT 'LOADING CRM TABLES DATA  ...';
    --truncate table first
    SET @START_TIME = GETDATE();
    PRINT 'TRUNCATE TABLE bronze.crm_cust_info ...';
    TRUNCATE TABLE bronze.crm_cust_info;
    PRINT 'BULK INSERT DATA INTO : bronze.crm_cust_info ...';
    BULK INSERT bronze.crm_cust_info
    FROM '/cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    );
    SET @END_TIME = GETDATE();
    PRINT 'Time taken to load bronze.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS VARCHAR(10)) + ' seconds';
    select * from bronze.crm_cust_info
    --count rows
    select count(*) from bronze.crm_cust_info
    --truncate first
    PRINT 'TRUNCATE TABLE bronze.crm_prd_info ...';
    TRUNCATE TABLE bronze.crm_prd_info
    PRINT 'BULK INSERT DATA INTO : bronze.crm_prd_info ...';
    BULK INSERT bronze.crm_prd_info
    FROM '/prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    );
    select * from bronze.crm_prd_info
    --count rows
    select count(*) from bronze.crm_prd_info
    --truncate first
    PRINT 'TRUNCATE TABLE bronze.crm_sales_details ...';
    TRUNCATE TABLE bronze.crm_sales_details
    PRINT 'BULK INSERT DATA INTO : bronze.crm_sales_details ...';
    BULK INSERT bronze.crm_sales_details
    FROM '/sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    );
    select * from bronze.crm_sales_details
    --count rows
    select count(*) from bronze.crm_sales_details
    PRINT '================================';
    PRINT 'Loading data into bronze tables...';
    PRINT '================================';

    PRINT 'LOADING ERP TABLES DATA  ...';
    --truncate first
    PRINT 'TRUNCATE TABLE bronze.erp_cust_az12 ...';
    TRUNCATE TABLE bronze.erp_cust_az12
    PRINT 'BULK INSERT DATA INTO : bronze.erp_cust_az12 ...';
    BULK INSERT bronze.erp_cust_az12
    FROM '/CUST_AZ12.csv '
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    );
    select * from bronze.erp_cust_az12
    --count rows
    select count(*) from bronze.erp_cust_az12
    --truncate first /Users/daniel/Downloads/DataWarehouse_data_set/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv
    PRINT 'TRUNCATE TABLE bronze.erp_loc_a101 ...';
    TRUNCATE TABLE bronze.erp_loc_a101
    PRINT 'BULK INSERT DATA INTO : bronze.erp_loc_a101 ...';
    BULK INSERT bronze.erp_loc_a101
    FROM '/LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    );
    select * from bronze.erp_loc_a101
    --count rows
    select count(*) from bronze.erp_loc_a101
    PRINT 'TRUNCATE bronze.erp_px_cat_g1v2'
    --truncate first docker cp /Users/daniel/Downloads/DataWarehouse_data_set/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv sqlserver:/PX_CAT_G1V2.csv
    TRUNCATE TABLE bronze.erp_px_cat_g1v2
    PRINT 'BULK INSERT DATA INTO : bronze.erp_px_cat_g1v2 ...';
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM '/PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    );
    select * from bronze.erp_px_cat_g1v2
    --count rows
    select count(*) from bronze.erp_px_cat_g1v2;
    --TOTAL TIME TAKEN
        PRINT 'Total time taken to load data into bronze tables: ' + CAST(DATEDIFF(SECOND, @START_TIME, GETDATE()) AS VARCHAR(10)) + ' seconds';
    END TRY
    BEGIN CATCH
        PRINT 'Error occurred while loading data into bronze tables: ' + ERROR_MESSAGE();
    END CATCH
END
GO
