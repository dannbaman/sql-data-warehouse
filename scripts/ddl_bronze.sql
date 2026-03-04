--Create stored procedure to load data from csv files into bronze tables
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    --truncate table first
    TRUNCATE TABLE bronze.crm_cust_info
    BULK INSERT bronze.crm_cust_info
    FROM '/cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    );
    select * from bronze.crm_cust_info
    --count rows
    select count(*) from bronze.crm_cust_info
    --truncate first
    TRUNCATE TABLE bronze.crm_prd_info
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
    TRUNCATE TABLE bronze.crm_sales_details
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
    --truncate first
    TRUNCATE TABLE bronze.erp_cust_az12
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
    TRUNCATE TABLE bronze.erp_loc_a101
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
    --truncate first docker cp /Users/daniel/Downloads/DataWarehouse_data_set/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv sqlserver:/PX_CAT_G1V2.csv
    TRUNCATE TABLE bronze.erp_px_cat_g1v2
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
    END
