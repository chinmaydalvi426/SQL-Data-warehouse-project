/*
  Stored procedure : Loading the Bronze layer 
  This stored procedure loads the data from  the external CSV files into the the bronze layer 
      1) Truncates the bronze table before loading data 
      2) Loads the entire data ino the Bronze table using the BULK INSERT command.

  USAGE EXAMPLE 
EXEC bronze.load_bronze;

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME ,@end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME;
	BEGIN TRY 
		SET @batch_start_time = GETDATE();
		PRINT '=====================================================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '=====================================================================';

		PRINT '=====================================================================';
		PRINT 'LOADING CRM TABLES';
		PRINT '=====================================================================';



		SET @start_time = GETDATE();
		PRINT 'TRUNCATING TABLE: bronze.crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT 'INSERTING DATA INTO : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\DataWare_House\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Load duration : '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		PRINT '------------------------------------------------------------------------------------';



		SET @start_time = GETDATE();
		PRINT 'TRUNCATING TABLE: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT 'INSERTING DATA INTO : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\DataWare_House\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Load duration : '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		PRINT '------------------------------------------------------------------------------------';



		SET @start_time = GETDATE();
		PRINT 'TRUNCATING TABLE: bronze.crm_sales_details ';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT 'INSERTING DATA INTO : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\DataWare_House\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Load duration : '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		PRINT '------------------------------------------------------------------------------------';





		PRINT '=====================================================================';
		PRINT 'LOADING ERP TABLES';
		PRINT '=====================================================================';

		SET @start_time = GETDATE();
		PRINT 'TRUNCATING TABLE: bronze.erp_CUST_AZ12 ';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		PRINT 'INSERTING DATA INTO : bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'D:\DataWare_House\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Load duration : '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		PRINT '------------------------------------------------------------------------------------';



		SET @start_time = GETDATE();
		PRINT 'TRUNCATING TABLE: bronze.erp_PX_CAT_G1V2 ';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		PRINT 'INSERTING DATA INTO : bronze.erp_PX_CAT_G1V2 ';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'D:\DataWare_House\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Load duration : '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		PRINT '------------------------------------------------------------------------------------';



		SET @start_time = GETDATE();
		PRINT 'TRUNCATING TABLE: bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;
		PRINT 'INSERTING DATA INTO : bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'D:\DataWare_House\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT '>> Load duration : '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		PRINT '------------------------------------------------------------------------------------';

		SET @batch_end_time = GETDATE();

		PRINT'=================================';
		PRINT'LOADING BRONZE LAYER IS COMPLETED';
		PRINT'  Total Load Duration: '+   CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR);
		PRINT'=================================';

	END TRY
	BEGIN CATCH 
		PRINT'================================================================================'
		PRINT' ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT'================================================================================'


	END CATCH



END
