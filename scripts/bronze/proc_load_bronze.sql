/*
Ini addalah stored procedure, untuk melakukan load data pada layer bronze.
sesuai dengan requirement awal, setiap kali loading, semua data yang ada akan dibuang dulu dan load kembali
data dari file terkait.
untuk setiap proses loading komponen data dari folder ERP ataupun CRM diberikan timer untuk mengukur seberapa lama 
waktu muat dari file terkait.
juga dilengkapi dengan timer untuk menghitung total waktu yang dibutuhkan untuk memuat semua data dari awal hingga akhir.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_ltime DATETIME, @end_ltime DATETIME;
	BEGIN TRY
		PRINT '================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================================';

		Print '----------------------------------------------------------------';
		Print 'Loading CRM Tables';
		Print '----------------------------------------------------------------';

		SET @start_ltime = GETDATE();
		SET @start_time = GETDATE();
		Print '>> Truncating Table: bronze.crm_cust_info' 
		TRUNCATE TABLE bronze.crm_cust_info;

		Print '>> Inserting data into: bronze.crm_cust_info' 
		BULK INSERT bronze.crm_cust_info
		from 'D:\SQL Dataplace\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------- <<'

		SET @start_time = GETDATE();
		Print '>> Truncating Table: bronze.crm_prd_info' 
		TRUNCATE TABLE bronze.crm_prd_info;

		Print '>> Inserting data into: bronze.crm_prd_info' 
		BULK INSERT bronze.crm_prd_info
		from 'D:\SQL Dataplace\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------- <<'

		SET @start_time = GETDATE();
		Print '>> Truncating Table: bronze.crm_sales_details' 
		TRUNCATE TABLE bronze.crm_sales_details;

		Print '>> Inserting data into: bronze.crm_sales_details' 

		BULK INSERT bronze.crm_sales_details
		from 'D:\SQL Dataplace\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------- <<'

		

		Print '----------------------------------------------------------------';
		Print 'Loading CRM Tables';
		Print '----------------------------------------------------------------';

		SET @start_time = GETDATE();
		Print '>> Truncating Table: bronze.erp_cust_az12' 
		TRUNCATE TABLE bronze.erp_cust_az12;

		Print '>> Inserting data into: bronze.erp_cust_az12' 

		BULK INSERT bronze.erp_cust_az12
		from 'D:\SQL Dataplace\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------- <<'

		SET @start_time = GETDATE();

		Print '>> Truncating Table: bronze.erp_loc_a101' 
		TRUNCATE TABLE bronze.erp_loc_a101;

		Print '>> Inserting data into: bronze.erp_loc_a101' 

		BULK INSERT bronze.erp_loc_a101
		from 'D:\SQL Dataplace\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------- <<'

		SET @start_time = GETDATE();

		Print '>> Truncating Table: bronze.erp_px_cat_g1v2' 
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		Print '>> Inserting data into: bronze.erp_px_cat_g1v2' 

		BULK INSERT bronze.erp_px_cat_g1v2
		from 'D:\SQL Dataplace\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		SET @end_ltime = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------- <<'
		PRINT '>>>> TOTAL BRONZE LAYER LOAD DURATION: ' + CAST(DATEDIFF(second, @start_ltime, @end_ltime) AS NVARCHAR) + ' seconds';
	END TRY

	BEGIN CATCH
		PRINT '================================================================';
		PRINT 'Error Occured During Loading Bronze Layer !!!';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '================================================================';
	END CATCH
END
