CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    rows_before INT;  -- Holds the row count before the COPY operation
    rows_after INT;   -- Holds the row count after the COPY operation
    rows_inserted INT; -- Holds the number of rows inserted
    start_time TIMESTAMP; -- Holds the start time for each table load
    end_time TIMESTAMP;   -- Holds the end time for each table load
    duration INTERVAL;    -- Holds the time difference
    overall_start_time TIMESTAMP; -- Holds the overall start time for all tables
    overall_end_time TIMESTAMP;   -- Holds the overall end time for all tables
    total_duration INTERVAL; -- Holds the overall time duration
BEGIN
    -- Error Handling Block: Start of Procedure
    BEGIN
        RAISE NOTICE '============================================';
        RAISE NOTICE 'Loading Silver Layer';
        RAISE NOTICE '============================================';

        -- Record the overall start time
        overall_start_time := CURRENT_TIMESTAMP;

        -- CRM Tables Processing
        RAISE NOTICE '====================';
        RAISE NOTICE 'Processing CRM Tables';
        RAISE NOTICE '====================';

        -- Truncate and load crm_cust_info
        RAISE NOTICE 'Truncating and Inserting data into crm_cust_info table...';
        TRUNCATE TABLE silver.crm_cust_info;
        SELECT COUNT(*) INTO rows_before FROM silver.crm_cust_info;
        
        -- Capture start time
        start_time := CURRENT_TIMESTAMP;
        
        INSERT INTO silver.crm_cust_info (
			cst_id
			,cst_key
			,cst_first_name
			,cst_last_name
			,cst_marital_status
			,cst_gndr
			,cst_create_date
		)
		SELECT DISTINCT ON (cst_id)
			cst_id
			,cst_key
			,TRIM(cst_firstname) AS cst_firstname -- Removing leading and trailing spaces from first name
			,TRIM(cst_lastname) AS cst_lastname -- Removing leading and trailing spaces from last name
			,CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' -- Converting 'S' to 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' -- Converting 'M' to 'Married'
				ELSE 'N/A' -- Assigning a value for unknown marital status
			END AS cst_marital_status
			,CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' -- Converting 'M' to 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' -- Converting 'F' to 'Female'
				ELSE 'N/A' -- Assigning a value for unknown gender
			END AS cst_gndr
			,cst_create_date
		FROM	
			bronze.crm_cust_info
		WHERE
			cst_id IS NOT NULL
		ORDER BY
			cst_id
			,cst_create_date DESC;
		
        -- Capture end time and calculate duration
        end_time := CURRENT_TIMESTAMP;
        SELECT COUNT(*) INTO rows_after FROM silver.crm_cust_info;
        rows_inserted := rows_after - rows_before;
        duration := AGE(end_time, start_time);
        
        RAISE NOTICE 'Rows inserted into crm_cust_info: %, Time taken: %', rows_inserted, duration;

        -- Truncate and load crm_prd_info
        RAISE NOTICE 'Truncating and Inserting data into crm_prd_info table...';
        TRUNCATE TABLE silver.crm_prd_info;
        SELECT COUNT(*) INTO rows_before FROM silver.crm_prd_info;
        
        -- Capture start time
        start_time := CURRENT_TIMESTAMP;
        

		INSERT INTO silver.crm_prd_info (
					prd_id
					,cat_id
					,prd_key
					,prd_nm
					,prd_cost
					,prd_line
					,prd_start_dt
					,prd_end_dt
		)
        SELECT
        	prd_id
        	,REPLACE(SUBSTRING(prd_key FROM 1 FOR 5),'-', '_') AS cat_id -- Extract category id
        	,SUBSTRING(prd_key FROM 7 FOR LENGTH(prd_key)) AS prd_key -- Extract product id
        	,prd_nm
        	,COALESCE(prd_cost, 0) AS prd_cost -- Handling null VALUES
            ,CASE 
		        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		    	ELSE 'N/A'
    		END AS prd_line -- Mapping product line codes to descriptive VALUES
    		,prd_start_dt::DATE AS prd_start_dt
    		,(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::DATE AS prd_end_dt -- Calculating end date as one day before the next start date 
        FROM
        	bronze.crm_prd_info;
        
        
        -- Capture end time and calculate duration
        end_time := CURRENT_TIMESTAMP;
        SELECT COUNT(*) INTO rows_after FROM silver.crm_prd_info;
        rows_inserted := rows_after - rows_before;
        duration := AGE(end_time, start_time);
        
        RAISE NOTICE 'Rows inserted into crm_prd_info: %, Time taken: %', rows_inserted, duration;
        
 		-- Truncate and load crm_sales_details
        RAISE NOTICE 'Truncating and Inserting data into crm_sales_details table...';
        TRUNCATE TABLE silver.crm_sales_details;
        SELECT COUNT(*) INTO rows_before FROM silver.crm_sales_details;
        
        -- Capture start time
        start_time := CURRENT_TIMESTAMP;
        
        
        INSERT INTO silver.crm_sales_details (
			sls_ord_num
			,sls_prd_key
			,sls_cust_id
			,sls_order_dt
			,sls_ship_dt
			,sls_due_date
			,sls_sales
			,sls_quantity
			,sls_price
		)
        SELECT
        	sls_ord_num
        	,sls_prd_key
        	,sls_cust_id
        	,CASE
        		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
        		ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
        	END sls_order_dt
        	,CASE
        		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
        		ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
        	END sls_ship_dt
        	,CASE
        		WHEN sls_due_date = 0 OR LENGTH(sls_due_date::TEXT) != 8 THEN NULL
        		ELSE TO_DATE(sls_due_date::TEXT, 'YYYYMMDD')
        	END sls_due_date
        	,CASE
        		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity *  ABS(sls_price) THEN sls_quantity * ABS(sls_price)
        		ELSE sls_sales
        	END AS sls_sales
        	,sls_quantity
        	,CASE
        		WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
        	END AS sls_price
        FROM
        	bronze.crm_sales_details;
        
        -- Capture end time and calculate duration
        end_time := CURRENT_TIMESTAMP;
        SELECT COUNT(*) INTO rows_after FROM silver.crm_sales_details;
        rows_inserted := rows_after - rows_before;
        duration := AGE(end_time, start_time);
        
        RAISE NOTICE 'Rows inserted into crm_sales_details: %, Time taken: %', rows_inserted, duration;
        

        -- ERP Tables Processing
        RAISE NOTICE '====================';
        RAISE NOTICE 'Processing ERP Tables';
        RAISE NOTICE '====================';

        -- Truncate and load erp_cust_az12
        RAISE NOTICE 'Truncating and Inserting data into erp_cust_az12 table...';
        TRUNCATE TABLE silver.erp_cust_az12;
        SELECT COUNT(*) INTO rows_before FROM silver.erp_cust_az12;
        
        -- Capture start time
        start_time := CURRENT_TIMESTAMP;
        
        INSERT INTO silver.erp_cust_az12 (
        	cid
        	,bdate
        	,gen
        )
        SELECT
        	CASE
        		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4 FOR LENGTH(cid)) -- Removing 'NAS' prefix if present
        		ELSE cid
        	END AS cid
        	,CASE
        		WHEN bdate > CURRENT_TIMESTAMP THEN NULL
        		ELSE bdate
        	END AS bdate -- Setting future birthdates TO NULL
        	,CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'N/A'
			END AS gen -- Normalizing gender values and handle unknown cases
        FROM
        	bronze.erp_cust_az12;
        
        -- Capture end time and calculate duration
        end_time := CURRENT_TIMESTAMP;
        SELECT COUNT(*) INTO rows_after FROM silver.erp_cust_az12;
        rows_inserted := rows_after - rows_before;
         duration := AGE(end_time, start_time);
        
        RAISE NOTICE 'Rows inserted into erp_cust_az12: %, Time taken: %', rows_inserted, duration;

        -- Truncate and load erp_loc_a101
        RAISE NOTICE 'Truncating and Inserting data into erp_loc_a101 table...';
        TRUNCATE TABLE silver.erp_loc_a101;
        SELECT COUNT(*) INTO rows_before FROM silver.erp_loc_a101;
        
        -- Capture start time
        start_time := CURRENT_TIMESTAMP;
        
        INSERT INTO silver.erp_loc_a101 (
			cid
			,cntry
        )
        SELECT
        	REPLACE(cid,'-','_') AS cid
        	,CASE
        		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
				ELSE TRIM(cntry)
			END AS cntry -- Normalizing and Handling missing or blank country codes
        FROM
        	bronze.erp_loc_a101;
        
        -- Capture end time and calculate duration
        end_time := CURRENT_TIMESTAMP;
        SELECT COUNT(*) INTO rows_after FROM silver.erp_loc_a101;
        rows_inserted := rows_after - rows_before;
        duration := AGE(end_time, start_time);
        
        RAISE NOTICE 'Rows inserted into erp_loc_a101: %, Time taken: %', rows_inserted, duration;

        
        -- Truncate and load erp_px_cat_g1v2
        RAISE NOTICE 'Truncating and Inserting data into erp_px_cat_g1v2 table...';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        SELECT COUNT(*) INTO rows_before FROM silver.erp_px_cat_g1v2;
        
        -- Capture start time
        start_time := CURRENT_TIMESTAMP;
        
        
        INSERT INTO silver.erp_px_cat_g1v2 (
          	id
        	,cat
        	,subcat
        	,maintenance
        )
        SELECT
        	id
        	,cat
        	,subcat
        	,maintenance
        FROM
          	bronze.erp_px_cat_g1v2;
        
        -- Capture end time and calculate duration
        end_time := CURRENT_TIMESTAMP;
        SELECT COUNT(*) INTO rows_after FROM silver.erp_px_cat_g1v2;
        rows_inserted := rows_after - rows_before;
        duration := AGE(end_time, start_time);
        
        RAISE NOTICE 'Rows inserted into erp_px_cat_g1v2: %, Time taken: %', rows_inserted, duration;

        -- Record the overall end time
        overall_end_time := CURRENT_TIMESTAMP;

         -- Calculate the total time taken for all tables
        total_duration := AGE(overall_end_time, overall_start_time);
        
        RAISE NOTICE '============================================';
        RAISE NOTICE 'Total Time Taken for All Tables: %', total_duration;
        RAISE NOTICE '============================================';

    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error occurred during the load process: %', SQLERRM;
    END;

    -- Error Handling Block: End of Procedure
    BEGIN
        RAISE NOTICE '============================================';
        RAISE NOTICE 'Finished Loading Silver Layer';
        RAISE NOTICE '============================================';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error occurred at the end of the procedure: %', SQLERRM;
    END;

END $$;       
        
        
        
        
        
        
        