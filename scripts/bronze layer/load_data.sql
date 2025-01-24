/*
-- LOADING THE BRONZE LAYER
=====================================================================================================

-- Loading the bronze layer tables with the required data from the CRM and ERP sources available.

*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
        RAISE NOTICE 'Loading Bronze Layer';
        RAISE NOTICE '============================================';

        -- Record the overall start time
        overall_start_time := CURRENT_TIMESTAMP;

        -- CRM Tables Processing
        RAISE NOTICE '====================';
        RAISE NOTICE 'Processing CRM Tables';
        RAISE NOTICE '====================';

        -- Truncate and load crm_cust_info
        RAISE NOTICE 'Truncating and Inserting data into crm_cust_info table...';
        TRUNCATE TABLE bronze.crm_cust_info;
        SELECT COUNT(*) INTO rows_before FROM bronze.crm_cust_info;
        
        -- Capture start time
        start_time := CURRENT_TIMESTAMP;
        
        COPY bronze.crm_cust_info
        FROM 'C:/Users/mukes/OneDrive/Desktop/VSCode/data_warehouse_project/datasets/source_crm/cust_info.csv'
        WITH (FORMAT CSV, HEADER TRUE);
        
        -- Capture end time and calculate duration
        end_time := CURRENT_TIMESTAMP;
        SELECT COUNT(*) INTO rows_after FROM bronze.crm_cust_info;
        rows_inserted := rows_after - rows_before;
        duration := AGE(end_time, start_time);
        
        RAISE NOTICE 'Rows inserted into crm_cust_info: %, Time taken: %', rows_inserted, duration;

        -- Truncate and load crm_prd_info
        RAISE NOTICE 'Truncating and Inserting data into crm_prd_info table...';
        TRUNCATE TABLE bronze.crm_prd_info;
        SELECT COUNT(*) INTO rows_before FROM bronze.crm_prd_info;
        
        -- Capture start time
        start_time := CURRENT_TIMESTAMP;
        
        COPY bronze.crm_prd_info
        FROM 'C:/Users/mukes/OneDrive/Desktop/VSCode/data_warehouse_project/datasets/source_crm/prd_info.csv'
        WITH (FORMAT CSV, HEADER TRUE);
        
        -- Capture end time and calculate duration
        end_time := CURRENT_TIMESTAMP;
        SELECT COUNT(*) INTO rows_after FROM bronze.crm_prd_info;
        rows_inserted := rows_after - rows_before;
        duration := AGE(end_time, start_time);
        
        RAISE NOTICE 'Rows inserted into crm_prd_info: %, Time taken: %', rows_inserted, duration;

        -- Truncate and load crm_sales_details
        RAISE NOTICE 'Truncating and Inserting data into crm_sales_details table...';
        TRUNCATE TABLE bronze.crm_sales_details;
        SELECT COUNT(*) INTO rows_before FROM bronze.crm_sales_details;
        
        -- Capture start time
        start_time := CURRENT_TIMESTAMP;
        
        COPY bronze.crm_sales_details
        FROM 'C:/Users/mukes/OneDrive/Desktop/VSCode/data_warehouse_project/datasets/source_crm/sales_details.csv'
        WITH (FORMAT CSV, HEADER TRUE);
        
        -- Capture end time and calculate duration
        end_time := CURRENT_TIMESTAMP;
        SELECT COUNT(*) INTO rows_after FROM bronze.crm_sales_details;
        rows_inserted := rows_after - rows_before;
        duration := AGE(end_time, start_time);
        
        RAISE NOTICE 'Rows inserted into crm_sales_details: %, Time taken: %', rows_inserted, duration;


        -- ERP Tables Processing
        RAISE NOTICE '====================';
        RAISE NOTICE 'Processing ERP Tables';
        RAISE NOTICE '====================';

        -- Truncate and load erp_cust_az12
        RAISE NOTICE 'Truncating and Inserting data into erp_cust_az12 table...';
        TRUNCATE TABLE bronze.erp_cust_az12;
        SELECT COUNT(*) INTO rows_before FROM bronze.erp_cust_az12;
        
        -- Capture start time
        start_time := CURRENT_TIMESTAMP;
        
        COPY bronze.erp_cust_az12
        FROM 'C:/Users/mukes/OneDrive/Desktop/VSCode/data_warehouse_project/datasets/source_erp/CUST_AZ12.csv'
        WITH (FORMAT CSV, HEADER TRUE);
        
        -- Capture end time and calculate duration
        end_time := CURRENT_TIMESTAMP;
        SELECT COUNT(*) INTO rows_after FROM bronze.erp_cust_az12;
        rows_inserted := rows_after - rows_before;
         duration := AGE(end_time, start_time);
        
        RAISE NOTICE 'Rows inserted into erp_cust_az12: %, Time taken: %', rows_inserted, duration;

        -- Truncate and load erp_loc_a101
        RAISE NOTICE 'Truncating and Inserting data into erp_loc_a101 table...';
        TRUNCATE TABLE bronze.erp_loc_a101;
        SELECT COUNT(*) INTO rows_before FROM bronze.erp_loc_a101;
        
        -- Capture start time
        start_time := CURRENT_TIMESTAMP;
        
        COPY bronze.erp_loc_a101
        FROM 'C:/Users/mukes/OneDrive/Desktop/VSCode/data_warehouse_project/datasets/source_erp/LOC_A101.csv'
        WITH (FORMAT CSV, HEADER TRUE);
        
        -- Capture end time and calculate duration
        end_time := CURRENT_TIMESTAMP;
        SELECT COUNT(*) INTO rows_after FROM bronze.erp_loc_a101;
        rows_inserted := rows_after - rows_before;
        duration := AGE(end_time, start_time);
        
        RAISE NOTICE 'Rows inserted into erp_loc_a101: %, Time taken: %', rows_inserted, duration;

        -- Truncate and load erp_px_cat_g1v2
        RAISE NOTICE 'Truncating and Inserting data into erp_px_cat_g1v2 table...';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        SELECT COUNT(*) INTO rows_before FROM bronze.erp_px_cat_g1v2;
        
        -- Capture start time
        start_time := CURRENT_TIMESTAMP;
        
        COPY bronze.erp_px_cat_g1v2
        FROM 'C:/Users/mukes/OneDrive/Desktop/VSCode/data_warehouse_project/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (FORMAT CSV, HEADER TRUE);
        
        -- Capture end time and calculate duration
        end_time := CURRENT_TIMESTAMP;
        SELECT COUNT(*) INTO rows_after FROM bronze.erp_px_cat_g1v2;
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
        RAISE NOTICE 'Finished Loading Bronze Layer';
        RAISE NOTICE '============================================';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error occurred at the end of the procedure: %', SQLERRM;
    END;

END $$;