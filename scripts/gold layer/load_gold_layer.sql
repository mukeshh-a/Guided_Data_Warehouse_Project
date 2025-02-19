/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean enriched and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
-- Drop the view if it exists
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'dim_customers' AND schemaname = 'gold') THEN
        EXECUTE 'DROP VIEW gold.dim_customers';
    END IF;
END $$;

-- Create the view
CREATE OR REPLACE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key -- Surrogate key
    ,ci.cst_id AS customer_id
    ,ci.cst_key AS customer_number
    ,ci.cst_first_name AS first_name
    ,ci.cst_last_name AS last_name
    ,loc.cntry AS country
    ,ci.cst_marital_status AS marital_status
    ,CASE 
        WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'N/A') -- Fallback to ERP data
    END AS gender
    ,ca.bdate AS birthdate
    ,ci.cst_create_date AS create_date
FROM
    silver.crm_cust_info ci
LEFT JOIN
    silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN
    silver.erp_loc_a101 loc ON ci.cst_key = loc.cid;


-- Create Dimension: gold.dim_products

-- Drop the view if it exists
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'dim_products' AND schemaname = 'gold') THEN
        EXECUTE 'DROP VIEW gold.dim_products';
    END IF;
END $$;

-- Create the view
CREATE OR REPLACE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key -- Surrogate key
    ,pi.prd_id AS product_id
    ,pi.prd_key AS product_number
    ,pi.prd_nm AS product_name
    ,pi.cat_id AS category_id
    ,pc.cat AS category
    ,pc.subcat AS subcategory
    ,pc.maintenance AS maintenance
    ,pi.prd_cost AS cost
    ,pi.prd_line AS product_line
    ,pi.prd_start_dt AS start_date
FROM
    silver.crm_prd_info pi
LEFT JOIN
    silver.erp_px_cat_g1v2 pc ON pi.cat_id = pc.id
WHERE
    pi.prd_end_dt IS NULL; 


-- Create Fact Table: gold.fact_sales

-- Drop the view if it exists
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM pg_views WHERE viewname = 'fact_sales' AND schemaname = 'gold') THEN
        EXECUTE 'DROP VIEW gold.fact_sales';
    END IF;
END $$;

-- Create the view
CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT
    csd.sls_ord_num  AS order_number
    ,dp.product_key  AS product_key
    ,dc.customer_key AS customer_key
    ,csd.sls_order_dt AS order_date
    ,csd.sls_ship_dt  AS shipping_date
    ,csd.sls_due_date   AS due_date
    ,csd.sls_sales    AS sales_amount
    ,csd.sls_quantity AS quantity
    ,csd.sls_price    AS price
FROM
	silver.crm_sales_details csd
LEFT JOIN
	gold.dim_products dp ON csd.sls_prd_key = dp.product_number
LEFT JOIN
	gold.dim_customers dc ON csd.sls_cust_id = dc.customer_id;

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    