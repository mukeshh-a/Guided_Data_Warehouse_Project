--------------------------------------------------------------------------------------------------------------------
/* Creating Database and Schemas */
--------------------------------------------------------------------------------------------------------------------

-- Drop the database if it exists
DROP DATABASE IF EXISTS data_warehouse;

-- Create the database
CREATE DATABASE data_warehouse;

-- Create Schemas
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;