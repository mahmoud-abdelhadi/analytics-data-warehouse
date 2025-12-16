/*
=====================
Create DB and Schemas
=====================
Script Purpose:
This script creates a new database named 'DataWarehouse',Additionally, the script sets up three schemas     within the database: 'bronze', 'silver', and 'gold'.
*/

-- Create Database 'Data Warehouse'

-- System DB in SQL server when you can go and create other DBs
use master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create our DWH
create database DataWarehouse;
GO

use DataWarehouse;
GO

-- Create Schemas (like folder or container that help you to keep things organized)
-- We will create schema for each layer (Bronze, Silver, Gold)
create schema bronze;
GO
create schema silver;
GO
create schema gold;
GO
