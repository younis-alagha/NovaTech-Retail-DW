-- ============================================
-- 01_bronze.sql
-- NovaTech Retail DW - Bronze Layer
-- ============================================

USE NovaTechRetailDW;
GO

-- Create Bronze Schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
END
GO

-- ============================================
-- Create Raw Tables
-- ============================================

-- Raw Employees
IF OBJECT_ID('bronze.raw_employees', 'U') IS NOT NULL
    DROP TABLE bronze.raw_employees;

CREATE TABLE bronze.raw_employees (
    employee_id      NVARCHAR(50),
    first_name       NVARCHAR(255),
    last_name        NVARCHAR(255),
    position         NVARCHAR(255),
    department       NVARCHAR(255),
    store            NVARCHAR(255),
    hire_date        NVARCHAR(50),
    hourly_rate      NVARCHAR(50),
    employment_type  NVARCHAR(50),
    manager_id       NVARCHAR(50)
);
GO

-- Raw Products
IF OBJECT_ID('bronze.raw_products', 'U') IS NOT NULL
    DROP TABLE bronze.raw_products;

CREATE TABLE bronze.raw_products (
    product_id           NVARCHAR(50),
    product_name         NVARCHAR(500),
    category             NVARCHAR(255),
    brand                NVARCHAR(255),
    price                NVARCHAR(50),
    protection_eligible  NVARCHAR(50),
    margin_pct           NVARCHAR(50)
);
GO

-- Raw POS Transactions
IF OBJECT_ID('bronze.raw_pos_transactions', 'U') IS NOT NULL
    DROP TABLE bronze.raw_pos_transactions;

CREATE TABLE bronze.raw_pos_transactions (
    record_id              NVARCHAR(50),
    employee_id            NVARCHAR(50),
    year                   NVARCHAR(10),
    month_num              NVARCHAR(10),
    month_name             NVARCHAR(50),
    sale_date              NVARCHAR(50),
    hours_worked           NVARCHAR(50),
    product_id             NVARCHAR(50),
    product_name           NVARCHAR(500),
    sale_amount            NVARCHAR(50),
    quantity               NVARCHAR(50),
    protection_eligible    NVARCHAR(50),
    protection_plan_sold   NVARCHAR(50),
    membership_sold        NVARCHAR(50),
    service_sold           NVARCHAR(50),
    is_return              NVARCHAR(50),
    store                  NVARCHAR(255)
);
GO

-- ============================================
-- Load Data from CSV
-- ============================================

PRINT '--------------------------------------------';
PRINT 'BRONZE LAYER — Loading CSV Files';
PRINT '--------------------------------------------';
GO

BULK INSERT bronze.raw_employees
FROM '/Data/Electronics Store/raw_employees.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
GO

BULK INSERT bronze.raw_products
FROM '/Data/Electronics Store/raw_products.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
GO

BULK INSERT bronze.raw_pos_transactions
FROM '/Data/Electronics Store/raw_pos_transactions.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    TABLOCK
);
GO

PRINT '>> Bronze layer load complete';
GO
