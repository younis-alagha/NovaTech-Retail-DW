-- ============================================
-- 02_silver.sql
-- NovaTech Retail DW - Silver Layer
-- ============================================

USE NovaTechRetailDW;
GO

-- Create Silver Schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

PRINT '--------------------------------------------';
PRINT 'SILVER LAYER — Cleaning and transforming';
PRINT '--------------------------------------------';
GO

/* =========================================================
   SILVER TABLES
========================================================= */

-- ========================
-- Employees
-- ========================
IF OBJECT_ID('silver.employees', 'U') IS NOT NULL
    DROP TABLE silver.employees;

CREATE TABLE silver.employees (
    employee_id      INT,
    first_name       NVARCHAR(255),
    last_name        NVARCHAR(255),
    position         NVARCHAR(255),
    department       NVARCHAR(255),
    store            NVARCHAR(255),
    hire_date        DATE,
    hourly_rate      NVARCHAR(50),
    employment_type  NVARCHAR(50),
    manager_id       NVARCHAR(50)
);
GO

WITH cleaned AS (
    SELECT
        TRY_CAST(employee_id AS INT) AS employee_id,
        UPPER(TRIM(first_name)) AS first_name,
        UPPER(TRIM(last_name)) AS last_name,
        COALESCE(position, 'N/A') AS position,
        CASE
            WHEN UPPER(TRIM(department)) LIKE '%APLL%' THEN 'APPLIANCES'
            WHEN UPPER(TRIM(department)) LIKE '%MOBL%' THEN 'MOBILE'
            ELSE UPPER(TRIM(department))
        END AS department,
        store,
        COALESCE(
            TRY_CONVERT(DATE, hire_date, 120),
            TRY_CONVERT(DATE, hire_date, 105)
        ) AS hire_date,
        COALESCE(hourly_rate, 'N/A') AS hourly_rate,
        employment_type,
        manager_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                UPPER(TRIM(first_name)),
                UPPER(TRIM(last_name)),
                COALESCE(
                    TRY_CONVERT(DATE, hire_date, 120),
                    TRY_CONVERT(DATE, hire_date, 105)
                )
            ORDER BY TRY_CAST(employee_id AS INT)
        ) AS row_num
    FROM bronze.raw_employees
)
INSERT INTO silver.employees
SELECT
    employee_id,
    first_name,
    last_name,
    position,
    department,
    store,
    hire_date,
    hourly_rate,
    employment_type,
    manager_id
FROM cleaned
WHERE row_num = 1
  AND employee_id IS NOT NULL;
GO

-- ========================
-- Products
-- ========================
IF OBJECT_ID('silver.products', 'U') IS NOT NULL
    DROP TABLE silver.products;

CREATE TABLE silver.products (
    product_id          INT,
    product_name        NVARCHAR(500),
    category            NVARCHAR(255),
    brand               NVARCHAR(255),
    price               DECIMAL(10,2),
    protection_eligible NVARCHAR(10),
    margin_pct          DECIMAL(5,2)
);
GO

WITH cleaned AS (
    SELECT
        TRY_CAST(product_id AS INT) AS product_id,
        UPPER(TRIM(product_name)) AS product_name,
        UPPER(TRIM(category)) AS category,
        UPPER(TRIM(brand)) AS brand,
        TRY_CAST(price AS DECIMAL(10,2)) AS price,
        CASE
            WHEN UPPER(TRIM(protection_eligible)) IN ('YES','Y','1','TRUE') THEN 'YES'
            ELSE 'NO'
        END AS protection_eligible,
        TRY_CAST(REPLACE(TRIM(margin_pct), CHAR(13), '') AS DECIMAL(5,2)) AS margin_pct,
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(product_name))
            ORDER BY TRY_CAST(product_id AS INT)
        ) AS row_num
    FROM bronze.raw_products
    WHERE product_name IS NOT NULL
)
INSERT INTO silver.products
SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    protection_eligible,
    margin_pct
FROM cleaned
WHERE row_num = 1
  AND product_id IS NOT NULL;
GO

-- ========================
-- POS Transactions
-- ========================
IF OBJECT_ID('silver.pos_transactions', 'U') IS NOT NULL
    DROP TABLE silver.pos_transactions;

CREATE TABLE silver.pos_transactions (
    record_id            INT,
    employee_id          INT,
    year                 INT,
    month_num            INT,
    month_name           NVARCHAR(50),
    sale_date            DATE,
    hours_worked         INT,
    product_id           INT,
    product_name         NVARCHAR(500),
    sale_amount          DECIMAL(10,2),
    quantity             INT,
    protection_eligible  NVARCHAR(10),
    protection_plan_sold INT,
    membership_sold      INT,
    service_sold         INT,
    is_return            INT,
    store                NVARCHAR(255)
);
GO

WITH cleaned AS (
    SELECT
        TRY_CAST(record_id AS INT)             AS record_id,
        TRY_CAST(employee_id AS INT)           AS employee_id,
        TRY_CAST(year AS INT)                  AS year,
        TRY_CAST(month_num AS INT)             AS month_num,
        UPPER(TRIM(month_name))                AS month_name,
        COALESCE(
            TRY_CONVERT(DATE, sale_date, 120),
            TRY_CONVERT(DATE, sale_date, 103)
        )                                      AS sale_date,
        TRY_CAST(hours_worked AS INT)          AS hours_worked,
        TRY_CAST(product_id AS INT)            AS product_id,
        UPPER(TRIM(product_name))              AS product_name,
        TRY_CAST(sale_amount AS DECIMAL(10,2)) AS sale_amount,
        TRY_CAST(quantity AS INT)              AS quantity,
        CASE
            WHEN UPPER(TRIM(protection_eligible)) IN ('YES','Y','1','TRUE') THEN 'YES'
            ELSE 'NO'
        END                                    AS protection_eligible,
        TRY_CAST(protection_plan_sold AS INT)  AS protection_plan_sold,
        TRY_CAST(membership_sold AS INT)       AS membership_sold,
        TRY_CAST(service_sold AS INT)          AS service_sold,
        TRY_CAST(is_return AS INT)             AS is_return,
        store,
        ROW_NUMBER() OVER (
            PARTITION BY
                TRY_CAST(record_id AS INT),
                TRY_CAST(employee_id AS INT),
                COALESCE(
                    TRY_CONVERT(DATE, sale_date, 120),
                    TRY_CONVERT(DATE, sale_date, 103)
                ),
                TRY_CAST(product_id AS INT)
            ORDER BY TRY_CAST(record_id AS INT)
        ) AS row_num
    FROM bronze.raw_pos_transactions
    WHERE TRY_CAST(employee_id AS INT) IS NOT NULL
      AND TRY_CAST(sale_amount AS DECIMAL(10,2)) IS NOT NULL
      AND TRY_CAST(sale_amount AS DECIMAL(10,2)) <> 0
)
INSERT INTO silver.pos_transactions
SELECT
    record_id,
    employee_id,
    year,
    month_num,
    month_name,
    sale_date,
    hours_worked,
    product_id,
    product_name,
    sale_amount,
    quantity,
    protection_eligible,
    protection_plan_sold,
    membership_sold,
    service_sold,
    is_return,
    store
FROM cleaned
WHERE row_num = 1;
GO

PRINT '>> Silver layer load complete';
GO
