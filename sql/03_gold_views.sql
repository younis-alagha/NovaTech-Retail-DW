-- ============================================
-- 03_gold_views.sql
-- NovaTech Retail DW - Gold Layer
-- ============================================

USE NovaTechRetailDW;
GO

-- Create Gold Schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END
GO

/* =========================================================
   GOLD VIEWS
========================================================= */

-- ============================================
-- vw_employee_monthly
-- How did each employee perform this month?
-- ============================================
CREATE OR ALTER VIEW gold.vw_employee_monthly AS
SELECT 
    s.month_num,
    s.year,
    s.employee_id,
    e.first_name,
    e.last_name,
    e.employment_type,
    e.position,
    e.department,
    MAX(s.hours_worked) AS hours_worked,
    SUM(s.sale_amount) AS total_sales,
    CAST(
        ROUND(
            CAST(SUM(CASE WHEN s.is_return = 0 THEN s.sale_amount ELSE 0 END) AS DECIMAL(10,2))
            / NULLIF(MAX(s.hours_worked), 0),
            2
        ) AS DECIMAL(10,2)
    ) AS sales_per_hour,
    COALESCE(
        CAST(
            ROUND(
                CAST(SUM(s.protection_plan_sold) AS DECIMAL(10,2))
                / NULLIF(CAST(SUM(CASE WHEN s.protection_eligible = 'YES' THEN 1 ELSE 0 END) AS DECIMAL(10,2)), 0)
                * 100,
                2
            ) AS DECIMAL(10,2)
        ),
        0
    ) AS protection_attach_rate,
    CAST(
        CAST(SUM(s.membership_sold) AS DECIMAL(10,2))
        / NULLIF(COUNT(s.quantity), 0) * 100
        AS DECIMAL(5,2)
    ) AS membership_attach,
    CAST(
        CAST(SUM(s.service_sold) AS DECIMAL(10,2))
        / NULLIF(COUNT(s.quantity), 0) * 100
        AS DECIMAL(5,2)
    ) AS service_attach_rate,
    CAST(
        CAST(SUM(s.is_return) AS DECIMAL(10,2))
        / NULLIF(COUNT(s.quantity), 0) * 100
        AS DECIMAL(5,2)
    ) AS returns_rate
FROM silver.pos_transactions s
JOIN silver.employees e
    ON s.employee_id = e.employee_id
GROUP BY 
    s.month_num,
    s.year,
    s.employee_id,
    e.first_name,
    e.last_name,
    e.employment_type,
    e.position,
    e.department;
GO

-- ============================================
-- vw_product_performance
-- What sells, what gets returned, what attaches?
-- ============================================
CREATE OR ALTER VIEW gold.vw_product_performance AS
SELECT 
    year,
    month_num,
    product_id,
    product_name,
    SUM(sale_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
    SUM(CASE WHEN protection_eligible = 'YES' THEN 1 ELSE 0 END) AS total_eligible_units,
    SUM(protection_plan_sold) AS total_protection_sold,
    COALESCE(
        CAST(
            ROUND(
                CAST(SUM(protection_plan_sold) AS DECIMAL(10,2))
                / NULLIF(CAST(SUM(CASE WHEN protection_eligible = 'YES' THEN 1 ELSE 0 END) AS DECIMAL(10,2)), 0)
                * 100,
                2
            ) AS DECIMAL(10,2)
        ),
        0
    ) AS protection_attach_rate,
    CAST(
        CAST(SUM(is_return) AS DECIMAL(10,2))
        / NULLIF(SUM(quantity), 0) * 100
        AS DECIMAL(5,2)
    ) AS returns_rate
FROM silver.pos_transactions
GROUP BY
    year,
    month_num,
    product_id,
    product_name;
GO

-- ============================================
-- vw_coaching_targets
-- Who needs coaching and why?
-- ============================================
CREATE OR ALTER VIEW gold.vw_coaching_targets AS
WITH ranked AS (
    SELECT
        month_num,
        year,
        employee_id,
        first_name,
        last_name,
        position,
        department,
        sales_per_hour,
        protection_attach_rate,
        ROUND(PERCENT_RANK() OVER (PARTITION BY month_num, year ORDER BY sales_per_hour), 2) AS sales_per_hour_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY month_num, year ORDER BY protection_attach_rate), 2) AS protection_attach_rank,
        LAG(sales_per_hour) OVER (PARTITION BY employee_id ORDER BY year, month_num) AS prev_mth_sales_per_hour,
        LAG(protection_attach_rate) OVER (PARTITION BY employee_id ORDER BY year, month_num) AS prev_mth_protection_attach
    FROM gold.vw_employee_monthly
)
SELECT
    year,
    month_num,
    employee_id,
    first_name,
    last_name,
    position,
    department,
    sales_per_hour,
    sales_per_hour_rank,
    prev_mth_sales_per_hour,
    ROUND(sales_per_hour - prev_mth_sales_per_hour, 2) AS sales_per_hour_change,
    protection_attach_rate,
    protection_attach_rank,
    prev_mth_protection_attach,
    ROUND(protection_attach_rate - prev_mth_protection_attach, 2) AS protection_change,
    CASE 
        WHEN ROUND(protection_attach_rate - prev_mth_protection_attach, 2) < 0 THEN 'Declining'
        WHEN ROUND(protection_attach_rate - prev_mth_protection_attach, 2) > 0 THEN 'Improving'
        ELSE 'Stable'
    END AS attach_trend,
    'High seller - Low attach' AS coaching_reason
FROM ranked
WHERE sales_per_hour_rank >= 0.70
  AND protection_attach_rank <= 0.30
  AND protection_attach_rate > 0;
GO

-- ============================================
-- vw_employee_trend
-- Is each employee improving over time?
-- ============================================
CREATE OR ALTER VIEW gold.vw_employee_trend AS
WITH lagged AS (
    SELECT
        year,
        month_num,
        employee_id,
        first_name,
        last_name,
        department,
        total_sales,
        LAG(total_sales) OVER (PARTITION BY employee_id ORDER BY year, month_num) AS prev_total_sales,
        sales_per_hour,
        LAG(sales_per_hour) OVER (PARTITION BY employee_id ORDER BY year, month_num) AS prev_sales_per_hr,
        protection_attach_rate,
        LAG(protection_attach_rate) OVER (PARTITION BY employee_id ORDER BY year, month_num) AS prev_protection_attach_rate,
        service_attach_rate,
        LAG(service_attach_rate) OVER (PARTITION BY employee_id ORDER BY year, month_num) AS prev_service_attach_rate,
        membership_attach,
        LAG(membership_attach) OVER (PARTITION BY employee_id ORDER BY year, month_num) AS prev_membership_attach,
        returns_rate,
        LAG(returns_rate) OVER (PARTITION BY employee_id ORDER BY year, month_num) AS prev_returns_rate
    FROM gold.vw_employee_monthly
)
SELECT
    year,
    month_num,
    employee_id,
    first_name,
    last_name,
    department,
    total_sales,
    prev_total_sales,
    total_sales - prev_total_sales AS chg_total_sales,
    sales_per_hour,
    prev_sales_per_hr,
    sales_per_hour - prev_sales_per_hr AS chg_sales_per_hour,
    protection_attach_rate,
    prev_protection_attach_rate,
    protection_attach_rate - prev_protection_attach_rate AS chg_protection_attach_rate,
    service_attach_rate,
    prev_service_attach_rate,
    service_attach_rate - prev_service_attach_rate AS chg_service_attach_rate,
    membership_attach,
    prev_membership_attach,
    membership_attach - prev_membership_attach AS chg_membership_attach,
    returns_rate,
    prev_returns_rate,
    returns_rate - prev_returns_rate AS chg_returns_rate
FROM lagged;
GO

-- ============================================
-- vw_employee_scorecard
-- One-row summary per employee
-- ============================================
CREATE OR ALTER VIEW gold.vw_employee_scorecard AS
WITH history AS (
    SELECT
        employee_id,
        MAX(first_name) AS first_name,
        MAX(last_name) AS last_name,
        MAX(department) AS department,
        MAX(position) AS position,
        MAX(employment_type) AS employment_type,
        COUNT(*) AS months_recorded,

        CAST(AVG(total_sales) AS DECIMAL(10,2)) AS avg_total_sales,
        MAX(total_sales) AS best_total_sales,
        MIN(total_sales) AS worst_total_sales,

        CAST(AVG(sales_per_hour) AS DECIMAL(10,2)) AS avg_sales_per_hour,
        MAX(sales_per_hour) AS best_sales_per_hour,
        MIN(sales_per_hour) AS worst_sales_per_hour,

        CAST(AVG(protection_attach_rate) AS DECIMAL(10,2)) AS avg_protection_attach_rate,
        MAX(protection_attach_rate) AS best_protection_attach_rate,
        MIN(protection_attach_rate) AS worst_protection_attach_rate,

        CAST(AVG(membership_attach) AS DECIMAL(10,2)) AS avg_membership_attach,
        MAX(membership_attach) AS best_membership_attach,
        MIN(membership_attach) AS worst_membership_attach,

        CAST(AVG(service_attach_rate) AS DECIMAL(10,2)) AS avg_service_attach_rate,
        MAX(service_attach_rate) AS best_service_attach_rate,
        MIN(service_attach_rate) AS worst_service_attach_rate,

        CAST(AVG(returns_rate) AS DECIMAL(10,2)) AS avg_returns_rate,
        MIN(returns_rate) AS best_returns_rate,
        MAX(returns_rate) AS worst_returns_rate
    FROM gold.vw_employee_monthly
    GROUP BY employee_id
),
latest AS (
    SELECT
        year,
        month_num,
        employee_id,
        first_name,
        last_name,
        employment_type,
        position,
        department,
        hours_worked,
        total_sales,
        sales_per_hour,
        protection_attach_rate,
        membership_attach,
        service_attach_rate,
        returns_rate,
        ROW_NUMBER() OVER (
            PARTITION BY employee_id
            ORDER BY year DESC, month_num DESC
        ) AS mth_desc
    FROM gold.vw_employee_monthly
),
latest_month AS (
    SELECT *
    FROM latest
    WHERE mth_desc = 1
),
trend_ranked AS (
    SELECT
        year,
        month_num,
        employee_id,
        prev_total_sales,
        chg_total_sales,
        prev_sales_per_hr,
        chg_sales_per_hour,
        prev_protection_attach_rate,
        chg_protection_attach_rate,
        prev_service_attach_rate,
        chg_service_attach_rate,
        prev_membership_attach,
        chg_membership_attach,
        prev_returns_rate,
        chg_returns_rate,
        ROW_NUMBER() OVER (
            PARTITION BY employee_id
            ORDER BY year DESC, month_num DESC
        ) AS trend_desc
    FROM gold.vw_employee_trend
),
latest_trend AS (
    SELECT *
    FROM trend_ranked
    WHERE trend_desc = 1
)
SELECT
    lm.employee_id,
    lm.first_name,
    lm.last_name,
    lm.department,
    lm.position,
    lm.employment_type,

    lm.year AS latest_year,
    lm.month_num AS latest_month_num,
    lm.hours_worked,
    lm.total_sales,
    lm.sales_per_hour,
    lm.protection_attach_rate,
    lm.membership_attach,
    lm.service_attach_rate,
    lm.returns_rate,

    lt.prev_total_sales,
    lt.chg_total_sales,
    lt.prev_sales_per_hr,
    lt.chg_sales_per_hour,
    lt.prev_protection_attach_rate,
    lt.chg_protection_attach_rate,
    lt.prev_service_attach_rate,
    lt.chg_service_attach_rate,
    lt.prev_membership_attach,
    lt.chg_membership_attach,
    lt.prev_returns_rate,
    lt.chg_returns_rate,

    h.months_recorded,
    h.avg_total_sales,
    h.best_total_sales,
    h.worst_total_sales,
    h.avg_sales_per_hour,
    h.best_sales_per_hour,
    h.worst_sales_per_hour,
    h.avg_protection_attach_rate,
    h.best_protection_attach_rate,
    h.worst_protection_attach_rate,
    h.avg_membership_attach,
    h.best_membership_attach,
    h.worst_membership_attach,
    h.avg_service_attach_rate,
    h.best_service_attach_rate,
    h.worst_service_attach_rate,
    h.avg_returns_rate,
    h.best_returns_rate,
    h.worst_returns_rate,

    CASE
        WHEN
            (CASE WHEN ISNULL(lt.chg_sales_per_hour, 0) > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN ISNULL(lt.chg_protection_attach_rate, 0) > 0 THEN 1 ELSE 0 END) +
            (CASE WHEN ISNULL(lt.chg_service_attach_rate, 0) > 0 THEN 1 ELSE 0 END) >= 2
        THEN 'Improving'
        WHEN
            (CASE WHEN ISNULL(lt.chg_sales_per_hour, 0) < 0 THEN 1 ELSE 0 END) +
            (CASE WHEN ISNULL(lt.chg_protection_attach_rate, 0) < 0 THEN 1 ELSE 0 END) +
            (CASE WHEN ISNULL(lt.chg_service_attach_rate, 0) < 0 THEN 1 ELSE 0 END) >= 2
        THEN 'Declining'
        ELSE 'Stable / Mixed'
    END AS overall_trend
FROM latest_month lm
LEFT JOIN history h
    ON lm.employee_id = h.employee_id
LEFT JOIN latest_trend lt
    ON lm.employee_id = lt.employee_id;
GO

-- ============================================
-- vw_department_summary
-- Which department is leading?
-- ============================================
CREATE OR ALTER VIEW gold.vw_department_summary AS
SELECT
    year,
    month_num,
    department,
    COUNT(DISTINCT employee_id) AS employee_count,
    SUM(total_sales) AS department_total_sales,
    CAST(AVG(sales_per_hour) AS DECIMAL(10,2)) AS avg_sales_per_hour,
    CAST(AVG(protection_attach_rate) AS DECIMAL(10,2)) AS avg_protection_attach_rate,
    CAST(AVG(membership_attach) AS DECIMAL(10,2)) AS avg_membership_attach,
    CAST(AVG(service_attach_rate) AS DECIMAL(10,2)) AS avg_service_attach_rate,
    CAST(AVG(returns_rate) AS DECIMAL(10,2)) AS avg_returns_rate
FROM gold.vw_employee_monthly
GROUP BY
    year,
    month_num,
    department;
GO

-- ============================================
-- vw_store_monthly
-- What is the store's overall health?
-- ============================================
CREATE OR ALTER VIEW gold.vw_store_monthly AS
SELECT
    year,
    month_num,
    COUNT(DISTINCT employee_id) AS employee_count,
    SUM(total_sales) AS store_total_sales,
    CAST(AVG(sales_per_hour) AS DECIMAL(10,2)) AS avg_sales_per_hour,
    CAST(AVG(protection_attach_rate) AS DECIMAL(10,2)) AS avg_protection_attach_rate,
    CAST(AVG(membership_attach) AS DECIMAL(10,2)) AS avg_membership_attach,
    CAST(AVG(service_attach_rate) AS DECIMAL(10,2)) AS avg_service_attach_rate,
    CAST(AVG(returns_rate) AS DECIMAL(10,2)) AS avg_returns_rate
FROM gold.vw_employee_monthly
GROUP BY
    year,
    month_num;
GO
