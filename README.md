# NovaTech Retail Data Warehouse

## What this project is

This project takes messy retail data and turns it into something useful.

Instead of guessing what’s happening in a store, this system shows:

- Who is selling well
- Who needs coaching
- Which departments are strong or weak
- How the whole store is performing over time

---

## The problem

Retail data is messy and hard to use.

- Data comes from different files
- Columns are inconsistent
- Numbers don’t mean anything without context

Managers can’t easily answer basic questions like:

- “Who is my best employee?”
- “Why is attach rate low?”
- “Which department is underperforming?”

---

## ✅ The solution

I built a 3-layer data warehouse that turns raw data into business insights.

---

## Architecture

`Bronze → Silver → Gold`

---

## 🥉 Bronze — Raw Data

This is the untouched data from CSV files.

- employees
- products
- transactions

👉 Just load it, don’t touch it

---

## 🥈 Silver — Clean Data

This layer fixes everything:

- cleaned names
- fixed data types
- removed duplicates
- standardized values

👉 Make it usable

---

## 🥇 Gold — Business Views

This is where the real value is.

Instead of raw tables, we create views that answer real business questions.

---

## What was built (Gold layer)

### Employee Performance

**`vw_employee_monthly`**

- sales
- sales per hour
- attach rates (protection, membership, services)
- return rate

👉 How did each employee perform this month?

---

### Product Insights

**`vw_product_performance`**

- what sells
- what gets returned
- what attaches protection well

👉 Which products are performing best?

---

### Coaching Targets

**`vw_coaching_targets`**

- finds strong sellers with weak attach rates
- flags who needs coaching

👉 Who should I focus on right now?

---

### Employee Trends

**`vw_employee_trend`**

- compares month-to-month performance
- shows improvement or decline

👉 Is this employee getting better or worse?

---

### Employee Scorecard

**`vw_employee_scorecard`**

- one row per employee
- latest performance
- historical averages
- trend direction

👉 What’s the full story of this employee?

---

### Department Summary

**`vw_department_summary`**

- compares departments monthly
- shows revenue and attach performance

👉 Which department is leading?

---

### Store Summary

**`vw_store_monthly`**

- one row per month for the entire store
- total sales and overall performance

👉 Is the store improving or declining?

---

## How it all flows

`Raw Data → Clean Data → Business Views → Decisions`

Or simply:

`Bronze → Silver → Gold`

---

## Key skills used

- SQL Server
- Data cleaning and transformation
- Window functions (`LAG`, `ROW_NUMBER`, `PERCENT_RANK`)
- Aggregations and KPI calculations
- Medallion architecture (Bronze / Silver / Gold)

---

## Why this project matters

This isn’t just SQL tables.

It simulates a real retail environment where decisions are based on data:

- identify top performers
- find coaching opportunities
- compare departments
- track store health

