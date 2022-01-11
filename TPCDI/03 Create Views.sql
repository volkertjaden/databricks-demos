-- Databricks notebook source
-- DBTITLE 1,Table Counts
select 'finwire_cmp_bronze' as table ,count(*) as row_count from lakehouse_tpcdi_bronze.finwire_cmp_bronze union
select 'finwire_fin_bronze',count(*) from lakehouse_tpcdi_bronze.finwire_fin_bronze union
select 'finwire_sec_bronze', count(*) from lakehouse_tpcdi_bronze.finwire_sec_bronze union
select 'prospect_bronze', count(*) from lakehouse_tpcdi_bronze.prospect_bronze union
select 'daily_market', count(*) from lakehouse_tpcdi_bronze.daily_market union
select 'customer_management', count(*) from lakehouse_tpcdi_bronze.customer_management 

-- COMMAND ----------

CREATE OR REPLACE VIEW lakehouse_tpcdi_bronze.table_counts AS (
  select 'finwire_cmp_bronze' as table ,count(*) as row_count from lakehouse_tpcdi_bronze.finwire_cmp_bronze union
  select 'finwire_fin_bronze',count(*) from lakehouse_tpcdi_bronze.finwire_fin_bronze union
  select 'finwire_sec_bronze', count(*) from lakehouse_tpcdi_bronze.finwire_sec_bronze union
  select 'prospect_bronze', count(*) from lakehouse_tpcdi_bronze.prospect_bronze union
  select 'daily_market', count(*) from lakehouse_tpcdi_bronze.daily_market union
  select 'customer_management', count(*) from lakehouse_tpcdi_bronze.customer_management 
)

-- COMMAND ----------

select
  *
from
  lakehouse_tpcdi_bronze.customer_management c
  inner join lakehouse_tpcdi_bronze.prospect_bronze p on lower(c.C_F_NAME) = lower(p.FirstName)
  and lower(c.C_L_NAME) = lower(p.LastName)
  and lower(c.C_ZIPCODE) = lower(p.PostalCode)
  and lower(c.C_ADLINE1) = lower(p.AddressLine1)
  and lower(c.C_ADLINE2) = lower(p.AddressLine2)
  and lower(c.C_CITY) = lower(p.City)

-- COMMAND ----------

select
    Country, State, City, count(*) as no_of_customers
  from
    lakehouse_tpcdi_bronze.customer_management c
    inner join lakehouse_tpcdi_bronze.prospect_bronze p on lower(c.C_F_NAME) = lower(p.FirstName)
    and lower(c.C_L_NAME) = lower(p.LastName)
    and lower(c.C_ZIPCODE) = lower(p.PostalCode)
    and lower(c.C_ADLINE1) = lower(p.AddressLine1)
    and lower(c.C_ADLINE2) = lower(p.AddressLine2)
    and lower(c.C_CITY) = lower(p.City)
  --where Country = 'United States of America' 
  group by 1,2,3
  

-- COMMAND ----------

CREATE OR REPLACE VIEW lakehouse_tpcdi_bronze.matched_prospects_stats AS (
  select
    Country, State ,City, count(*) as no_of_customers
  from
    lakehouse_tpcdi_bronze.customer_management c
    inner join lakehouse_tpcdi_bronze.prospect_bronze p on lower(c.C_F_NAME) = lower(p.FirstName)
    and lower(c.C_L_NAME) = lower(p.LastName)
    and lower(c.C_ZIPCODE) = lower(p.PostalCode)
    and lower(c.C_ADLINE1) = lower(p.AddressLine1)
    and lower(c.C_ADLINE2) = lower(p.AddressLine2)
    and lower(c.C_CITY) = lower(p.City)
  group by 1,2,3
)

-- COMMAND ----------

select 
avg(dm.dm_close) avg_close, avg(dm.dm_vol) as avg_vol , sec.ExID as exchange_id
from lakehouse_tpcdi_bronze.daily_market dm 
    inner join lakehouse_tpcdi_bronze.finwire_sec_bronze sec on sec.Symbol = dm.dm_s_symb
group by sec.ExID 

-- COMMAND ----------

CREATE OR REPLACE VIEW lakehouse_tpcdi_bronze.analyze_daily_market AS (
  select 
  avg(dm.dm_close) avg_close, avg(dm.dm_vol) as avg_vol , sec.ExID as exchange_id
  from lakehouse_tpcdi_bronze.daily_market dm 
      inner join lakehouse_tpcdi_bronze.finwire_sec_bronze sec on sec.Symbol = dm.dm_s_symb
  group by sec.ExID 
)
