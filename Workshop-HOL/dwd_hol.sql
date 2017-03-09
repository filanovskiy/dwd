-- Initialize
DROP TABLE WEB_CLICKSTREAMS_JSON;
DROP TABLE WEB_CLICKSTREAMS_JSON_TRUNC;
DROP TABLE WEB_SALES;
DROP TABLE WEB_SALES_CSV;
DROP TABLE WEB_SALES_CATPART;
DROP TABLE EMP_HBASE;


-----------------------------------------------------
--
-- PART 1:  Creating tables
--
-----------------------------------------------------
-- Review data
--[need cli access]
hadoop fs -tail /user/hive/warehouse/csv.db/web_clickstreams_json/000000_0

-- HDFS Source

-- [db12c connection]
CREATE TABLE WEB_CLICKSTREAMS_JSON
   (	VAL VARCHAR2(4000 BYTE)
   ) 
   ORGANIZATION EXTERNAL 
    ( TYPE ORACLE_HDFS
      DEFAULT DIRECTORY DEFAULT_DIR
      LOCATION ('/user/hive/warehouse/csv.db/web_clickstreams_json')
    )
   REJECT LIMIT UNLIMITED ;


-- Query clickstream.  Notice it matches source
SELECT * FROM web_clickstreams_json WHERE rownum < 50;
   
-- Use database JSON features to query table.  Derive columns from attributes.
SELECT j.val."date",
  j.val.am_pm,
  j.val.shift,
  j.val.sub_shift,
  j.val.web_page,
  j.val.item_sk
FROM web_clickstreams_json j 
WHERE j.val.sub_shift = 'evening'
AND j.val.item_sk='4867';


-- Update ACCESS PARAMETERS.  Simple example shows how to handle overflow.
CREATE TABLE WEB_CLICKSTREAMS_JSON_TRUNC
   (	VAL VARCHAR2(40 BYTE)
   ) 
   ORGANIZATION EXTERNAL 
    ( TYPE ORACLE_HDFS
      DEFAULT DIRECTORY DEFAULT_DIR
      ACCESS PARAMETERS (
                  com.oracle.bigdata.cluster=cluster
                  com.oracle.bigdata.overflow={"action":"truncate"}                     
                 )
      LOCATION ('/user/hive/warehouse/csv.db/web_clickstreams_json')
    )
   REJECT LIMIT UNLIMITED ;

SELECT * FROM web_clickstreams_json_trunc WHERE rownum < 50;


-- Create table WEB_SALES over CSV data
-- Update access parameters - which will identify how the data should be parsed on the cell
CREATE TABLE WEB_SALES_CSV
(
  WS_SOLD_DATE_SK NUMBER 
, WS_SOLD_TIME_SK NUMBER 
, WS_SHIP_DATE_SK NUMBER 
, WS_ITEM_SK NUMBER 
, WS_BILL_CUSTOMER_SK NUMBER 
, WS_BILL_CDEMO_SK NUMBER 
, WS_BILL_HDEMO_SK NUMBER 
, WS_BILL_ADDR_SK NUMBER 
, WS_SHIP_CUSTOMER_SK NUMBER 
, WS_SHIP_CDEMO_SK NUMBER 
, WS_SHIP_HDEMO_SK NUMBER 
, WS_SHIP_ADDR_SK NUMBER 
, WS_WEB_PAGE_SK NUMBER 
, WS_WEB_SITE_SK NUMBER 
, WS_SHIP_MODE_SK NUMBER 
, WS_WAREHOUSE_SK NUMBER 
, WS_PROMO_SK NUMBER 
, WS_ORDER_NUMBER NUMBER 
, WS_QUANTITY NUMBER 
, WS_WHOLESALE_COST NUMBER
, WS_LIST_PRICE NUMBER
, WS_SALES_PRICE NUMBER
, WS_EXT_DISCOUNT_AMT NUMBER 
, WS_EXT_SALES_PRICE NUMBER
, WS_EXT_WHOLESALE_COST NUMBER
, WS_EXT_LIST_PRICE NUMBER
, WS_EXT_TAX NUMBER
, WS_COUPON_AMT NUMBER 
, WS_EXT_SHIP_COST NUMBER
, WS_NET_PAID NUMBER 
, WS_NET_PAID_INC_TAX NUMBER
, WS_NET_PAID_INC_SHIP NUMBER 
, WS_NET_PAID_INC_SHIP_TAX NUMBER 
, WS_NET_PROFIT NUMBER 
) 
ORGANIZATION EXTERNAL 
( 
  TYPE ORACLE_HDFS
  DEFAULT DIRECTORY DEFAULT_DIR 
  ACCESS PARAMETERS 
  ( 
    com.oracle.bigdata.cluster=cluster
    com.oracle.bigdata.fileformat=TEXTFILE
    com.oracle.bigdata.rowformat: DELIMITED FIELDS TERMINATED BY '|' 

  ) 
  LOCATION ('/user/hive/warehouse/csv.db/web_sales')
) 
REJECT LIMIT UNLIMITED;

-- Query the data 
SELECT ws_ship_date_sk, 
       ws_item_sk,
       ws_list_price,
       ws_sales_price,
       ws_ext_discount_amt
FROM web_sales_csv 
WHERE ws_sold_date_sk=36939
  AND ws_web_site_sk = 7;


-- Make the query a bit more meaningful
SELECT s.ws_ship_date_sk,
       d.d_date as ship_date,
       s.ws_item_sk,
       i.i_class,
       i.i_category,
       i.i_color,
       s.ws_list_price,
       s.ws_sales_price,
       s.ws_ext_discount_amt
FROM web_sales_csv s, bds.item_orcl i, bds.date_dim_orcl d 
WHERE ws_ship_date_sk = d.d_date_sk
  AND ws_item_sk = i.i_item_sk
  AND s.ws_item_sk in (114783, 34603)
  AND s.ws_web_site_sk = 7;
;


-- Create table WEB_SALES over CSV.WEB_SALES hive table
-- Notice with Hive driver - you do not need to specify how to locate or parse the data
-- And, you can use DBMS_HADOOP package or SQL Developer UI to create the table
-- Instead of using DDL below - create the table by right-clicking on Hive table in the Hive
-- SQL Dev connection:
-- csv.web_sales

-- In HIVE worksheet, describe the CSV.WEB_SALES table

-- [hive connection]
show create table csv.web_sales;  

-- In Oracle Database ...

-- [db12c connection]
CREATE TABLE WEB_SALES
(
  WS_SOLD_DATE_SK NUMBER 
, WS_SOLD_TIME_SK NUMBER 
, WS_SHIP_DATE_SK NUMBER 
, WS_ITEM_SK NUMBER 
, WS_BILL_CUSTOMER_SK NUMBER 
, WS_BILL_CDEMO_SK NUMBER 
, WS_BILL_HDEMO_SK NUMBER 
, WS_BILL_ADDR_SK NUMBER 
, WS_SHIP_CUSTOMER_SK NUMBER 
, WS_SHIP_CDEMO_SK NUMBER 
, WS_SHIP_HDEMO_SK NUMBER 
, WS_SHIP_ADDR_SK NUMBER 
, WS_WEB_PAGE_SK NUMBER 
, WS_WEB_SITE_SK NUMBER 
, WS_SHIP_MODE_SK NUMBER 
, WS_WAREHOUSE_SK NUMBER 
, WS_PROMO_SK NUMBER 
, WS_ORDER_NUMBER NUMBER 
, WS_QUANTITY NUMBER 
, WS_WHOLESALE_COST NUMBER 
, WS_LIST_PRICE NUMBER 
, WS_SALES_PRICE NUMBER 
, WS_EXT_DISCOUNT_AMT NUMBER 
, WS_EXT_SALES_PRICE NUMBER 
, WS_EXT_WHOLESALE_COST NUMBER 
, WS_EXT_LIST_PRICE NUMBER 
, WS_EXT_TAX NUMBER 
, WS_COUPON_AMT NUMBER 
, WS_EXT_SHIP_COST NUMBER 
, WS_NET_PAID NUMBER 
, WS_NET_PAID_INC_TAX NUMBER 
, WS_NET_PAID_INC_SHIP NUMBER 
, WS_NET_PAID_INC_SHIP_TAX NUMBER 
, WS_NET_PROFIT NUMBER 
) 
ORGANIZATION EXTERNAL 
( 
  TYPE ORACLE_HIVE 
  DEFAULT DIRECTORY DEFAULT_DIR 
  ACCESS PARAMETERS 
  ( 
    com.oracle.bigdata.tablename: csv.web_sales 
  ) 
) 
REJECT LIMIT UNLIMITED;

-- Review the results
SELECT ws_ship_date_sk, 
       ws_item_sk,
       ws_list_price,
       ws_sales_price,
       ws_ext_discount_amt
FROM web_sales 
WHERE ws_sold_date_sk=36939
  AND ws_web_site_sk =7
;

-- What if you want to create external tables for all of the hive tables in the "parq" database?
-- First, let's look at the hive tables in database "parq":

SELECT * FROM all_hive_tables WHERE database_name='parq';


-- Generate the DDL (but don't run it) for all tables in that databse using the PL/SQL snippet below
-- You will need to connect the Dbms Output "view" to the connection

DECLARE 
   DDLout VARCHAR2(4000);
   o_table_name VARCHAR2(30);
BEGIN
   dbms_output.enable;
   
   -- Find all tables in Hive database "parq"
   FOR htable_rec IN (
        SELECT *
          FROM all_hive_tables
         WHERE database_name = 'parq')
   LOOP  -- Loop over each table and generate the DDL
      DDLout := null;
      o_table_name := htable_rec.table_name;
      
      -- Generate DDL for this table
      dbms_hadoop.create_extddl_for_hive(
        CLUSTER_ID=>htable_rec.cluster_id,
        DB_NAME=>htable_rec.database_name,
        HIVE_TABLE_NAME=>htable_rec.table_name,
        HIVE_PARTITION=>FALSE,
        TABLE_NAME=>o_table_name,
        PERFORM_DDL=>FALSE,
        TEXT_OF_DDL=>DDLout
      );

    dbms_output.put_line('-- Table: ' || htable_rec.table_name);
    dbms_output.put_line('drop table ' || o_table_name || ';');
    
    -- Make some modifications to the generated SQL.  You can put whatever
    -- customizations you want here.
    DDLout := replace(DDLout, 'COMPLEX', 'NUMBER');
    DDLout := replace(DDLout, 'PARALLEL 2 ', '');
    dbms_output.put_line(DDLout || ';');
    
   END LOOP;
   dbms_output.put_line('--Done.');
END;
/


-- Changes in Hive are automatically picked up by BDS at query execution time.  Let's see
-- that in action:
-- Update the hive source.  It now points to a small data set.  
-- Notice the BDS table didn't change to query this new source

-- Execute below in hive.  NOTE:  There is only one hive table that is shared by everyone - so 
-- only one person should do this!

--[hive connection] 
ALTER TABLE csv.web_sales SET LOCATION "hdfs://bds1.localdomain:8020/data/tpcds/web_sales_5rec";
select * from csv.web_sales;


-- execute below in Oracle Database

--[db12c connection]
SELECT *
FROM web_sales;

-- Reset the hive source and query again.
-- execute below in Hive

--[hive connection]
ALTER TABLE csv.web_sales SET LOCATION "hdfs://bds1.localdomain:8020/user/hive/warehouse/csv.db/web_sales";

SELECT * 
FROM csv.web_sales
LIMIT 100;

-- execute below in Oracle Database

--[db12c connection]
SELECT *
FROM web_sales
WHERE rownum < 100;


-----------------------------------------------------
--
-- PART 2:  Performance features
--
-----------------------------------------------------
--Go to View Reports... User Defined Reports.. Offload Report

-- SmartScan - what cells are available
SELECT * FROM v$cell;

-- Performance Statistics
-- What has offloaded during this session?
-- Collect Statistics on WEB_SALES

-- What functions are pushed down?
SELECT * FROM v$sqlfn_metadata
WHERE offloadable = 'YES';

-- Try out the JSON functions.  Run the explain plan from our first query
-- Notice the EXTERNAL TABLE ACCESS(STORAGE FULL) + Filter Predicates
-- Data is filtered in the storage cells
SELECT j.val."date",
  j.val.am_pm,
  j.val.shift,
  j.val.sub_shift,
  j.val.web_page,
  j.val.item_sk
FROM web_clickstreams_json j 
WHERE j.val.sub_shift = 'evening'
AND j.val.item_sk='4867';

-- Run the query and review the offload statistics.  
-- The offload statistics are cumulative for the session.  Reconnect to see the
-- results for this specific query
SELECT ms.sid, sn.name,ms.value
  FROM V$MYSTAT ms, V$STATNAME sn
 WHERE ms.STATISTIC#=sn.STATISTIC#
   AND sn.name LIKE '%XT%';

-- We have a custom procedure that saves the offload stats for each query.
-- Use this procedure in combination with the SQL Developer Offload Report
-- 1) Run the procedure below, 2) re-execute the JSON query, 3) run the procedure below
-- Then, look at the Offload Report
execute collect_offload_stats


--
-- Storage Index
--
-- Storage index is automatically built based on attributes in where clause
-- Index recreated for blocks that do not contain the data
-- You have been creating and using storage indexes without even realizing it!

-- Disable SI
alter session set "_kcfis_test_control1"=32;
alter session set "_kcfis_storageidx_disabled"=TRUE;

-- Run query - should be a bit slower
select count(*) from (
SELECT ws_ship_date_sk, 
       ws_item_sk,
       ws_list_price,
       ws_sales_price,
       ws_ext_discount_amt
FROM web_sales_csv 
WHERE ws_web_site_sk = 7
  AND ws_item_sk > 131040 and ws_item_sk < 131112 )
;
  
execute collect_offload_stats

-- Enable SI again :)
alter session set "_kcfis_test_control1"=0;
alter session set "_kcfis_storageidx_disabled"=FALSE;

-- Run query to influence SI creation.  This should return no rows - and a null result
-- Because SI is related to a block - and data is triple replicated - running the query 
-- multiple times will continue to improve SI coverage
SELECT count(*)
FROM WEB_SALES_CSV 
WHERE 
  WS_SOLD_DATE_SK = -1
  AND WS_SOLD_TIME_SK = -1
  AND WS_SHIP_DATE_SK = -1
  AND WS_ITEM_SK = -1
  AND WS_SHIP_CUSTOMER_SK = -1
  AND WS_WEB_PAGE_SK = -1
  AND WS_WEB_SITE_SK = -1
  AND WS_WAREHOUSE_SK = -1
  AND WS_QUANTITY = -1
  AND WS_WHOLESALE_COST = -1
  AND WS_LIST_PRICE = -1
  AND WS_SALES_PRICE = -1
  AND WS_COUPON_AMT = -1;

-- Examine performance statistics
execute collect_offload_stats


-- Run the following query with SI enabled and review the statistics
select count(*) from (
SELECT ws_ship_date_sk, 
       ws_item_sk,
       ws_list_price,
       ws_sales_price,
       ws_ext_discount_amt
FROM web_sales_csv 
WHERE ws_web_site_sk = 7
  AND ws_item_sk > 131040 and ws_item_sk < 131112 );
    
execute collect_offload_stats


-- 
-- Bloom filters

-- You should gather statistics on your tables in order for the database to use the 
-- best plan.  Instead of gathering statistics - we'll give some hints.

-- First, let's run this query WITHOUT join filters.  Review the explain plan
-- and notice that there are no item filters in storage
select count(*) from (
SELECT /*+ NO_PX_JOIN_FILTER(s) USE_HASH(i s) */ 
       s.ws_ship_date_sk,
       d.d_date as ship_date,
       s.ws_item_sk,
       i.i_current_price,
       i.i_class,
       i.i_category,
       i.i_color,
       s.ws_list_price,
       s.ws_sales_price,
       s.ws_ext_discount_amt
FROM web_sales_csv s, bds.item_orcl i, bds.date_dim_orcl d 
WHERE s.ws_ship_date_sk = d.d_date_sk
  AND s.ws_item_sk = i.i_item_sk
  AND i.i_category = 'Music'
  AND i.i_class in ('Jazz')
  AND s.ws_sales_price > 290
)
;

-- Examine performance statistics
execute collect_offload_stats

-- Run again - this time Bloom filters will engage 
-- Look at the statistics and notice the reduction in bytes returned
select count(*) from (
SELECT /*+ PX_JOIN_FILTER(s) USE_HASH(i s) */ 
       s.ws_ship_date_sk,
       d.d_date as ship_date,
       s.ws_item_sk,
       i.i_current_price,
       i.i_class,
       i.i_category,
       i.i_color,
       s.ws_list_price,
       s.ws_sales_price,
       s.ws_ext_discount_amt
FROM web_sales_csv s, bds.item_orcl i, bds.date_dim_orcl d 
WHERE s.ws_ship_date_sk = d.d_date_sk
  AND s.ws_item_sk = i.i_item_sk
  AND i.i_category = 'Music'
  AND i.i_class in ('Jazz')
  AND s.ws_sales_price > 290
)
;


-- Examine performance statistics
execute collect_offload_stats

--
--  Partition pruning
--
-- WEB_SALES_CATPART is the same table - but partitioned by Item Category
-- This table is also in Parquet format (but doesn't have to be -
-- storage format doesn't matter)

-- Create the table over the PARQ.WEB_SALES_CATAPART hive table.
-- Notice - you don't care that it is partitioned.  This will be determined
-- at query execution time
-- Use SQL Developer UI or the DDL below:
CREATE TABLE WEB_SALES_CATPART 
(
  WS_SOLD_DATE_SK NUMBER 
, WS_SOLD_TIME_SK NUMBER 
, WS_SHIP_DATE_SK NUMBER 
, WS_ITEM_SK NUMBER 
, WS_BILL_CUSTOMER_SK NUMBER 
, WS_BILL_CDEMO_SK NUMBER 
, WS_BILL_HDEMO_SK NUMBER 
, WS_BILL_ADDR_SK NUMBER 
, WS_SHIP_CUSTOMER_SK NUMBER 
, WS_SHIP_CDEMO_SK NUMBER 
, WS_SHIP_HDEMO_SK NUMBER 
, WS_SHIP_ADDR_SK NUMBER 
, WS_WEB_PAGE_SK NUMBER 
, WS_WEB_SITE_SK NUMBER 
, WS_SHIP_MODE_SK NUMBER 
, WS_WAREHOUSE_SK NUMBER 
, WS_PROMO_SK NUMBER 
, WS_ORDER_NUMBER NUMBER 
, WS_QUANTITY NUMBER 
, WS_WHOLESALE_COST NUMBER 
, WS_LIST_PRICE NUMBER 
, WS_SALES_PRICE NUMBER 
, WS_EXT_DISCOUNT_AMT NUMBER 
, WS_EXT_SALES_PRICE NUMBER 
, WS_EXT_WHOLESALE_COST NUMBER 
, WS_EXT_LIST_PRICE NUMBER 
, WS_EXT_TAX NUMBER 
, WS_COUPON_AMT NUMBER 
, WS_EXT_SHIP_COST NUMBER 
, WS_NET_PAID NUMBER 
, WS_NET_PAID_INC_TAX NUMBER 
, WS_NET_PAID_INC_SHIP NUMBER 
, WS_NET_PAID_INC_SHIP_TAX NUMBER 
, WS_NET_PROFIT NUMBER 
, WS_CATEGORY VARCHAR2(4000) 
) 
ORGANIZATION EXTERNAL 
( 
  TYPE ORACLE_HIVE 
  DEFAULT DIRECTORY DEFAULT_DIR 
  ACCESS PARAMETERS 
  ( 
    com.oracle.bigdata.tablename: parq.web_sales_catpart 
  ) 
) 
REJECT LIMIT UNLIMITED;


-- Run query that spans all partitions.  Review BYTES REQUESTED
SELECT count(*) 
FROM WEB_SALES_CATPART 
WHERE WS_SOLD_DATE_SK = -1
  AND ws_sales_price < 0;
  
-- Examine offload statistics
execute collect_offload_stats

-- Run a query for partition "Electronics"
select count(*) from (
SELECT /*+ PX_JOIN_FILTER(s) USE_HASH(i s) USE_HASH(d s) */ 
       s.ws_sold_date_sk,
       d.d_date as sold_date,
       s.ws_item_sk,
       i.i_class,
       i.i_category,
       i.i_color,
       s.ws_list_price,
       s.ws_sales_price,
       s.ws_ext_discount_amt
FROM web_sales_catpart s, bds.item_orcl i, bds.date_dim_orcl d 
WHERE s.ws_sold_date_sk = d.d_date_sk
  AND s.ws_item_sk = i.i_item_sk
  AND s.ws_category = 'Electronics'
  AND d.d_day_name = 'Monday'
  AND d.d_following_holiday='Y'
  AND d.d_year = 2005
  AND s.ws_sales_price > 290
  );

-- Examine offload statistics
execute collect_offload_stats

-- Using Analytic Functions
-- Because you are querying Oracle tables - all of Oracle Database query capabilities
-- will work.  This greatly simplifies running existing query workloads against
-- internal tables to run against external stores.

SELECT * FROM (
SELECT d.d_date,
       i.i_category,
       i.i_class,
       i.i_item_sk,
       sum(s.ws_quantity * s.ws_sales_price) as sales,
       rank() over(PARTITION BY d.d_date
              ORDER BY SUM(s.ws_quantity * s.ws_sales_price) DESC) AS
              rank_within_day
FROM web_sales_csv s, bds.item_orcl i, bds.date_dim_orcl d 
WHERE s.ws_sold_date_sk = d.d_date_sk
  AND s.ws_item_sk = i.i_item_sk
  AND d.d_day_name = 'Monday'
  AND d.d_following_holiday='Y'
  AND d.d_year = 2005
  AND s.ws_web_site_sk = 7
GROUP BY i.i_category, i.i_class, i.i_item_sk, d.d_date
)
WHERE rank_within_day <=5
ORDER BY d_date ASC, rank_within_day ASC;

execute collect_offload_stats

-- Oracle query execution will take advantage of MVs when querying data in HDFS.
-- Below is an MV that was created over STORE_SALES hive table.  This MV is stored
-- Oracle Database
/*
    CREATE MATERIALIZED VIEW bds.mv_store_sales_item 
    ON PREBUILT TABLE 
    ENABLE QUERY REWRITE AS (
      select ss_item_sk, 
             sum(ss_quantity) as ss_quantity,
             sum(ss_ext_wholesale_cost) as ss_ext_wholesale_cost,
             sum(ss_net_paid) as ss_net_paid,
             sum(ss_net_profit) as ss_net_profit
      from bds.store_sales
      group by ss_item_sk
    );
*/

-- Run a query against this fact table and notice the performance
-- Review the explain plan and notice that aggregation to Category started from
-- the MV stored in an Oracle Database summary table
alter session set query_rewrite_integrity=stale_tolerated;
SELECT i_category,
       SUM(ss_quantity)
FROM bds.store_sales, bds.item_orcl
WHERE ss_item_sk = i_item_sk
  AND i_size in ('small', 'petite')
  AND i_wholesale_cost > 80
GROUP BY i_category;


-- If there is already a summary table in Hadoop - it can also be leveraged with query rewrite.
-- There is a second MV where the source data for the MV is HDFS:

/*
    CREATE MATERIALIZED VIEW bds.mv_store_sales_qtr_class 
    ON PREBUILT TABLE
    WITH REDUCED PRECISION
    ENABLE QUERY REWRITE AS (
        select
               i.I_CLASS,
               sum(s.ss_quantity) as ss_quantity,
               sum(s.ss_wholesale_cost) as ss_wholesale_cost,
               suM(s.ss_ext_discount_amt) as ss_ext_discount_amt,
               sum(s.ss_ext_tax) as ss_ext_tax,
               sum(s.ss_coupon_amt) as ss_coupon_amt,
               d.D_QUARTER_NAME
        from DATE_DIM_ORCL d, ITEM_ORCL i, STORE_SALES s
        where s.ss_item_sk = i.i_item_sk
          and s.ss_sold_date_sk = date_dim_orcl.d_date_sk
        group by d.D_QUARTER_NAME,
               i.I_CLASS
        );
*/

-- What is the quarterly performance by category with yearly totals?
select
       i.i_category,
       d.d_year,
       d.d_quarter_name,
       sum(s.ss_quantity) quantity
from bds.DATE_DIM_ORCL d, bds.ITEM_ORCL i, bds.STORE_SALES s
where s.ss_item_sk = i.i_item_sk
  and s.ss_sold_date_sk = d.d_date_sk
  and d.d_quarter_name in ('2005Q1', '2005Q2', '2005Q3', '2005Q4')
group by rollup (i.i_category, d.d_year, d.D_QUARTER_NAME)
;



-----------------------------------------------------
-- PART 3:  Security
--
-- This section shows how to apply redaction and VPD policies to data
-- in Hadoop
-----------------------------------------------------

-- All queries against the web sales table should only return products
-- that the currently connected user is responsible for.
-- Use VPD policies to limit user access

-- View user mapping
SELECT * FROM bds.user_category;

-- Create the VPD packages that filter the data

CREATE OR REPLACE PACKAGE bds_vpd_cat_example AS
   p_cat bds.user_category.cat%TYPE;
   
   FUNCTION predicate(obj_schema VARCHAR2,
                      obj_name   VARCHAR2) RETURN VARCHAR2;
END bds_vpd_cat_example;
/

CREATE OR REPLACE PACKAGE BODY bds_vpd_cat_example as
 
   FUNCTION predicate (obj_schema varchar2, obj_name VARCHAR2) return varchar2 is
          
    BEGIN    
      -- The current user can see only certain categories
      SELECT cat
      INTO p_cat
      FROM bds.user_category
      WHERE userid = sys_context('USERENV', 'CURRENT_USER');
      
      -- Return the "where clause"       
      RETURN 'ws_category = ''' || p_cat || '''';
      
      EXCEPTION
      when others
         then return 'ws_category = ''Music''';
    END predicate;

END bds_vpd_cat_example;
/


-- Add the VPD Policy
BEGIN
  dbms_rls.add_policy(object_schema => USER,
    object_name     => 'WEB_SALES_CATPART',
    policy_name     => 'FILTER_CAT',
    function_schema => USER,
    policy_function => 'bds_vpd_cat_example.predicate',
    statement_types => 'select');
END;
/


-- Run an explain plan on the query.  Notice that the data will now be filtered by
-- category.
-- Run the query - notice that CUSTOMER_ORCL has redaction policies - so some user data
-- is redacted.  
SELECT s.ws_bill_customer_sk,
       c.c_customer_sk,
       s.ws_category,
       c.c_last_name,
       c.c_first_name,
       c.c_birth_year,
       s.ws_sales_price
FROM bds.customer_orcl c, web_sales_catpart s
WHERE s.ws_bill_customer_sk = c.c_customer_sk
;

-- Drop the policy if you want to see everything
BEGIN
  dbms_rls.drop_policy(object_schema => user,
    object_name     => 'WEB_SALES_CATPART',
    policy_name     => 'FILTER_CAT'
);
END;
/

-- The customer identifier in the fact could be PII.
-- Add a redaction policy to the customer identifier in the sales table
    
BEGIN
  DBMS_REDACT.ADD_POLICY(
    object_schema => USER,
    object_name => 'WEB_SALES_CATPART',
    column_name => 'WS_BILL_CUSTOMER_SK',
    policy_name => 'web_sales_redact',
    function_type => DBMS_REDACT.PARTIAL,
    function_parameters => '9,1,7',
    expression => '1=1'
  );
  
END;
/

-- Run that same query again.  Notice that joins on redacted columns work fine :)
SELECT s.ws_bill_customer_sk,
       c.c_customer_sk,
       s.ws_category,
       s.ws_sales_price,
       c.c_first_name,
       c.c_last_name,
       c.c_birth_year
FROM bds.customer_orcl c, web_sales_catpart s
WHERE s.ws_bill_customer_sk = c.c_customer_sk
;


--
-- PART 4 :  Accessing NoSQL Stores
--

-- HBase. Create table and put data there 
hbase shell
 
disable 'emp'
drop 'emp'
create 'emp', 'personal data', 'professional data'
put 'emp','6179995','personal data:name','raj'
put 'emp','6179995','personal data:city','palo alto'
put 'emp','6179995','professional data:position','developer'
put 'emp','6179995','professional data:salary','50000'
put 'emp','6274234','personal data:name','alex'
put 'emp','6274234','personal data:city','belmont'
put 'emp','6274234','professional data:position','pm'
put 'emp','6274234','professional data:salary','60000'
scan 'emp'

-- In Hive 

-- [hive connection]
drop table emp_hbase;
CREATE EXTERNAL TABLE IF NOT EXISTS emp_hbase (rowkey STRING, ename STRING, city STRING,  position STRING,salary STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key#binary,personal data:name, personal data:city, professional data:position, professional data:salary')
TBLPROPERTIES ('hbase.table.name' = 'emp');

-- In the Oracle DB
--[db12c connection]
CREATE TABLE emp_hbase
   (
  rowkey number, ename VARCHAR2(4000), city VARCHAR2(4000),  position VARCHAR2(4000),salary number
   ) 
   ORGANIZATION EXTERNAL 
    ( TYPE ORACLE_HIVE
      DEFAULT DIRECTORY "DEFAULT_DIR"
      ACCESS PARAMETERS
      ( com.oracle.bigdata.tablename=emp_hbase)
    )
   REJECT LIMIT UNLIMITED 
  PARALLEL ;
  
-- query data
select * from emp_hbase






