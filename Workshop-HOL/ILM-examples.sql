--
-- Create tablespace and move partitions
--
select table_name, partition_name, tablespace_name from user_tab_partitions where table_name = 'STORE_SALES_ORCL';

create tablespace cold_hdfs_2004_q1 datafile 'cold_hdfs_2004_q1.dbf' size 1G reuse autoextend on nologging;
alter table store_sales_orcl move partition P_2004_02 tablespace cold_hdfs_2004_q1 online;
alter table store_sales_orcl move partition P_2004_03 tablespace cold_hdfs_2004_q1 online;
alter table store_sales_orcl move partition P_2004_04 tablespace cold_hdfs_2004_q1 online;

-- Which tablespaces are available?  Log into bds12c as oracle and run this:
./bds-copy-tbs-to-hdfs.sh --list=%cold%

-- Run script to 1) set it read-only, 2) take the tablespace offline, 3) move tablespace
-- 4) rename the tablespace data files, 5) bring the tablespace online
./bds-copy-tbs-to-hdfs.sh --tablespace=cold_hdfs_2004_q1

-- Look at the data files.  Notice that some tablespaces are now stored in HDFS.
select * from dba_data_files;
select * from dba_tablespaces;

-- Update some session vars.  Because there's not a ton of data, we want to force 
-- smartscan to engage
alter session set "_small_table_threshold" = 20;
alter session set "_very_large_object_threshold" = 100;
alter system flush shared_pool;

-- This is querying data across HDFS and Local
-- Smart Scan may not engage b/c of small data set
select count(*) 
from store_sales_orcl
where ss_sold_date_sk in (38017, 3801);

execute collect_offload_stats;


-- Query larger table where the entire table space is on HDFS
-- Because it's bigger, Smart Scan will engage
select /*+ PARALLEL(16) */ i_class, d_year, d_moy, count(*)
from store_sales_orcl_hdfs s, date_dim_orcl d, item_orcl i
where  s.ss_sold_date_sk = d.d_date_sk
and i.i_item_sk = s.ss_item_sk
and d_day_name in ('Saturday','Sunday')
group by i_class, d_year, d_moy;

execute collect_offload_stats;
