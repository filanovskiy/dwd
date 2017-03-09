-- use below for capturing stats
drop table bds.offload_stats;
drop sequence bds.offload_seq;

CREATE GLOBAL TEMPORARY TABLE "BDS"."OFFLOAD_STATS" 
   (	"ID" NUMBER, 
	"SID" NUMBER, 
	"NAME" VARCHAR2(64 BYTE), 
	"VALUE" NUMBER
   ) ON COMMIT PRESERVE ROWS ;

create sequence bds.offload_seq start with 1 increment by 1;

create or replace PROCEDURE bds.collect_offload_stats AS
  i  number;
BEGIN
  i := bds.offload_seq.nextval;
  INSERT INTO bds.offload_stats (id, sid, name, value)
  select i, ms.sid, sn.name, ms.value
  from v$mystat ms, v$statname sn
  where ms.statistic# = sn.statistic#
    and sn.name like '%XT%';
   
  commit;
END collect_offload_stats;
/

grant select on bds.offload_seq to public;
grant select on bds.offload_stats to public;
grant execute on bds.collect_offload_stats to public;
CREATE OR REPLACE  PUBLIC SYNONYM COLLECT_OFFLOAD_STATS FOR BDS.COLLECT_OFFLOAD_STATS;

