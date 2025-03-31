-- Query to demo 'Exploding joins'

--use schema snowflake_sample_data.tpch_sf100;

--alter session set use_cached_result  = false;

--without join condition
select * 
from orders as o
    join customer as c
where c_custkey in (121361, 84973)
and  o_orderdate >= CAST('1998-06-02' AS date);

------------


/*Query ID : 01bb4c8c-3204-3c79-0002-2fc60004502e
Query duration : 48s
Warehouse:	X-SMALL*/

--------------


--with join condition
/* 
select * 
from orders as o
    join customer as c
    on o.o_custkey=c.c_custkey
where c_custkey in (121361,84973)
and  o_orderdate >= CAST('1995-06-01' AS date);
*/
