-- Using UNION without ALL

--use schema snowflake_sample_data.tpch_sf100;


SELECT o_orderkey, o_orderdate
FROM orders
WHERE o_orderdate BETWEEN '1994-01-01' AND '1995-01-01'

UNION

SELECT l_orderkey, l_commitdate AS orderdate
FROM lineitem
WHERE l_commitdate BETWEEN '1994-01-01' AND '1995-01-01';

--01bb8fea-3204-48f5-0002-2fc6000cd012



------------

SELECT o_orderkey, o_orderdate
FROM orders
WHERE o_orderdate BETWEEN '1994-01-01' AND '1995-01-01'

UNION ALL

SELECT l_orderkey, l_commitdate AS orderdate
FROM lineitem
WHERE l_commitdate BETWEEN '1994-01-01' AND '1995-01-01';

--01bb8ff6-3204-489e-0002-2fc6000cb4e6