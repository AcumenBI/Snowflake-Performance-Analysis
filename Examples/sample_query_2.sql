-- Query to demo 'Memory Spillage - to local storage'

--use schema snowflake_sample_data.tpch_sf10;

--alter session set use_cached_result  = false;


SELECT 
    o.O_ORDERKEY,        -- The order ID from the Orders table
    o.O_CUSTKEY,         -- The customer ID from the Orders table
    o.o_ORDERDATE,       -- The order date from the Orders table
    l.L_LINENUMBER,      -- The line item number from the Lineitem table
    l.L_QUANTITY,        -- The quantity for the line item
    l.L_EXTENDEDPRICE,    -- The extended price for the line item
    l.L_DISCOUNT         -- The discount applied to the line item
FROM 
    orders o  -- Orders table
JOIN 
    lineitem l -- Lineitem table
    ON o.O_ORDERKEY = l.L_ORDERKEY  -- Join condition: OrderKey from both tables
ORDER BY 
    o.O_ORDERKEY, l.L_LINENUMBER;


---------

/*Query ID : 01bb3c67-3204-3926-0000-00022fc6e3e9
Query duration : 21s
Warehouse:	X-SMALL*/