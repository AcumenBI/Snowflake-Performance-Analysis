--use schema snowflake_sample_data.tpch_sf1;

WITH customer_revenue AS (
    SELECT 
        o.o_custkey,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue
    FROM 
        orders o
    JOIN 
        lineitem l
        ON o.o_orderkey = l.l_orderkey
    WHERE 
        o.o_orderdate >= '1993-01-01'
        AND o.o_orderdate < '1994-01-01'
    GROUP BY 
        o.o_custkey
),
ranked_customers AS (
    SELECT 
        o_custkey,
        total_revenue,
        ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS rank
    FROM 
        customer_revenue
)
SELECT 
    c.c_custkey,
    c.c_name,
    c.c_address,
    rc.total_revenue
FROM 
    ranked_customers rc
JOIN 
    customer c
    ON rc.o_custkey = c.c_custkey
WHERE 
    rc.rank <= 10
ORDER BY 
    rc.total_revenue DESC;

--01bb6e5f-3204-43bd-0002-2fc60007102a
