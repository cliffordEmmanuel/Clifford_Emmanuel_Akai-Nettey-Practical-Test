queries = {
    "check_db":"SELECT 1 FROM pg_database WHERE datname = :db_name;",
    "top_10":"""
        SELECT
            customerid,
            SUM(unitprice * quantity) as TotalPurchaseAmount
        FROM
            online_retail
        WHERE 
            LOWER(invoiceno) NOT ILIKE 'c%%' --remove cancelled transactions
        GROUP BY
            customerid
        ORDER BY
            TotalPurchaseAmount desc
        LIMIT 10;
    """,
    "popular_products":"""
        SELECT
            stockcode,
            description,
            COUNT(DISTINCT invoiceno) as NumberOfOrders
        FROM
            online_retail
        WHERE
            LOWER(invoiceno) NOT ILIKE 'c%%' --remove cancelled transactions
        GROUP BY
            stockcode,
            description
        ORDER BY
            NumberOfOrders desc;
    """,
    "monthly_revenue":"""
        SELECT
            TO_CHAR(DATE_TRUNC('month', invoicedate), 'YYYY-MM') as Month,
            SUM(unitprice * quantity) as TotalRevenue
        FROM
            online_retail
        WHERE
            LOWER(invoiceno) NOT ILIKE 'c%%' --remove cancelled transactions
        GROUP BY
            DATE_TRUNC('month', invoicedate)
        ORDER BY
            Month;
    """,
}