/* These queries are run against the postgresql version of chinook database */


/* Finds the top 5 most popular genres by total sales. */
SELECT
	g.name AS Genre
	, SUM(il.unit_price * il.quantity ) AS TotalSales
FROM
	genre g
INNER JOIN track t ON
	g.genre_id = t.genre_id
INNER JOIN invoice_line il ON
	t.track_id = il.track_id
GROUP BY
	g.name
ORDER BY
	TotalSales DESC
LIMIT 5
;

/* Calculates the average invoice total by country */
SELECT
	billing_country AS Country
	, AVG(total) AS AverageInvoiceTotal
FROM
	invoice i
GROUP BY
	billing_country
ORDER BY
	AverageInvoiceTotal
;


/* Identifies the top 3 most valued customers based on the total sum of invoices. */
SELECT
	(c.first_name || ' ' || c.last_name) AS CustomerName
	, SUM(i.total) AS TotalSpent
FROM
	customer c
INNER JOIN invoice i ON
	c.customer_id = i.customer_id
GROUP BY
	c.first_name
	, c.last_name
ORDER BY
	TotalSpent
limit 3
;


/*
    Generate a report listing all employees who have sold over a specified 
    amount (provide examples for amounts 1000,1000,5000)
*/

-- Making this query a function to parameterise the sales amount
CREATE OR REPLACE FUNCTION get_employee_sales_report(threshold NUMERIC)
RETURNS TABLE (
	"EmployeeName" TEXT,
	"TotalSales" NUMERIC
) AS $$
BEGIN
	RETURN query
	SELECT
		(e.first_name || ' ' || e.last_name) AS "EmployeeName",
		sum(i.total) AS "TotalSales"
	FROM
		employee e
	INNER JOIN customer c ON
		e.employee_id = c.support_rep_id
	INNER JOIN invoice i ON
		c.customer_id = i.customer_id
	GROUP BY
		e.employee_id
		, e.first_name
		, e.last_name
	HAVING SUM(i.total) > threshold
	ORDER BY
		"TotalSales" DESC;
END;
$$ LANGUAGE plpgsql;


/* Run the report for several total sales amount */
SELECT * FROM get_employee_sales_report(500); 
SELECT * FROM get_employee_sales_report(800);



