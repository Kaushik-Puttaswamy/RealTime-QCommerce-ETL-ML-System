SELECT location, ROUND(AVG(total_price), 2) AS avg_order_value
FROM orders
GROUP BY location
ORDER BY avg_order_value DESC;