
Customer_id INT PRIMARY KEY,
email  VARCHAR(50) UNIQUE,
first_name VARCHAR(50),
last_name VARCHAR(50),
signup_date DATE,
country VARCHAR(50),
customer_tier VARCHAR(50) NULL 
);

CREATE TABLE products (
product_id INT PRIMARY KEY,
product_name VARCHAR(50) NOT NULL,
category VARCHAR(50) NOT NULL,
subcategory VARCHAR(50),
unit_cost INT,CREATE TABLE customers (
c
selling_price INT CHECK (selling_price >= unit_cost),
launch_date DATE,
is_active BOOLEAN
);

CREATE TABLE orders (
order_id INT PRIMARY KEY,
customer_id INT,
order_date DATE,
order_status VARCHAR(50),
payment_method VARCHAR(50),
shipping_country VARCHAR(50),
total_amount INT,
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
order_item_id INT PRIMARY KEY,
order_id INT,
product_id INT,
quantity INT CHECK (quantity > 0),
unit_price INT,
discount_percent INT CHECK (discount_percent BETWEEN 0 AND 100),
FOREIGN KEY (order_id) REFERENCES orders(order_id),
FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE inventory (
inventory_id INT PRIMARY KEY,
product_id INT,
warehouse_location VARCHAR(50),
stock_quantity INT,
last_updated TIMESTAMP,
FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE customer_reviews (
review_id INT PRIMARY KEY,
product_id INT,
customer_id INT,
rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
review_text VARCHAR(150),
review_date DATE,
is_verified_purchase BOOLEAN NOT NULL DEFAULT FALSE,
FOREIGN KEY (product_id) REFERENCES products(product_id),
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE promotions (
promotion_id INT PRIMARY KEY,
promotion_code VARCHAR(50) UNIQUE,
discount_type VARCHAR(50) CHECK (discount_type IN ('percentage', 'fixed_amount')),
discount_value INT CHECK (discount_value > 0),
start_date DATE NOT NULL CHECK (start_date <= end_date),
end_date DATE NOT NULL,
max_uses INT DEFAULT 1,
times_used INT DEFAULT 0 
);

ALTER TABLE promotions
ADD CONSTRAINT chk_start_before_end
CHECK (start_date <= end_date);

CREATE TABLE promotion_usage (
usage_id INT PRIMARY KEY,
promotion_id INT,
order_id INT,
discount_applied BOOLEAN,
used_date DATE,
FOREIGN KEY (promotion_id) REFERENCES promotions(promotion_id),
FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
SELECT inet_server_port();
SELECT * FROM customers;
SELECT * FROM products;
SELECT * FROM orders;
SELECT * FROM order_items;
SELECT * FROM inventory;
SELECT * FROM customer_reviews;
SELECT * FROM promotions;
SELECT * FROM promotion_usage;
CREATE TABLE orders_backup AS SELECT * FROM orders;


SELECT SUM(CASE WHEN usage_id IS NULL THEN 1 ELSE 0 END) AS Null_order_id,
SUM(CASE WHEN promotion_id IS NULL THEN 1 ELSE 0 END) AS Null_customer_id,
SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS Null_order_date,
SUM(CASE WHEN discount_applied IS NULL THEN 1 ELSE 0 END) AS Null_order_status,
SUM(CASE WHEN used_date IS NULL THEN 1 ELSE 0 END) AS Null_payment_method
SUM(CASE WHEN end_date IS NULL THEN 1 ELSE 0 END) AS Null_shipping_country,
SUM(CASE WHEN max_uses IS NULL THEN 1 ELSE 0 END) AS Null_shipping_country,
SUM(CASE WHEN times_used IS NULL THEN 1 ELSE 0 END) AS Null_shipping_country
FROM promotions;

--
CREATE OR REPLACE VIEW vw_base_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    DATE_TRUNC('month', o.order_date) AS order_month,
    o.order_status,
    o.payment_method,
    o.shipping_country,
    o.final_total   -- ✅ USE THIS
FROM orders o;

SELECT * FROM vw_base_orders
CREATE OR REPLACE VIEW vw_base_order_items AS
SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.discount_percent,
    (oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100.0)) AS net_amount
FROM order_items oi;

CREATE OR REPLACE VIEW vw_base_products AS
SELECT
    product_id,
    product_name,
    category,
    subcategory,
    unit_cost,
    selling_price,
    launch_date,
    is_active
FROM products;

CREATE OR REPLACE VIEW vw_base_customers AS
SELECT
    customer_id,
    signup_date,
    country,
    customer_tier,
	is_active
FROM customers;

CREATE OR REPLACE VIEW vw_base_reviews AS
SELECT
    product_id,
    customer_id,
    rating,
    review_date
FROM customer_reviews
WHERE is_verified_purchase = TRUE;

CREATE OR REPLACE VIEW vw_base_inventory AS
SELECT
    product_id,
    SUM(stock_quantity) AS current_stock,
    MAX(last_updated) AS last_updated
FROM inventory
GROUP BY product_id;


--Find orders with invalid customer_id (customer doesn't exist) -- No Issues (-- Fixed customers table customer_tier Null into Unknown)
SELECT o.order_id, o.customer_id
FROM orders AS o LEFT JOIN customers AS c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL OR o.customer_id IS NULL;

-- 1a) Fix: set invalid customer_id to NULL (keeps FK integrity since NULLs are allowed)
UPDATE orders
SET customer_id = NULL
WHERE customer_id IS NOT NULL
  AND customer_id NOT IN (SELECT customer_id FROM customers);

--Find order_items with invalid product_id  --No issues
SELECT COUNT(*) FROM  order_items AS oi LEFT JOIN products AS p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;



--Find reviews from customers who never bought the product -- fixed
SELECT * FROM customer_reviews
WHERE is_verified_purchase IS false;

--Find reviews from customers who never bought the product  -- With update 
SELECT r.review_id, r.customer_id, r.product_id,
       o.order_id AS matched_order
FROM customer_reviews r
LEFT JOIN orders o
    ON o.customer_id = r.customer_id
LEFT JOIN order_items oi
    ON oi.order_id = o.order_id
   AND oi.product_id = r.product_id;

UPDATE customer_reviews AS r
SET is_verified_purchase = CASE 
    WHEN sub.order_id IS NULL THEN FALSE
    ELSE TRUE
END
FROM (
    SELECT 
        r.review_id,
        oi.order_id
    FROM customer_reviews r
    LEFT JOIN orders o 
        ON o.customer_id = r.customer_id
    LEFT JOIN order_items oi
        ON oi.order_id = o.order_id
       AND oi.product_id = r.product_id
) AS sub
WHERE r.review_id = sub.review_id;




--Find orders where total_amount ≠ sum of (order_items.quantity * unit_price * (1 - discount_percent/100))
WITH calc AS (SELECT o.order_id, o.total_amount, 
COALESCE(SUM(oi.quantity * oi.unit_price * (1 - discount_percent/100)),0) AS qty_sum
FROM orders AS o LEFT JOIN order_items AS oi ON o.order_id = oi.order_id
GROUP BY o.order_id, o.total_amount)
SELECT order_id, total_amount, qty_sum, (total_amount - qty_sum) AS Difference
FROM calc
WHERE total_amount <> qty_sum;



--Find products where selling_price < unit_cost
SELECT product_id, selling_price , unit_cost
FROM products
WHERE  selling_price IS NOT NULL
AND unit_cost IS NOT NULL 
AND selling_price < unit_cost;

--Find promotions where times_used  > max_uses
SELECT promotion_id, times_used, max_uses, (max_uses - times_used) AS times_left
FROM promotions
WHERE times_used IS NOT NULL
AND max_uses IS NOT NULL
AND times_used > max_uses;


--Find duplicate customer emails
SELECT lower(trim(email)) AS mail, COUNT(trim(email)) AS mail_count
FROM customers
GROUP BY lower(trim(email))
HAVING COUNT(trim(email)) > 1

SELECT REPLACE(LOWER(TRIM(email)), ' ', '') AS email_norm,
       COUNT(*) AS cnt
FROM customers
GROUP BY REPLACE(LOWER(TRIM(email)), ' ', '')
HAVING COUNT(*) > 1;

SELECT 
    REGEXP_REPLACE(LOWER(email), '[\s]+', '', 'g') AS email_norm,
    COUNT(*)
FROM customers
GROUP BY REGEXP_REPLACE(LOWER(email), '[\s]+', '', 'g')
HAVING COUNT(*) > 1;


-- Find products with no inventory record
SELECT * FROM products;
SELECT * FROM inventory;

SELECT p.product_id, i.product_id, i.stock_quantity, i.last_updated
FROM products AS p
LEFT JOIN inventory AS i ON p.product_id = i.product_id
WHERE i.product_id IS NULL;


--Find customers with no orders (how many inactive customers?)
SELECT * FROM customers;
SELECT * FROM orders;

SELECT c.customer_id, o.customer_id, o.order_id, o.total_amount
FROM customers AS c LEFT JOIN orders AS o ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;


--Find orders with future dates  --(None)
SELECT MAX(order_date) AS last_order_date
FROM orders;

--Find ratings outside 1-5 range  --(None)
SELECT review_id, rating 
FROM customer_reviews
WHERE rating NOT BETWEEN 1 AND 5;


--Find negative quantities or prices  -- (None)
SELECT product_id, product_name, unit_cost, selling_price
FROM products 
WHERE unit_cost <= 0 OR selling_price <= 0;

SELECT p.product_id, i.stock_quantity
FROM products AS p LEFT JOIN inventory AS i ON p.product_id = i.product_id
WHERE i.stock_quantity <= 0 ;
SELECT * FROM order_items
SELECT * FROM products
SELECT * FROM inventory

---- There is 11 Null in subcategory - products --  fixed 
UPDATE products
SET subcategory = 'General'
WHERE subcategory IS NULL;


-- There is 5 Null in total_amount - orders -- need fix
SELECT * FROM orders
WHERE total_amount IS NULL;

SELECT o.order_id, SUM(oi.quantity * oi.unit_price) AS total_amount
FROM orders AS o LEFT JOIN order_items AS oi ON o.order_id = oi.order_id
WHERE o.total_amount IS NULL
GROUP BY o.order_id;


UPDATE orders o
SET total_amount = sub.calculated_total
FROM (
    SELECT 
        order_id,
        COALESCE(SUM(quantity * unit_price), 0) AS calculated_total
    FROM order_items
    GROUP BY order_id
) AS sub
WHERE o.order_id = sub.order_id
  AND o.total_amount IS NULL;

--There is Null in review_text - customer_reviews -- need fix
SELECT * FROM customer_reviews
WHERE review_text IS NULL;


--Find orders where total_amount ≠ sum of (order_items.quantity * unit_price * (1 - discount_percent/100)) 
-- There are errors qty_sum is < total_amount  -- fixed 
ALTER TABLE orders
ADD COLUMN final_total NUMERIC(10,2);


-- Fix the final_total (using 100.0 to handle decimals correctly)
UPDATE orders o
SET final_total = sub.calculated_total
FROM (
    SELECT 
        order_id,
        ROUND(SUM(quantity * unit_price * (1 - COALESCE(discount_percent,0)/100.0)), 2) AS calculated_total
    FROM order_items
    GROUP BY order_id
) AS sub
WHERE o.order_id = sub.order_id;

--Find orders where total_amount ≠ sum of (order_items.quantity * unit_price * (1 - discount_percent/100))
WITH calc AS (SELECT o.order_id, o.final_total, 
COALESCE(SUM(oi.quantity * oi.unit_price * (1 - discount_percent/100)),0) AS qty_sum
FROM orders AS o LEFT JOIN order_items AS oi ON o.order_id = oi.order_id
GROUP BY o.order_id, o.final_total)
SELECT order_id, final_total, qty_sum, (final_total - qty_sum) AS Difference
FROM calc
WHERE final_total <> qty_sum;

SELECT * FROM orders;

--validation
SELECT 
    o.order_id,
    o.final_total,
    SUM(oi.quantity * oi.unit_price) AS total_without_discount,
    (o.final_total - SUM(oi.quantity * oi.unit_price)) AS difference
FROM orders AS o
LEFT JOIN order_items AS oi 
    ON o.order_id = oi.order_id
GROUP BY 
    o.order_id, 
    o.final_total
	HAVING (o.final_total - SUM(oi.quantity * oi.unit_price)) > 0
ORDER BY 
    difference DESC;


--Find customers with no orders (how many inactive customers?)  -- fixed
SELECT c.customer_id, o.customer_id, o.order_id, o.total_amount
FROM customers AS c LEFT JOIN orders AS o ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;

ALTER TABLE customers
ADD COLUMN is_active BOOLEAN DEFAULT FALSE;

UPDATE customers
SET is_active = TRUE
WHERE customer_id IN (
SELECT DISTINCT customer_id FROM orders
);

SELECT * FROM customers 
WHERE is_active is FALSE;


-- stock_quantity in minus is fixed
SELECT p.product_id, i.stock_quantity
FROM products AS p LEFT JOIN inventory AS i ON p.product_id = i.product_id
WHERE i.stock_quantity <= 0 ;

UPDATE inventory
SET stock_quantity = 0
WHERE stock_quantity < 0;


--Essential Business Queries (Part 1)
--**Sales Performance:**
--1. Total revenue by month for 2024
EXPLAIN ANALYZE
SELECT
    TO_CHAR(order_month,'YYYY-MM') AS month,
    SUM(final_total) AS total_sales
FROM vw_base_orders
WHERE order_date >= DATE '2024-01-01'
  AND order_date <  DATE '2025-01-01'
GROUP BY order_month
ORDER BY order_month;




-- 2. Top 10 Products by Revenue
WITH product_revenue AS (
    SELECT
        p.product_id,
        p.product_name,
        COALESCE(SUM(oi.net_amount), 0) AS total_revenue
    FROM vw_base_products p
    LEFT JOIN vw_base_order_items oi
        ON p.product_id = oi.product_id
    GROUP BY
        p.product_id,
        p.product_name
)
SELECT
    product_id,
    product_name,
    ROUND(total_revenue,2) AS total_revenue,
    DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM product_revenue
WHERE total_revenue > 0
ORDER BY revenue_rank
LIMIT 10;





-- 3. Revenue by Category (with Percentage of Total)
WITH category_revenue AS (
    SELECT
        p.category,
        COALESCE(SUM(oi.net_amount), 0) AS total_revenue
    FROM vw_base_products p
    LEFT JOIN vw_base_order_items oi
        ON p.product_id = oi.product_id
    GROUP BY p.category
)
SELECT
    category,
    ROUND(total_revenue,2) AS total_revenue,
    ROUND(
        total_revenue * 100.0 / SUM(total_revenue) OVER (),
        2
    ) AS percent_of_total
FROM category_revenue
ORDER BY total_revenue DESC;



--4. Average order value by country
SELECT
    c.country,
    ROUND(SUM(o.final_total)::numeric / COUNT(o.order_id), 2) AS avg_order_value
FROM vw_base_customers c
LEFT JOIN vw_base_orders o
  ON c.customer_id = o.customer_id
WHERE c.country IS NOT NULL
GROUP BY c.country
ORDER BY avg_order_value DESC;

--5. Customer acquisition by month (new signups)  
WITH signup_months AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', signup_date) AS signup_month
    FROM customers
)
SELECT
    TO_CHAR(signup_month, 'YYYY-MM') AS signup_month,
    COUNT(customer_id) AS total_signup_customers
FROM signup_months
GROUP BY signup_month
ORDER BY signup_month;




--6. Customer lifetime value (total spent per customer)
SELECT
    c.customer_id,
    COALESCE(SUM(o.final_total), 0) AS customer_lifetime_value
FROM vw_base_customers c
LEFT JOIN vw_base_orders o
    ON c.customer_id = o.customer_id
GROUP BY c.customer_id
ORDER BY c.customer_id;



-- 7. Repeat Purchase Rate (% of customers with 2+ orders)
WITH customer_orders AS (
    SELECT
        c.customer_id,
        COUNT(o.order_id) AS order_count
    FROM vw_base_customers c
    LEFT JOIN vw_base_orders o
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
),
counts AS (
    SELECT
        COUNT(*) AS total_customers,
        COUNT(*) FILTER (WHERE order_count >= 2) AS repeat_customers_2_plus,
        COUNT(*) FILTER (WHERE order_count >= 3) AS loyal_customers_3_plus
    FROM customer_orders
)
SELECT
    ROUND(
        repeat_customers_2_plus::NUMERIC
        / NULLIF(total_customers, 0) * 100,
        2
    ) AS repeat_purchase_rate_2_plus,
    ROUND(
        loyal_customers_3_plus::NUMERIC
        / NULLIF(total_customers, 0) * 100,
        2
    ) AS repeat_purchase_rate_3_plus
FROM counts;


-- 8. Average time between first and second order               
WITH ranked_orders AS (
    SELECT
        customer_id,
        order_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date, order_id
        ) AS order_rank
    FROM vw_base_orders
),

first_second_orders AS (
    SELECT
        customer_id,
        MAX(CASE WHEN order_rank = 1 THEN order_date END) AS first_order_date,
        MAX(CASE WHEN order_rank = 2 THEN order_date END) AS second_order_date
    FROM ranked_orders
    GROUP BY customer_id
)

SELECT
    ROUND(AVG(second_order_date - first_order_date), 2)
        AS avg_days_until_second_order
FROM first_second_orders
WHERE second_order_date IS NOT NULL;



--9. Customers who haven't ordered in 90+ days (churn risk)      
WITH last_order AS (
    SELECT
        c.customer_id,
        MAX(o.order_date) AS last_order_date
    FROM vw_base_customers c
    LEFT JOIN vw_base_orders o
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
)
SELECT
    customer_id,
    DATE '2025-01-01' - last_order_date AS days_since_last_order
FROM last_order
WHERE last_order_date IS NOT NULL
  AND DATE '2025-01-01' - last_order_date > 90;



--10. Customer tier distribution (how many bronze/silver/gold?)  
WITH customer_spend AS (
    SELECT
        c.customer_id,
        COALESCE(SUM(o.final_total), 0) AS total_spend
    FROM vw_base_customers c
    LEFT JOIN vw_base_orders o
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
),
customer_tiers AS (
    SELECT
        customer_id,
        total_spend,
        CASE
            WHEN total_spend >= 20000 THEN 'Gold'
            WHEN total_spend >= 10000 THEN 'Silver'
            ELSE 'Bronze'
        END AS customer_tier
    FROM customer_spend
)
SELECT
    customer_tier,
    COUNT(*) AS customer_count
FROM customer_tiers
GROUP BY customer_tier
ORDER BY customer_count DESC;





--Essential Business Queries (Part 2)

--**Product Performance:**
--11. Products with inventory below 20 units (reorder alert)
SELECT
    p.product_id,
    p.product_name,
    i.current_stock,
    CASE
        WHEN i.current_stock < 20 THEN 'reorder_alert'
        ELSE 'enough_stock'
    END AS stock_status
FROM vw_base_products p
LEFT JOIN vw_base_inventory i
    ON p.product_id = i.product_id
WHERE i.current_stock < 20;


-- 12. Average rating per product (only verified purchases)
SELECT
    p.product_id,
    p.product_name,
    ROUND(AVG(r.rating), 2) AS avg_rating
FROM vw_base_products p
JOIN vw_base_reviews r
  ON p.product_id = r.product_id
GROUP BY
    p.product_id,
    p.product_name;


--13. Products with no sales in last 30 days 
WITH last_sales AS (
    SELECT
        p.product_id,
        p.product_name,
        MAX(o.order_date) AS last_order_date
    FROM vw_base_products p
    LEFT JOIN vw_base_order_items oi
        ON p.product_id = oi.product_id
    LEFT JOIN vw_base_orders o
        ON oi.order_id = o.order_id
    GROUP BY
        p.product_id,
        p.product_name
)
SELECT
    product_id,
    product_name,
    last_order_date
FROM last_sales
WHERE last_order_date IS NULL
   OR last_order_date < DATE '2024-12-30' - INTERVAL '30 days';


-- 14. Profit margin by category
-- Formula: ((Avg Selling Price - Avg Unit Cost) / Avg Selling Price) * 100
WITH category_avg_costs AS (
    SELECT
        category,
        AVG(selling_price) AS avg_selling_price,
        AVG(unit_cost)     AS avg_unit_cost
    FROM vw_base_products
    GROUP BY category
)
SELECT
    category,
    ROUND(
        ((avg_selling_price - avg_unit_cost)
         / NULLIF(avg_selling_price, 0)) * 100,
        2
    ) AS profit_margin_percent
FROM category_avg_costs
ORDER BY profit_margin_percent DESC;


--15. Best-selling product per category
WITH product_sales AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        SUM(oi.net_amount) AS total_sales
    FROM vw_base_order_items oi
    JOIN vw_base_products p
        ON oi.product_id = p.product_id
    GROUP BY
        p.product_id,
        p.product_name,
        p.category
),
ranked_products AS (
    SELECT
        product_id,
        product_name,
        category,
        total_sales,
        DENSE_RANK() OVER (
            PARTITION BY category
            ORDER BY total_sales DESC
        ) AS product_rank
    FROM product_sales
)
SELECT
    category,
    product_id,
    product_name,
    total_sales
FROM ranked_products
WHERE product_rank = 1
ORDER BY total_sales DESC;


--**Order Analytics:**         
--16. Orders by status (count and % of total) 
WITH orders_by_status AS (
    SELECT
        order_status,
        COUNT(*) AS status_count
    FROM vw_base_orders
    GROUP BY order_status
),
total_orders AS (
    SELECT
        COUNT(*) AS total_count
    FROM vw_base_orders
)
SELECT
    obs.order_status,
    obs.status_count,
    ROUND(
        obs.status_count * 100.0 / t.total_count,
        2
    ) AS status_percentage
FROM orders_by_status obs
CROSS JOIN total_orders t
ORDER BY status_percentage DESC;




--17. Average items per order
WITH order_quantity AS (
    SELECT
        order_id,
        SUM(quantity) AS total_items_per_order
    FROM vw_base_order_items
    GROUP BY order_id
)
SELECT
    ROUND(AVG(total_items_per_order), 2) AS avg_items_per_order
FROM order_quantity;


--18. Most popular payment method by country
WITH payment_usage AS (
    SELECT
        shipping_country,
        payment_method,
        COUNT(*) AS usage_count
    FROM vw_base_orders
    GROUP BY shipping_country, payment_method
),
ranked_payment_methods AS (
    SELECT
        shipping_country,
        payment_method,
        usage_count,
        DENSE_RANK() OVER (
            PARTITION BY shipping_country
            ORDER BY usage_count DESC
        ) AS payment_rank
    FROM payment_usage
)
SELECT
    shipping_country,
    payment_method,
    usage_count
FROM ranked_payment_methods
WHERE payment_rank = 1
ORDER BY shipping_country;


--19. Orders that used a promotion code
SELECT
    o.order_id,
    o.order_date,
    o.order_status,
    o.shipping_country,
    o.final_total,
    TRUE AS promotion_used,
    MAX(pu.used_date) AS promotion_used_date
FROM vw_base_orders o
JOIN promotion_usage pu
    ON o.order_id = pu.order_id
GROUP BY
    o.order_id,
    o.order_date,
    o.order_status,
    o.shipping_country,
    o.final_total
ORDER BY o.order_date;



--20. Average discount percent applied per order
WITH order_level_discount AS (
    SELECT
        oi.order_id,
        SUM(oi.quantity * oi.unit_price) AS gross_order_value,
        SUM(oi.quantity * oi.unit_price * (oi.discount_percent / 100.0)) AS discount_value
    FROM vw_base_order_items oi
    GROUP BY oi.order_id
)
SELECT
    ROUND(
        AVG((discount_value / NULLIF(gross_order_value, 0)) * 100),
        2
    ) AS avg_discount_percent_per_order
FROM order_level_discount;





--**Your Task:** Write queries combining 3+ tables with advanced logic
--**Challenge Questions:**
--**21. Customer Cohort Analysis**
--For customers who signed up in Q1 2024:
-- How many are still active (ordered in last 30 days)?
-- What's their average order count?
-- What's their total revenue contribution?
-- Compare to Q2 2024 cohort
-- Cohort comparison: Q1 2024 vs Q2 2024
-- Reference date used for "last 30 days" (adjust if needed)
-- Reference parameters
WITH params AS (
    SELECT 
        DATE '2024-12-31' AS ref_date,
        30 AS window_days
),

-- Customer cohorts (using base customers view)
cohorts AS (
    SELECT 
        customer_id,
        'Q1 2024' AS cohort
    FROM vw_base_customers
    WHERE signup_date >= DATE '2024-01-01'
      AND signup_date <  DATE '2024-04-01'

    UNION ALL

    SELECT 
        customer_id,
        'Q2 2024' AS cohort
    FROM vw_base_customers
    WHERE signup_date >= DATE '2024-04-01'
      AND signup_date <  DATE '2024-07-01'
),

-- Orders aggregated per customer (using base orders view)
orders_by_customer AS (
    SELECT
        customer_id,
        COUNT(order_id)               AS order_count,
        COALESCE(SUM(final_total),0)  AS total_revenue,
        MAX(order_date)               AS last_order_date
    FROM vw_base_orders
    GROUP BY customer_id
),

-- Join cohorts with metrics
cohort_customer_metrics AS (
    SELECT
        c.cohort,
        c.customer_id,
        COALESCE(o.order_count, 0)    AS order_count,
        COALESCE(o.total_revenue, 0)  AS total_revenue,
        o.last_order_date,
        p.ref_date,
        p.window_days
    FROM cohorts c
    LEFT JOIN orders_by_customer o
        ON c.customer_id = o.customer_id
    CROSS JOIN params p
)

-- Final cohort comparison
SELECT
    cohort,
    COUNT(customer_id) AS total_customers,

    SUM(
        CASE 
            WHEN last_order_date >= ref_date - (window_days * INTERVAL '1 day')
            THEN 1 ELSE 0
        END
    ) AS active_customers_last_30d,

    ROUND(AVG(order_count)::numeric, 2) AS avg_orders_per_customer,
    SUM(order_count) AS total_orders,

    ROUND(SUM(total_revenue)::numeric, 2) AS total_revenue,
    ROUND(AVG(total_revenue)::numeric, 2) AS avg_revenue_per_customer,

    ROUND(
        CASE 
            WHEN SUM(order_count) = 0 THEN 0
            ELSE SUM(total_revenue) / SUM(order_count)
        END::numeric, 2
    ) AS avg_order_value,

    ROUND(
        SUM(
            CASE 
                WHEN last_order_date >= ref_date - (window_days * INTERVAL '1 day')
                THEN 1 ELSE 0
            END
        )::numeric / COUNT(customer_id),
        4
    ) AS pct_active

FROM cohort_customer_metrics
GROUP BY cohort
ORDER BY cohort;


--**22. Product Performance Dashboard**
--For each product, calculate:
--- Total revenue -- 
--- Total units sold -- 
--- Average rating --
--- Number of reviews --
--- Current inventory level -- 
--- Last sale date
--- Status: 'Hot' (sold 50+ units), 'Moderate'(10-49), 'Slow' (<10)
WITH product_sales AS (
    SELECT
        p.product_id,
        COALESCE(SUM(oi.net_amount), 0) AS total_revenue,
        COALESCE(SUM(oi.quantity), 0) AS total_units_sold,
        MAX(o.order_date) AS last_sale_date
    FROM vw_base_products p
    LEFT JOIN vw_base_order_items oi
        ON p.product_id = oi.product_id
    LEFT JOIN vw_base_orders o
        ON oi.order_id = o.order_id
    GROUP BY p.product_id
),

product_reviews AS (
    SELECT
        product_id,
        ROUND(AVG(rating), 2) AS avg_rating,
        COUNT(*) AS review_count
    FROM vw_base_reviews
    GROUP BY product_id
)

SELECT
    p.product_id,
    ps.total_revenue,
    ps.total_units_sold,
    COALESCE(pr.avg_rating, 0) AS avg_rating,
    COALESCE(pr.review_count, 0) AS review_count,
    COALESCE(i.current_stock, 0) AS current_inventory,
    ps.last_sale_date,
    CASE
        WHEN ps.total_units_sold >= 50 THEN 'Hot'
        WHEN ps.total_units_sold BETWEEN 10 AND 49 THEN 'Moderate'
        ELSE 'Slow'
    END AS product_status
FROM vw_base_products p
LEFT JOIN product_sales ps
    ON p.product_id = ps.product_id
LEFT JOIN product_reviews pr
    ON p.product_id = pr.product_id
LEFT JOIN vw_base_inventory i
    ON p.product_id = i.product_id;


--**23. Promotion Effectiveness**
--For each promotion:
-- Times used
-- Total discount given
-- Average order value with promotion vs without
-- Most popular product bought with this promotion
-- ROI: (Revenue from promotion orders - Total discount) / Total discount


WITH
/* 1) Times used per promotion */
promo_usage_counts AS (
    SELECT
        promotion_id,
        COUNT(order_id) AS times_used
    FROM promotion_usage
    GROUP BY promotion_id
),

/* 2) Actual discount given (item-level truth) */
promo_discounts AS (
    SELECT
        pu.promotion_id,
        ROUND(
            SUM(
                oi.quantity * oi.unit_price * (oi.discount_percent / 100.0)
            ), 2
        ) AS total_discount_given
    FROM promotion_usage pu
    JOIN vw_base_order_items oi
        ON pu.order_id = oi.order_id
    GROUP BY pu.promotion_id
),

/* 3) Revenue from promotion orders (net revenue) */
promo_revenue AS (
    SELECT
        pu.promotion_id,
        SUM(oi.net_amount) AS total_revenue
    FROM promotion_usage pu
    JOIN vw_base_order_items oi
        ON pu.order_id = oi.order_id
    GROUP BY pu.promotion_id
),

/* 4) Average order value with vs without promotion */
order_promo_flag AS (
    SELECT
        o.order_id,
        o.final_total,
        CASE
            WHEN pu.order_id IS NOT NULL THEN 1
            ELSE 0
        END AS has_promotion
    FROM vw_base_orders o
    LEFT JOIN promotion_usage pu
        ON o.order_id = pu.order_id
),

avg_values AS (
    SELECT
        ROUND(
            AVG(CASE WHEN has_promotion = 1 THEN final_total END),
            2
        ) AS avg_with_promotion,
        ROUND(
            AVG(CASE WHEN has_promotion = 0 THEN final_total END),
            2
        ) AS avg_without_promotion
    FROM order_promo_flag
),

/* 5) Most popular product per promotion */
product_counts AS (
    SELECT
        pu.promotion_id,
        oi.product_id,
        COUNT(*) AS purchase_count
    FROM promotion_usage pu
    JOIN vw_base_order_items oi
        ON pu.order_id = oi.order_id
    GROUP BY pu.promotion_id, oi.product_id
),

ranked_products AS (
    SELECT
        promotion_id,
        product_id,
        ROW_NUMBER() OVER (
            PARTITION BY promotion_id
            ORDER BY purchase_count DESC
        ) AS rn
    FROM product_counts
),

top_product AS (
    SELECT
        promotion_id,
        product_id AS most_popular_product
    FROM ranked_products
    WHERE rn = 1
),

/* 6) ROI calculation */
roi_calc AS (
    SELECT
        r.promotion_id,
        ROUND(
            (r.total_revenue - d.total_discount_given)
            / NULLIF(d.total_discount_given, 0),
            2
        ) AS roi
    FROM promo_revenue r
    JOIN promo_discounts d
        ON r.promotion_id = d.promotion_id
)

/* Final output */
SELECT
    puc.promotion_id,
    puc.times_used,
    pd.total_discount_given,
    tp.most_popular_product,
    av.avg_with_promotion,
    av.avg_without_promotion,
    rc.roi
FROM promo_usage_counts puc
LEFT JOIN promo_discounts pd
    ON puc.promotion_id = pd.promotion_id
LEFT JOIN top_product tp
    ON puc.promotion_id = tp.promotion_id
LEFT JOIN roi_calc rc
    ON puc.promotion_id = rc.promotion_id
CROSS JOIN avg_values av
ORDER BY puc.promotion_id;





--**24. Customer Segmentation (RFM Analysis)**
--For each customer, calculate:
--- Recency: Days since last order
-- Frequency: Total number of orders
-- Monetary: Total amount spent

--Then classify into segments
-- 'Champions': Recent, Frequent, High spend
-- 'Loyal': Frequent, High spend, but not recent
-- 'At Risk': Not recent, but was frequent
-- 'Lost': Not recent, low frequency
WITH rfm_base AS (
    SELECT
        c.customer_id,
        DATE '2024-12-31' - MAX(o.order_date) AS recency_days,
        COUNT(o.order_id) AS frequency,
        COALESCE(SUM(o.final_total), 0) AS monetary
    FROM vw_base_customers c
    LEFT JOIN vw_base_orders o
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
)
SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    CASE
        WHEN recency_days <= 15
             AND frequency >= 5
             AND monetary >= 12000
            THEN 'Champions'
        WHEN recency_days > 15
             AND frequency >= 5
             AND monetary >= 12000
            THEN 'Loyal'
        WHEN recency_days > 30
             AND frequency >= 3
            THEN 'At Risk'
        ELSE 'Lost'
    END AS segment
FROM rfm_base;




--### Day 8: Time-Series & Trend Analysis


--**Your Task:** Write queries analyzing trends over time
--**25. Revenue Growth Rate (Month-over-Month)**
--For each month:
-- Total revenue
-- Previous month revenue
-- Growth rate %
-- 3-month moving average
-- 25. Revenue Growth Rate (Month-over-Month)
WITH monthly_revenue AS (
    SELECT
        order_month,
        SUM(final_total) AS total_revenue
    FROM vw_base_orders
    GROUP BY order_month
),
revenue_with_lag AS (
    SELECT
        order_month,
        total_revenue,
        LAG(total_revenue) OVER (ORDER BY order_month) AS previous_month_revenue
    FROM monthly_revenue
)
SELECT
    TO_CHAR(order_month, 'YYYY-MM') AS month_year,
    total_revenue,
    COALESCE(previous_month_revenue, 0) AS previous_month_revenue,
    ROUND(
        (total_revenue - previous_month_revenue)
        / NULLIF(previous_month_revenue, 0) * 100,
        2
    ) AS growth_rate_percent,
    ROUND(
        AVG(total_revenue) OVER (
            ORDER BY order_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS three_month_moving_average
FROM revenue_with_lag
ORDER BY order_month;



SELECT * FROM customers;
SELECT * FROM products;
SELECT * FROM orders;
SELECT * FROM order_items;
SELECT * FROM inventory;
SELECT * FROM customer_reviews;
SELECT * FROM promotions;
SELECT * FROM promotion_usage;

--**26. Customer Retention Curve**
--For customers acquired each month:
-- How many made 2nd purchase within 30 days? 
-- How many made 2nd purchase within 60 days?
-- How many made 2nd purchase within 90 days?
-- Customer Retention Curve: 2nd purchase within 30 / 60 / 90 days

WITH ordered_orders AS (
    SELECT
        customer_id,
        order_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date
        ) AS order_rank
    FROM vw_base_orders
),

first_second_orders AS (
    SELECT
        customer_id,
        MAX(CASE WHEN order_rank = 1 THEN order_date END) AS first_order_date,
        MAX(CASE WHEN order_rank = 2 THEN order_date END) AS second_order_date
    FROM ordered_orders
    GROUP BY customer_id
)

SELECT
    COUNT(*) FILTER (
        WHERE second_order_date <= first_order_date + INTERVAL '30 days'
    ) AS customers_30_days,

    COUNT(*) FILTER (
        WHERE second_order_date <= first_order_date + INTERVAL '60 days'
    ) AS customers_60_days,

    COUNT(*) FILTER (
        WHERE second_order_date <= first_order_date + INTERVAL '90 days'
    ) AS customers_90_days
FROM first_second_orders
WHERE second_order_date IS NOT NULL;





--**27. Product Lifecycle Analysis**
--For each product:
-- Launch date
-- Days_since_launch
-- Monthly sales trend (first 3 months after launch)
-- Whether sales are increasing, stable, or declining
WITH product_lifecycle AS (
    SELECT
        p.product_id,
        p.product_name,
        p.launch_date,
        (DATE '2024-12-31' - p.launch_date) AS days_since_launch
    FROM vw_base_products p
),

sales_trends AS (
    SELECT
        pl.product_id,
        pl.product_name,
        pl.launch_date,
        pl.days_since_launch,
        EXTRACT(MONTH FROM AGE(o.order_date, pl.launch_date)) + 1 AS month_number,
        SUM(oi.net_amount) AS monthly_revenue
    FROM product_lifecycle pl
    LEFT JOIN vw_base_order_items oi
        ON pl.product_id = oi.product_id
    LEFT JOIN vw_base_orders o
        ON oi.order_id = o.order_id
    WHERE
        o.order_date >= pl.launch_date
        AND o.order_date < pl.launch_date + INTERVAL '3 months'
    GROUP BY
        pl.product_id,
        pl.product_name,
        pl.launch_date,
        pl.days_since_launch,
        month_number
),

monthly_pivot AS (
    SELECT
        product_id,
        product_name,
        launch_date,
        days_since_launch,
        ROUND(COALESCE(SUM(CASE WHEN month_number = 1 THEN monthly_revenue END), 0),2) AS m1,
        ROUND(COALESCE(SUM(CASE WHEN month_number = 2 THEN monthly_revenue END), 0),2) AS m2,
        ROUND(COALESCE(SUM(CASE WHEN month_number = 3 THEN monthly_revenue END), 0),2) AS m3
    FROM sales_trends
    GROUP BY
        product_id,
        product_name,
        launch_date,
        days_since_launch
)

SELECT
    *,
    CASE
        WHEN m1 < m2 AND m2 < m3 THEN 'Increasing'
        WHEN m1 > m2 AND m2 > m3 THEN 'Declining'
        ELSE 'Stable'
    END AS sales_trend
FROM monthly_pivot
ORDER BY product_id;







-- **28. Seasonal Patterns**
-- Compare sales by:
-- Day of week (Monday vs Sunday patterns)
-- Month (seasonal trends)
-- Holiday periods vs non-holiday
-- Day of Week Sales Pattern

SELECT
    CASE EXTRACT(DOW FROM order_date)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_of_week,
    COUNT(order_id) AS total_orders,
    SUM(final_total) AS total_sales
FROM vw_base_orders
GROUP BY EXTRACT(DOW FROM order_date)
ORDER BY (EXTRACT(DOW FROM order_date) + 6) % 7; -- Monday first


-- Monthly Seasonal Trends

SELECT
    TO_CHAR(order_month, 'YYYY-MM') AS year_month,
    COUNT(order_id) AS total_orders,
    SUM(final_total) AS total_sales
FROM vw_base_orders
GROUP BY order_month
ORDER BY order_month;



-- Holiday vs Non-Holiday Sales Comparison

SELECT
    CASE
        WHEN EXTRACT(DOW FROM order_date) = 0 THEN 'Holiday'
        ELSE 'Non-Holiday'
    END AS day_type,
    COUNT(order_id) AS total_orders,
    SUM(final_total) AS total_sales
FROM vw_base_orders
GROUP BY day_type;



--Indexes
-- Orders: most reports filter/join on order_date and customer_id
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);



-- Order items: join to orders and products, aggregate quantities
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_order_items_order_product ON order_items(order_id, product_id);



-- Products: category filters and active flag are common
CREATE INDEX idx_products_category_active ON products(category, is_active);
CREATE INDEX idx_products_launch_date ON products(launch_date);

-- Inventory: join by product_id
CREATE INDEX idx_inventory_product_id ON inventory(product_id);

-- Customers: signup_date (cohorts), country
CREATE INDEX idx_customers_signup_date ON customers(signup_date);
CREATE INDEX idx_customers_country ON customers(country);

-- Reviews: lookup by product and customer
CREATE INDEX idx_reviews_product_customer ON customer_reviews(product_id, customer_id);
CREATE INDEX idx_reviews_review_date ON customer_reviews(review_date);

-- Promotions
CREATE INDEX idx_promotions_dates ON promotions(start_date, end_date);
CREATE INDEX idx_promotion_usage_promo ON promotion_usage(promotion_id);



-- Quality assurance

CREATE TABLE IF NOT EXISTS dq_issues (
    id BIGSERIAL PRIMARY KEY,
    check_name TEXT NOT NULL,
    severity TEXT NOT NULL,         -- e.g., CRITICAL / HIGH / MEDIUM / LOW
    row_count BIGINT NOT NULL,
    sample_ids TEXT,                -- comma-separated sample ids or short JSON
    details TEXT,                   -- short message
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
CREATE INDEX idx_dq_issues_detected_at ON dq_issues(detected_at);

--A. Orders with invalid customer_id
SELECT COUNT(*) AS cnt,
       STRING_AGG(o.order_id::text, ',' ORDER BY o.order_id LIMIT 10) AS sample_ids
FROM vw_base_orders o
LEFT JOIN vw_base_customers c USING (customer_id)
WHERE c.customer_id IS NULL;

--B. order_items with invalid product_id
SELECT COUNT(*), STRING_AGG(order_item_id::text, ',' ORDER BY order_item_id LIMIT 10)
FROM vw_base_order_items oi
LEFT JOIN vw_base_products p USING (product_id)
WHERE p.product_id IS NULL;

--C. Reviews from customers who never bought the product
SELECT COUNT(*),
       STRING_AGG(CONCAT(r.product_id,'|',r.customer_id), ',' ORDER BY r.review_date LIMIT 10)
FROM vw_base_reviews r
LEFT JOIN vw_base_order_items oi
  ON r.product_id = oi.product_id
LEFT JOIN vw_base_orders o
  ON oi.order_id = o.order_id AND o.customer_id = r.customer_id
WHERE o.order_id IS NULL;

--D. Orders where final_total ≠ SUM(net_amount)
SELECT COUNT(*),
       STRING_AGG(o.order_id::text, ',' ORDER BY o.order_id LIMIT 10)
FROM vw_base_orders o
JOIN (
   SELECT order_id, SUM(net_amount) AS calc_total
   FROM vw_base_order_items
   GROUP BY order_id
) t ON t.order_id = o.order_id
WHERE COALESCE(o.final_total,0) <> ROUND(COALESCE(t.calc_total,0),2);

--E. Products where selling_price < unit_cost
SELECT COUNT(*), STRING_AGG(product_id::text, ',' ORDER BY product_id LIMIT 10)
FROM vw_base_products
WHERE selling_price < unit_cost;

--F. Promotions where times_used > max_uses
SELECT COUNT(*), STRING_AGG(promotion_id::text, ',' ORDER BY promotion_id LIMIT 10)
FROM promotions
WHERE times_used > max_uses;


--G. Duplicate customer emails
SELECT COUNT(*),
       STRING_AGG(email, ',' ORDER BY email LIMIT 10)
FROM (
    SELECT email, COUNT(*) AS c
    FROM customers
    GROUP BY email
    HAVING COUNT(*) > 1
) dup;


--H. Count NULLs in critical columns (example for orders)
SELECT
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS missing_customer_id,
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS missing_order_date,
    SUM(CASE WHEN final_total IS NULL THEN 1 ELSE 0 END) AS missing_final_total
FROM vw_base_orders;

--I. Products with no inventory record
SELECT COUNT(*), STRING_AGG(p.product_id::text, ',' ORDER BY p.product_id LIMIT 10)
FROM vw_base_products p
LEFT JOIN vw_base_inventory i USING (product_id)
WHERE i.product_id IS NULL;


--J. Customers with no orders (inactive customers)
SELECT COUNT(*), STRING_AGG(c.customer_id::text, ',' ORDER BY c.customer_id LIMIT 10)
FROM vw_base_customers c
LEFT JOIN vw_base_orders o USING (customer_id)
WHERE o.order_id IS NULL;


--K. Orders with future dates
SELECT COUNT(*), STRING_AGG(order_id::text, ',' ORDER BY order_date LIMIT 10)
FROM vw_base_orders
WHERE order_date > now()::date;


--L. Ratings outside 1-5
SELECT COUNT(*), STRING_AGG(CONCAT(review_id,'|',rating), ',' ORDER BY review_date LIMIT 10)
FROM customer_reviews
WHERE rating NOT BETWEEN 1 AND 5;


--M. Negative quantities or prices
SELECT COUNT(*), STRING_AGG(order_item_id::text, ',' ORDER BY order_item_id LIMIT 10)
FROM vw_base_order_items
WHERE quantity <= 0 OR unit_price < 0;

--N. Promotion usage referencing missing promotion
SELECT COUNT(*) AS row_count,
       STRING_AGG(pu.promotion_id::text, ',' ORDER BY pu.promotion_id LIMIT 10) AS sample_ids
FROM promotion_usage pu
LEFT JOIN promotions p USING (promotion_id)
WHERE p.promotion_id IS NULL;


--O. Orders with no order_items
SELECT COUNT(*) AS row_count,
       STRING_AGG(o.order_id::text, ',' ORDER BY o.order_id LIMIT 10) AS sample_ids
FROM vw_base_orders o
LEFT JOIN vw_base_order_items oi USING (order_id)
WHERE oi.order_id IS NULL;


--P. Discount percent outside 0–100
SELECT COUNT(*) AS row_count,
       STRING_AGG(order_item_id::text, ',' ORDER BY order_item_id LIMIT 10) AS sample_ids
FROM vw_base_order_items
WHERE discount_percent < 0
   OR discount_percent > 100;


--Q. Sold quantity exceeds inventory (historical check)
SELECT COUNT(*) AS row_count,
       STRING_AGG(p.product_id::text, ',' ORDER BY p.product_id LIMIT 10) AS sample_ids
FROM vw_base_products p
JOIN vw_base_inventory i USING (product_id)
JOIN vw_base_order_items oi USING (product_id)
GROUP BY p.product_id, i.stock_quantity
HAVING SUM(oi.quantity) > i.stock_quantity;


--R. Invalid email formats (not duplicates)
SELECT COUNT(*) AS row_count,
       STRING_AGG(customer_id::text, ',' ORDER BY customer_id LIMIT 10) AS sample_ids
FROM customers
WHERE email IS NOT NULL
  AND email !~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$';


-- Security



CREATE OR REPLACE FUNCTION public.run_data_quality_checks()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_cnt bigint;
    v_fix_cnt_totals bigint;
    v_fix_cnt_cust bigint;
    v_samples text;
BEGIN
    ------------------------------------------------------------------
    -- PHASE 1: ACTIVE DATA HEALING (Fixing issues before reporting)
    ------------------------------------------------------------------
    
    -- AUTO-HEAL 1: Ensure the Ghost Customer (-1) exists
    INSERT INTO customers (customer_id, email, first_name, last_name, signup_date, country, customer_tier)
    VALUES (-1, 'system.orphan@example.com', 'Unknown', 'Customer', CURRENT_DATE, 'Unknown', 'None')
    ON CONFLICT (customer_id) DO NOTHING;

    -- Reassign orphaned orders to the Ghost Customer
    WITH updated_orphans AS (
        UPDATE orders
        SET customer_id = -1
        WHERE customer_id IS NULL
        RETURNING order_id
    )
    SELECT count(*) INTO v_fix_cnt_cust FROM updated_orphans;

    -- Audit Log the fix
    IF v_fix_cnt_cust > 0 THEN
        INSERT INTO data_fix_audit (check_name, fixed_by, sql_executed, notes)
        VALUES (
            'orders_invalid_customer', 
            'SYSTEM_AUTO_HEAL', 
            'UPDATE orders SET customer_id = -1', 
            'Reassigned ' || v_fix_cnt_cust || ' orphaned orders to Ghost Customer (-1).'
        );
    END IF;

    -- AUTO-HEAL 2: Fix Broken Math (final_total)
    WITH updated_orders AS (
        UPDATE orders o
        SET final_total = sub.calculated_total
        FROM (
            SELECT order_id,
                   ROUND(SUM(quantity * unit_price * (1 - COALESCE(discount_percent,0)/100.0)), 2) AS calculated_total
            FROM order_items
            GROUP BY order_id
        ) AS sub
        WHERE o.order_id = sub.order_id 
        AND (o.final_total IS NULL OR COALESCE(o.final_total, 0) <> sub.calculated_total)
        RETURNING o.order_id
    )
    SELECT count(*) INTO v_fix_cnt_totals FROM updated_orders;

    -- Audit Log the fix
    IF v_fix_cnt_totals > 0 THEN
        INSERT INTO data_fix_audit (check_name, fixed_by, sql_executed, notes)
        VALUES (
            'orders_total_mismatch', 
            'SYSTEM_AUTO_HEAL', 
            'UPDATE orders SET final_total = sub.calculated_total', 
            'Auto-calculated and fixed ' || v_fix_cnt_totals || ' missing/incorrect order totals.'
        );
    END IF;

    ------------------------------------------------------------------
    -- PHASE 2: WIPE & REPLACE (Clean yesterday's dashboard)
    ------------------------------------------------------------------
    
    -- This empties the active issues table and resets the ID sequence to 1
    TRUNCATE TABLE dq_issues RESTART IDENTITY;

    ------------------------------------------------------------------
    -- PHASE 3: PASSIVE DATA QUALITY CHECKS (Safety Nets)
    ------------------------------------------------------------------

    -- A. Orders with invalid customer_id
    SELECT COUNT(*), array_to_string((array_agg(o.order_id ORDER BY o.order_id))[1:10], ',')
    INTO v_cnt, v_samples FROM vw_base_orders o
    LEFT JOIN vw_base_customers c USING (customer_id) WHERE c.customer_id IS NULL;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'orders_invalid_customer','HIGH',v_cnt,v_samples,'orders reference non-existing customers',DEFAULT);
        PERFORM pg_notify('dq_channel','orders_invalid_customer|'||v_cnt);
    END IF;

    -- B. Order items with invalid product_id
    SELECT COUNT(*), array_to_string((array_agg(order_item_id ORDER BY order_item_id))[1:10], ',')
    INTO v_cnt, v_samples FROM vw_base_order_items oi
    LEFT JOIN vw_base_products p USING (product_id) WHERE p.product_id IS NULL;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'order_items_invalid_product','HIGH',v_cnt,v_samples,'order_items reference non-existing products',DEFAULT);
        PERFORM pg_notify('dq_channel','order_items_invalid_product|'||v_cnt);
    END IF;

    -- C. Reviews without verified purchase
    SELECT COUNT(*), array_to_string((array_agg(CONCAT(r.product_id,'|',r.customer_id) ORDER BY r.review_date))[1:10], ',')
    INTO v_cnt, v_samples FROM vw_base_reviews r
    LEFT JOIN vw_base_order_items oi ON r.product_id = oi.product_id
    LEFT JOIN vw_base_orders o ON oi.order_id = o.order_id AND o.customer_id = r.customer_id
    WHERE o.order_id IS NULL;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'reviews_without_purchase','MEDIUM',v_cnt,v_samples,'reviews exist without matching customer purchase',DEFAULT);
        PERFORM pg_notify('dq_channel','reviews_without_purchase|'||v_cnt);
    END IF;

    -- D. Order total mismatch
    SELECT COUNT(*), array_to_string((array_agg(o.order_id ORDER BY o.order_id))[1:10], ',')
    INTO v_cnt, v_samples FROM vw_base_orders o
    JOIN (SELECT order_id, SUM(net_amount) AS calc_total FROM vw_base_order_items GROUP BY order_id) t ON t.order_id = o.order_id
    WHERE COALESCE(o.final_total,0) <> ROUND(COALESCE(t.calc_total,0),2);

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'orders_total_mismatch','CRITICAL',v_cnt,v_samples,'final_total differs from sum of order_items',DEFAULT);
        PERFORM pg_notify('dq_channel','orders_total_mismatch|'||v_cnt);
    END IF;

    -- E. Selling price below unit cost
    SELECT COUNT(*), array_to_string((array_agg(product_id ORDER BY product_id))[1:10], ',')
    INTO v_cnt, v_samples FROM vw_base_products WHERE selling_price < unit_cost;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'products_price_below_cost','HIGH',v_cnt,v_samples,'selling_price lower than unit_cost',DEFAULT);
        PERFORM pg_notify('dq_channel','products_price_below_cost|'||v_cnt);
    END IF;

    -- F. Promotions exceeding max usage
    SELECT COUNT(*), array_to_string((array_agg(promotion_id ORDER BY promotion_id))[1:10], ',')
    INTO v_cnt, v_samples FROM promotions WHERE times_used > max_uses;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'promotion_overuse','HIGH',v_cnt,v_samples,'promotion usage exceeded max_uses',DEFAULT);
        PERFORM pg_notify('dq_channel','promotion_overuse|'||v_cnt);
    END IF;

    -- G. Duplicate customer emails
    SELECT COUNT(*), array_to_string((array_agg(email ORDER BY email))[1:10], ',')
    INTO v_cnt, v_samples FROM (SELECT email FROM customers GROUP BY email HAVING COUNT(*) > 1) d;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'duplicate_customer_email','MEDIUM',v_cnt,v_samples,'duplicate customer emails detected',DEFAULT);
        PERFORM pg_notify('dq_channel','duplicate_customer_email|'||v_cnt);
    END IF;

    -- H. Orders with missing critical fields
    SELECT COUNT(*), array_to_string((array_agg(order_id ORDER BY order_id))[1:10], ',')
    INTO v_cnt, v_samples FROM vw_base_orders WHERE customer_id IS NULL OR order_date IS NULL OR final_total IS NULL;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'orders_missing_critical_fields','CRITICAL',v_cnt,v_samples,'orders contain NULLs in critical columns',DEFAULT);
        PERFORM pg_notify('dq_channel','orders_missing_critical_fields|'||v_cnt);
    END IF;

    -- I. Products without inventory record
    SELECT COUNT(*), array_to_string((array_agg(p.product_id ORDER BY p.product_id))[1:10], ',')
    INTO v_cnt, v_samples FROM vw_base_products p LEFT JOIN vw_base_inventory i USING (product_id) WHERE i.product_id IS NULL;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'products_missing_inventory','HIGH',v_cnt,v_samples,'products have no inventory record',DEFAULT);
        PERFORM pg_notify('dq_channel','products_missing_inventory|'||v_cnt);
    END IF;

    -- J. Customers with no orders
    SELECT COUNT(*), array_to_string((array_agg(c.customer_id ORDER BY c.customer_id))[1:10], ',')
    INTO v_cnt, v_samples FROM vw_base_customers c LEFT JOIN vw_base_orders o USING (customer_id) WHERE o.order_id IS NULL;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'inactive_customers','LOW',v_cnt,v_samples,'customers without any orders',DEFAULT);
        PERFORM pg_notify('dq_channel','inactive_customers|'||v_cnt);
    END IF;

    -- K. Orders with future dates
    SELECT COUNT(*), array_to_string((array_agg(order_id ORDER BY order_date))[1:10], ',')
    INTO v_cnt, v_samples FROM vw_base_orders WHERE order_date > CURRENT_DATE;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'orders_future_date','HIGH',v_cnt,v_samples,'orders dated in the future',DEFAULT);
        PERFORM pg_notify('dq_channel','orders_future_date|'||v_cnt);
    END IF;

    -- L. Ratings outside 1–5
    SELECT COUNT(*), array_to_string((array_agg(review_id ORDER BY review_date))[1:10], ',')
    INTO v_cnt, v_samples FROM customer_reviews WHERE rating NOT BETWEEN 1 AND 5;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'invalid_review_rating','MEDIUM',v_cnt,v_samples,'ratings outside valid range 1–5',DEFAULT);
        PERFORM pg_notify('dq_channel','invalid_review_rating|'||v_cnt);
    END IF;

    -- M. Negative quantities or prices
    SELECT COUNT(*), array_to_string((array_agg(order_item_id ORDER BY order_item_id))[1:10], ',')
    INTO v_cnt, v_samples FROM vw_base_order_items WHERE quantity <= 0 OR unit_price < 0;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'negative_quantity_or_price','CRITICAL',v_cnt,v_samples,'negative or zero quantities/prices detected',DEFAULT);
        PERFORM pg_notify('dq_channel','negative_quantity_or_price|'||v_cnt);
    END IF;

    -- N. Promotion usage invalid
    SELECT COUNT(*), array_to_string((array_agg(pu.promotion_id ORDER BY pu.promotion_id))[1:10], ',')
    INTO v_cnt, v_samples FROM promotion_usage pu LEFT JOIN promotions p USING (promotion_id) WHERE p.promotion_id IS NULL;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'promotion_usage_invalid','HIGH',v_cnt,v_samples,'promotion_usage references missing promotions',DEFAULT);
        PERFORM pg_notify('dq_channel','promotion_usage_invalid|'||v_cnt);
    END IF;

    -- O. Orders without order_items
    SELECT COUNT(*), array_to_string((array_agg(o.order_id ORDER BY o.order_id))[1:10], ',')
    INTO v_cnt, v_samples FROM vw_base_orders o LEFT JOIN vw_base_order_items oi USING (order_id) WHERE oi.order_id IS NULL;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'orders_without_items','CRITICAL',v_cnt,v_samples,'orders have no order_items',DEFAULT);
        PERFORM pg_notify('dq_channel','orders_without_items|'||v_cnt);
    END IF;

    -- P. Discount percent outside 0–100
    SELECT COUNT(*), array_to_string((array_agg(order_item_id ORDER BY order_item_id))[1:10], ',')
    INTO v_cnt, v_samples FROM vw_base_order_items WHERE discount_percent < 0 OR discount_percent > 100;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'invalid_discount_percent','HIGH',v_cnt,v_samples,'discount_percent outside 0–100 range',DEFAULT);
        PERFORM pg_notify('dq_channel','invalid_discount_percent|'||v_cnt);
    END IF;

    -- Q. Negative Inventory Levels
    SELECT COUNT(*), array_to_string((array_agg(product_id ORDER BY product_id))[1:10], ',')
    INTO v_cnt, v_samples FROM vw_base_inventory WHERE current_stock < 0;

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'negative_inventory','HIGH',v_cnt,v_samples,'warehouse inventory count is negative',DEFAULT);
        PERFORM pg_notify('dq_channel','negative_inventory|'||v_cnt);
    END IF;

    -- R. Invalid email formats
    SELECT COUNT(*), array_to_string((array_agg(customer_id ORDER BY customer_id))[1:10], ',')
    INTO v_cnt, v_samples FROM customers WHERE email IS NOT NULL AND email !~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$';

    IF v_cnt > 0 THEN
        INSERT INTO dq_issues VALUES (DEFAULT,'invalid_email_format','LOW',v_cnt,v_samples,'customers have invalid email format',DEFAULT);
        PERFORM pg_notify('dq_channel','invalid_email_format|'||v_cnt);
    END IF;

END;
$$;

TRUNCATE TABLE dq_issues RESTART IDENTITY;
-- 1. Table to store errors (The "Bad News" List)
CREATE TABLE IF NOT EXISTS dq_issues (
    id serial PRIMARY KEY,
    check_name text NOT NULL,
    severity text NOT NULL,        -- HIGH, MEDIUM, LOW, CRITICAL
    row_count bigint NOT NULL,     -- How many bad rows found
    sample_ids text,               -- IDs of a few bad rows (for you to check)
    details text,                  -- Description of the error
    detected_at timestamptz DEFAULT now()
);
INSERT INTO dq_issues (id, check_name, severity, row_count, sample_ids, details, detected_at)
VALUES (19, 'test_check_ai2', 'High', 0, Null, 'test_failure_inserted_manually', NOW());

-- 2. Index for faster searching
CREATE INDEX IF NOT EXISTS idx_dq_detected_at ON dq_issues(detected_at);

-- 3. Table to track your Manual Fixes (The "Audit Trail")
CREATE TABLE IF NOT EXISTS data_fix_audit  (
    audit_id BIGSERIAL PRIMARY KEY,
	check_name text NOT NULL,
    fix_date TIMESTAMPTZ DEFAULT now(),
    fixed_by TEXT,
    sql_executed TEXT,
    notes TEXT
);
INSERT INTO data_fix_audit (
    fixed_by,
	check_name,
    sql_executed,
    notes
)
VALUES (
    'Sabir',
	'orders_total_mismatch',
    'Yes',
    'Done'
);
SELECT * FROM data_fix_audit;
SELECT * FROM dq_issues;

SELECT public.run_data_quality_checks();


SELECT cron.unschedule('dq_test'); 

-- 2. Schedule for 6:25 PM IST (12:55 UTC)
-- Format: 'minute hour day month weekday' -> '55 12 * * *'
SELECT cron.schedule(
    'dq_test',                                 
    '55 12 * * *',                               
    $$SELECT public.run_data_quality_checks();$$ 
);


SELECT now();
SELECT * FROM cron.job;


SELECT * FROM cron.job;



SELECT * FROM dq_issues
ORDER BY detected_at ASC;


DO $$
BEGIN
    -- 1. Create "CHAMPIONS" & "LOYAL" (High Frequency, High Recency)
    -- Concentrates 1,000 orders onto just 150 customers in February 2026.
    -- Fixes: Repeat Purchase % spikes, Active Customers spikes, RFM shows "Champions"
    UPDATE orders 
    SET 
        customer_id = floor(random() * 150 + 1), 
        order_date = '2026-02-01'::date + (random() * 26)::int 
    WHERE order_id BETWEEN 1 AND 1000;

    -- 2. Create "RECENT / NEW CUSTOMERS" (Good Recency, mixed frequency)
    -- Forces 1,500 orders into Jan and Feb 2026 across 300 different customers.
    -- Fixes: Ensures a massive chunk of "Active Customers" for your reporting month.
    UPDATE orders 
    SET 
        customer_id = floor(random() * 300 + 151),
        order_date = '2026-01-15'::date + (random() * 40)::int 
    WHERE order_id BETWEEN 1001 AND 2500;

    -- 3. Create "AT RISK" (Good Frequency, Poor Recency)
    -- Concentrates 1,000 orders onto 200 customers, but dates them back to Fall 2025.
    -- Fixes: Populates the "At Risk" and "Needs Attention" buckets in your RFM chart.
    UPDATE orders 
    SET 
        customer_id = floor(random() * 200 + 451),
        order_date = '2025-09-01'::date + (random() * 60)::int 
    WHERE order_id BETWEEN 2501 AND 3500;
    
    -- Note: Orders 3501 to 5000 remain untouched and spread out to act as your "Lost" cohort.
END $$;