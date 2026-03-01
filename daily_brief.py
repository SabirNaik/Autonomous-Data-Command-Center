import psycopg2
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# --- CONFIGURATION ---
DB_CONFIG = {
    "dbname": "analytics",
    "user": "admin",
    "password": "<YOUR_DB_PASSWORD>",
    "host": "localhost",
    "port": "5432"
}

EMAIL_CONFIG = {
    "sender": "your_email@gmail.com",
    "password": "<YOUR_GMAIL_APP_PASSWORD>", 
    "receiver": "your_email@gmail.com"
}

def get_executive_metrics():
    print("🔌 Extracting ALL 28 Analytics Modules...")
    m = {}
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # [Global Date]
        cursor.execute("SELECT MAX(order_date) FROM orders")
        m['report_date'] = cursor.fetchone()[0]

        # ---------------------------------------------------------
        # POD 1: TOPLINE, TRENDS & TIME (Q1, 3, 16, 17, 25, 28)
        # ---------------------------------------------------------
        cursor.execute("""
            WITH md AS (SELECT MAX(order_date) as d FROM orders),
                 t_ords AS (SELECT SUM(total_amount) as r, COUNT(o.order_id) as o FROM orders o CROSS JOIN md WHERE o.order_date = d),
                 t_items AS (SELECT SUM(oi.quantity) as q FROM order_items oi JOIN orders o ON oi.order_id = o.order_id CROSS JOIN md WHERE o.order_date = d),
                 y AS (SELECT SUM(total_amount) as r FROM orders o CROSS JOIN md WHERE o.order_date = d - INTERVAL '1 day')
            SELECT COALESCE(t_ords.r,0), COALESCE(y.r,0), COALESCE(t_ords.o,0), COALESCE(t_items.q,0) FROM t_ords CROSS JOIN y CROSS JOIN t_items;
        """)
        res = cursor.fetchone()
        m['rev_today'], m['rev_yest'], m['orders_today'], q_today = res
        m['rev_change'] = ((m['rev_today'] - m['rev_yest']) / m['rev_yest'] * 100) if m['rev_yest'] > 0 else 0
        m['avg_items'] = round(q_today / m['orders_today'], 1) if m['orders_today'] > 0 else 0
        m['aov'] = round(m['rev_today'] / m['orders_today'], 0) if m['orders_today'] > 0 else 0

        # Q25 (MoM Growth), Q28 (Seasonality), Q16 (Status)
        cursor.execute("SELECT ROUND(((SUM(CASE WHEN DATE_TRUNC('month', order_date) = DATE_TRUNC('month', CURRENT_DATE) THEN total_amount ELSE 0 END) - SUM(CASE WHEN DATE_TRUNC('month', order_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') THEN total_amount ELSE 0 END)) / NULLIF(SUM(CASE WHEN DATE_TRUNC('month', order_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') THEN total_amount ELSE 0 END), 0)) * 100, 1) FROM orders;")
        m['mom_growth'] = cursor.fetchone()[0] or 0
        cursor.execute("SELECT TO_CHAR(order_date, 'Day'), COUNT(order_id) FROM orders GROUP BY TO_CHAR(order_date, 'Day') ORDER BY COUNT(order_id) DESC LIMIT 1;")
        bd = cursor.fetchone()
        m['best_day'] = bd[0].strip() if bd else "N/A"
        cursor.execute("SELECT order_status, COUNT(order_id) FROM orders GROUP BY order_status ORDER BY COUNT(order_id) DESC LIMIT 1;")
        st = cursor.fetchone()
        m['top_status'] = f"{st[0]} ({st[1]})" if st else "N/A"
        
        # Q3 (Category Rev)
        cursor.execute("SELECT p.category, SUM(oi.quantity * oi.unit_price) as rev FROM order_items oi JOIN products p ON oi.product_id=p.product_id GROUP BY p.category ORDER BY rev DESC LIMIT 1;")
        cat = cursor.fetchone()
        m['top_cat_rev'] = f"{cat[0]} (₹{cat[1]:,.0f})" if cat else "N/A"

        # ---------------------------------------------------------
        # POD 2: CUSTOMERS & COHORTS (Q4, 5, 6, 7, 8, 9, 10, 21, 24, 26)
        # ---------------------------------------------------------
        # Q24 (RFM), Q6 (LTV)
        cursor.execute("""
            WITH rfm AS (SELECT customer_id, (SELECT MAX(order_date) FROM orders) - MAX(order_date) AS recency, COUNT(order_id) AS freq, SUM(total_amount) as ltv FROM orders GROUP BY customer_id)
            SELECT COUNT(*) FILTER(WHERE recency <= 15 AND freq >= 2), COUNT(*) FILTER(WHERE recency > 30 AND freq >= 1), ROUND(AVG(ltv),0) FROM rfm;
        """)
        rfm = cursor.fetchone()
        m['champs'], m['at_risk'], m['ltv'] = rfm[0] or 0, rfm[1] or 0, rfm[2] or 0

        # Q7 (Repeat), Q9 (Churn), Q10 (Tier), Q5 (Acquisition)
        cursor.execute("SELECT ROUND(COUNT(*) FILTER (WHERE (SELECT COUNT(*) FROM orders WHERE customer_id = c.customer_id) >= 2)::numeric / NULLIF(COUNT(*), 0) * 100, 1) FROM customers c;")
        m['repeat'] = cursor.fetchone()[0] or 0
        cursor.execute("SELECT COUNT(*) FROM (SELECT customer_id, MAX(order_date) as ld FROM orders GROUP BY customer_id) s WHERE (SELECT MAX(order_date) FROM orders) - ld > 90;")
        m['churn90'] = cursor.fetchone()[0] or 0
        cursor.execute("SELECT customer_tier, COUNT(*) FROM customers WHERE customer_tier IS NOT NULL GROUP BY customer_tier ORDER BY COUNT(*) DESC LIMIT 1;")
        tier = cursor.fetchone()
        m['tier'] = f"{tier[0]} ({tier[1]})" if tier else "N/A"
        cursor.execute("SELECT COUNT(*) FROM customers WHERE signup_date >= DATE_TRUNC('month', CURRENT_DATE);")
        m['new_cust'] = cursor.fetchone()[0] or 0

        # Q4 (Country AOV), Q8 (Days to 2nd Order), Q21 (Cohort), Q26 (Retention Curve)
        cursor.execute("SELECT c.country, ROUND(AVG(o.total_amount),0) FROM orders o JOIN customers c ON o.customer_id=c.customer_id GROUP BY c.country ORDER BY 2 DESC LIMIT 1;")
        c_aov = cursor.fetchone()
        m['country_aov'] = f"{c_aov[0]} (₹{c_aov[1]:,.0f})" if c_aov else "N/A"
        
        cursor.execute("WITH rn AS (SELECT customer_id, order_date, ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_date) as r FROM orders) SELECT ROUND(AVG(r2.order_date - r1.order_date),1) FROM rn r1 JOIN rn r2 ON r1.customer_id=r2.customer_id AND r1.r=1 AND r2.r=2;")
        days2 = cursor.fetchone()
        m['days_to_2nd'] = days2[0] if days2 and days2[0] else "N/A"

        cursor.execute("SELECT ROUND(COUNT(DISTINCT customer_id) FILTER(WHERE order_date >= CURRENT_DATE - 30)::numeric / NULLIF(COUNT(DISTINCT customer_id),0)*100,1) FROM orders;")
        m['cohort_active'] = cursor.fetchone()[0] or 0

        cursor.execute("WITH rn AS (SELECT customer_id, order_date, ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY order_date) as r FROM orders) SELECT COUNT(*) FROM rn r1 JOIN rn r2 ON r1.customer_id=r2.customer_id AND r1.r=1 AND r2.r=2 WHERE r2.order_date - r1.order_date <= 30;")
        m['retention_30d'] = cursor.fetchone()[0] or 0

        # ---------------------------------------------------------
        # POD 3: PRODUCTS & INVENTORY (Q2, 11-15, 22, 27)
        # ---------------------------------------------------------
        # Q2 (Top Product), Q14 (Margin)
        cursor.execute("SELECT p.product_name, SUM(oi.quantity * oi.unit_price * (1 - COALESCE(oi.discount_percent,0)/100.0)) as rev FROM order_items oi JOIN products p ON oi.product_id = p.product_id GROUP BY p.product_name ORDER BY rev DESC LIMIT 1;")
        tp = cursor.fetchone()
        m['top_prod'] = f"{tp[0]} (₹{tp[1]:,.0f})" if tp else "N/A"
        
        cursor.execute("SELECT category, ROUND(((AVG(selling_price) - AVG(unit_cost)) / NULLIF(AVG(selling_price), 0)) * 100, 1) as m FROM products GROUP BY category ORDER BY m DESC LIMIT 1;")
        mar = cursor.fetchone()
        m['margin'] = f"{mar[0]} ({mar[1]}%)" if mar else "N/A"

        # Q11 (Low Stock), Q12 (Rating), Q13 (Dead Stock)
        cursor.execute("SELECT p.product_name, SUM(i.stock_quantity) FROM inventory i JOIN products p ON i.product_id = p.product_id GROUP BY p.product_name HAVING SUM(i.stock_quantity) < 20 LIMIT 3;")
        ls = cursor.fetchall()
        m['low_stock'] = ", ".join([f"{r[0]} ({r[1]})" for r in ls]) if ls else "None"
        
        cursor.execute("SELECT ROUND(AVG(rating), 1) FROM customer_reviews;")
        m['rating'] = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT COUNT(DISTINCT p.product_id) FROM products p LEFT JOIN order_items oi ON p.product_id=oi.product_id LEFT JOIN orders o ON oi.order_id=o.order_id WHERE o.order_date < CURRENT_DATE - INTERVAL '30 days' OR o.order_date IS NULL;")
        m['dead_stock'] = cursor.fetchone()[0] or 0

        # Q15 (Top Cat Mover), Q22 (Hot Products), Q27 (Lifecycle)
        cursor.execute("WITH ps AS (SELECT p.category, p.product_name, SUM(oi.quantity) as q FROM order_items oi JOIN products p ON oi.product_id=p.product_id GROUP BY p.category, p.product_name), rk AS (SELECT category, product_name, q, ROW_NUMBER() OVER(PARTITION BY category ORDER BY q DESC) as rn FROM ps) SELECT category, product_name FROM rk WHERE rn=1 LIMIT 1;")
        tcm = cursor.fetchone()
        m['cat_mover'] = f"{tcm[1]} ({tcm[0]})" if tcm else "N/A"
        
        cursor.execute("SELECT COUNT(*) FROM (SELECT product_id, SUM(quantity) as q FROM order_items GROUP BY product_id HAVING SUM(quantity)>=50) s;")
        m['hot_prods'] = cursor.fetchone()[0] or 0

        cursor.execute("SELECT COUNT(*) FROM products WHERE launch_date >= CURRENT_DATE - 90;")
        m['new_launches'] = cursor.fetchone()[0] or 0

        # ---------------------------------------------------------
        # POD 4: PROMOS, PAYMENTS & OPS (Q18-20, 23)
        # ---------------------------------------------------------
        # Q23 (Promo ROI), Q20 (Avg Discount), Q18 (Payment), Q19 (Promo Penetration)
        cursor.execute("""
            WITH rev AS (SELECT SUM(oi.quantity * oi.unit_price * (1 - COALESCE(oi.discount_percent,0)/100.0)) as r FROM promotion_usage pu JOIN order_items oi ON pu.order_id = oi.order_id),
                 disc AS (SELECT SUM(oi.quantity * oi.unit_price * (COALESCE(oi.discount_percent,0)/100.0)) as d FROM promotion_usage pu JOIN order_items oi ON pu.order_id = oi.order_id)
            SELECT ROUND(((rev.r - disc.d) / NULLIF(disc.d, 0)) * 100, 1) FROM rev CROSS JOIN disc;
        """)
        promo = cursor.fetchone()
        m['promo_roi'] = f"{promo[0]}%" if promo and promo[0] else "0%"
        
        cursor.execute("SELECT COALESCE(ROUND(AVG(discount_percent), 1), 0) FROM order_items WHERE discount_percent > 0;")
        m['avg_disc'] = f"{cursor.fetchone()[0]}%"

        cursor.execute("SELECT payment_method, COUNT(order_id) FROM orders GROUP BY payment_method ORDER BY COUNT(order_id) DESC LIMIT 1;")
        pay = cursor.fetchone()
        m['payment'] = pay[0] if pay else "N/A"

        cursor.execute("SELECT ROUND(COUNT(DISTINCT pu.order_id)::numeric / NULLIF(COUNT(DISTINCT o.order_id),0)*100,1) FROM orders o LEFT JOIN promotion_usage pu ON o.order_id=pu.order_id;")
        m['promo_pen'] = f"{cursor.fetchone()[0]}%"

        conn.close()
        return m
    except Exception as e:
        print(f"❌ DB Error: {e}")
        return None

def generate_ceo_email(data):
    print("🧠 Synthesizing Command Center... Engaging AI for Deep Strategy...")
    
    # Removed all Query references for a cleaner executive view
    html_dashboard = f"""
    <h2>🏢 Enterprise Analytics Command Center</h2>
    <p><b>Date:</b> {data['report_date']}</p>
    <hr>

    <h3>📈 1. Topline, Trends & Time</h3>
    <ul>
        <li><b>Daily Revenue:</b> ₹{data['rev_today']:,.0f} (Δ {data['rev_change']:+.2f}%)</li>
        <li><b>MoM Growth Rate:</b> {data['mom_growth']:+.2f}%</li>
        <li><b>Order Volume:</b> {data['orders_today']} <i>(Most common status: {data['top_status']})</i></li>
        <li><b>AOV & Density:</b> ₹{data['aov']:,.0f} | {data['avg_items']} items/order</li>
        <li><b>Top Category Contribution:</b> {data['top_cat_rev']}</li>
        <li><b>Seasonal Peak:</b> {data['best_day']} is historically the strongest day</li>
    </ul>

    <h3>👥 2. Customer Intel & Cohorts</h3>
    <ul>
        <li><b>RFM Segmentation:</b> {data['champs']} Champions | {data['at_risk']} At Risk</li>
        <li><b>Acquisition & Churn:</b> {data['new_cust']} MTD Signups | {data['churn90']} Churned (>90d)</li>
        <li><b>Customer Lifetime Value (LTV):</b> ₹{data['ltv']:,.0f} avg.</li>
        <li><b>Retention Dynamics:</b> {data['repeat']}% Repeat Rate</li>
        <li><b>Top Country by AOV:</b> {data['country_aov']}</li>
        <li><b>Time Between Orders:</b> {data['days_to_2nd']} days avg. to 2nd purchase</li>
        <li><b>Cohort Health:</b> {data['cohort_active']}% active in last 30d</li>
        <li><b>Retention Curve:</b> {data['retention_30d']} users repurchased within 30 days</li>
        <li><b>Tier Distribution:</b> Majority segment is {data['tier']}</li>
    </ul>

    <h3>🛍️ 3. Product & Inventory Health</h3>
    <ul>
        <li><b>Top Product Revenue:</b> {data['top_prod']}</li>
        <li><b>Best in Category:</b> {data['cat_mover']}</li>
        <li><b>Peak Category Margin:</b> {data['margin']}</li>
        <li><b>Quality Sentiment:</b> {data['rating']} ⭐ average rating</li>
        <li><b>Product Status:</b> {data['hot_prods']} 'Hot' items (>50 sold)</li>
        <li><b>Lifecycle Trends:</b> {data['new_launches']} products in launch phase (<90d)</li>
        <li><b>⚠️ Critical Low Stock (<20):</b> {data['low_stock']}</li>
        <li><b>Dead Stock Alert:</b> {data['dead_stock']} products with 0 sales in 30 days</li>
    </ul>

    <h3>🎯 4. Marketing, Promos & Ops</h3>
    <ul>
        <li><b>Promo Effectiveness (ROI):</b> {data['promo_roi']} <i>(Avg discount: {data['avg_disc']})</i></li>
        <li><b>Promo Penetration:</b> {data['promo_pen']} of all orders used a code</li>
        <li><b>Dominant Payment Route:</b> {data['payment']}</li>
    </ul>
    <hr>
    """

    # UPGRADED AI PROMPT: Added {:.2f} to rev_change to force rounding to 2 decimals
    ai_context = f"Rev Change: {data['rev_change']:.2f}%. AOV: {data['aov']}. At Risk Customers: {data['at_risk']}. Low Stock: {data['low_stock']}. Promo ROI: {data['promo_roi']}."
    
    prompt = (
        "You are an elite Chief Data Officer providing strategic counsel to the CEO.\n"
        f"Context: {ai_context}\n\n"
        "Write ONLY the 'AI Strategic Synthesis' section using basic HTML (<h3>, <p>, <ul>, <li>). NO MARKDOWN BLOCKS.\n"
        "Do NOT just repeat the numbers. Analyze the root cause of the data, connect the dots, and provide highly specific, actionable business suggestions.\n"
        "Use exactly this format:\n\n"
        "<h3>🧠 5. AI Strategic Synthesis</h3>\n"
        "<b>📊 Diagnostic Readout:</b>\n"
        "<ul>\n"
        "<li>📉 <b>Revenue & Cart Dynamics:</b> [Analyze the revenue change vs AOV. What is the business impact?]</li>\n"
        "<li>⚠️ <b>Churn Gravity:</b> [Analyze the 'At Risk' users. Why might they be leaving?]</li>\n"
        "<li>🚀 <b>Promo Multiplier:</b> [Analyze the Promo ROI. Are we relying too heavily on discounts?]</li>\n"
        "</ul>\n"
        "<b>🎯 Priority Directives & Growth Suggestions:</b>\n"
        "<ul>\n"
        "<li>⚡ <b>Immediate (Supply Chain):</b> [Suggest a specific action for the low stock items, like pausing ads for those SKUs or emergency POs]</li>\n"
        "<li>🛡️ <b>Within 48h (Retention):</b> [Suggest a specific win-back campaign idea for the at-risk customers]</li>\n"
        "<li>💡 <b>Strategic Suggestion (Growth):</b> [Provide one out-of-the-box idea (e.g., A/B test, bundling, pricing tweak) to fix the revenue drop or boost AOV]</li>\n"
        "</ul>"
    )

    try:
        url = "http://localhost:11434/api/generate"
        response = requests.post(url, json={"model": "qwen2.5-coder", "prompt": prompt, "stream": False, "options": {"temperature": 0.4}})
        ai_insights = response.json()['response'].replace('```html', '').replace('```', '').strip()
        
        signature = """
        <br><br>
        <hr>
        <p>Best regards,<br>
        <b>Sabir Naik</b><br>
        Data & Analytics<br>
        sabirnaik537@gmail.com</p>
        """
        return html_dashboard + "\n" + ai_insights + signature
    except Exception as e:
        return html_dashboard + f"<p>⚠️ AI Engine Offline: {e}</p>"

def send_email(html_body, data):
    print("📧 Dispatching 28-Query Command Center Brief...")
    msg = MIMEMultipart()
    status = "Yield Positive" if data['rev_change'] >= 0 else "Contraction"
    msg['Subject'] = f"🚀 Enterprise Command Center — {data['report_date']} | Rev: ₹{data['rev_today']:,.0f} ({status})"
    msg['From'] = EMAIL_CONFIG["sender"]
    msg['To'] = EMAIL_CONFIG["receiver"]

    full_html = f"<html><body>{html_body}</body></html>"
    msg.attach(MIMEText(full_html, 'html'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_CONFIG["sender"], EMAIL_CONFIG["password"])
            server.send_message(msg)
        print("✅ 28-Query Pipeline Successfully Executed!")
    except Exception as e:
        print(f"❌ Dispatch Error: {e}")

if __name__ == "__main__":
    metrics = get_executive_metrics()
    if metrics:
        final_text = generate_ceo_email(metrics)
        send_email(final_text, metrics)