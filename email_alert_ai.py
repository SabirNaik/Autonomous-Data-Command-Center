import smtplib
import psycopg2
import requests
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date

# --- 1. CONFIGURATION ---
DB_CONFIG = {
    "dbname": "analytics",
    "user": "admin",
    "password": "<YOUR_DB_PASSWORD>",  
    "host": "localhost",
    "port": "5432"
}

EMAIL_CONFIG = {
    "sender": "your_email@gmail.com",
    "password": "<YOUR_GMAIL_APP_PASSWORD>", # App Password
    "receiver": "your_email@gmail.com"
}

# --- 2. AI FUNCTION (SMART & SIMPLE) ---
def ask_local_ai(check_name, error_message):
    """Sends the error to local Ollama with 'Smart & Simple' instructions."""
    try:
        url = "http://localhost:11434/api/generate"
        
        # --- THE SMART PROMPT ---
        system_instruction = (
            "You are a helpful Data Engineer. Analyze the error below for the check '" + check_name + "'.\n"
            "1. LANGUAGE: Use simple, plain English. No corporate jargon.\n"
            "2. MANUAL TESTS: If the error mentions 'manual', 'test', or 'simulation', explicitly say this is just a drill.\n"
            "3. SQL FIX: Write a specific SQL query. Do NOT use 'table_name'. Guess the table name based on the check name (e.g., if check is 'orders_mismatch', use 'orders' table).\n"
            "4. FORMAT: Use these exact HTML tags:\n"
            "<b>Problem:</b> [What went wrong in simple terms]<br>"
            "<b>Action:</b> [What I should do next]<br>"
            "<b>SQL Fix:</b> <code>[A smart SQL query to investigate or fix it]</code><br>"
            "<b>Status:</b> [Simulated / Critical / Warning]"
        )
        
        prompt = f"{system_instruction}\n\nError Message: '{error_message}'"
        
        payload = {
            "model": "qwen2.5-coder",
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.2} # Low temp = smarter, less random
        }
        
        response = requests.post(url, json=payload)
        return response.json()['response']
    except Exception as e:
        return f"<b>AI Offline:</b> {e}"

# --- 3. HELPER: GET OUTSTANDING ISSUES ---
def get_outstanding_issues(cursor):
    """Fetches issues from last 30 days, excluding fixed ones."""
    query = """
        WITH RecentIssues AS (
            SELECT check_name, severity, details, MAX(detected_at) as last_seen
            FROM dq_issues
            WHERE detected_at >= CURRENT_DATE - INTERVAL '30 days'
            AND detected_at < CURRENT_DATE
            GROUP BY check_name, severity, details
        )
        SELECT r.check_name, r.severity, r.details, r.last_seen::date
        FROM RecentIssues r
        LEFT JOIN data_fix_audit f 
            ON r.check_name = f.check_name 
            AND f.fix_date >= r.last_seen
        WHERE f.audit_id IS NULL
        ORDER BY r.last_seen DESC
        LIMIT 10;
    """
    cursor.execute(query)
    return cursor.fetchall()

# --- 4. MAIN LOGIC ---
def check_and_alert():
    conn = None
    try:
        print("🔌 Connecting to Database...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 1. Get TODAY'S Failures
        query_today = """
            SELECT check_name, severity, row_count, sample_ids, details, detected_at
            FROM dq_issues 
            WHERE severity ILIKE 'high'
            AND detected_at::date = CURRENT_DATE;
        """
        cursor.execute(query_today)
        failures_today = cursor.fetchall()

        if failures_today:
            print(f"🚨 Found {len(failures_today)} issues today. Asking AI & Checking Audit Logs...")
            
            # 2. Get Outstanding History
            outstanding_history = get_outstanding_issues(cursor)

            # --- START HTML EMAIL ---
            html_content = f"""
            <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <h2 style="color: #d9534f;">🚨 Data Quality Alert: {date.today()}</h2>
                <p>The following <strong>Critical</strong> checks failed today.</p>
                <hr>
            """

            # LOOP TODAY'S FAILURES
            for issue in failures_today:
                check_name, severity, row_count, sample_ids, details, detected_at = issue
                
                # Context for AI
                fake_error_context = f"Check '{check_name}' failed. Details: {details}"
                
                # Pass check_name explicitly so AI can be smarter
                ai_advice = ask_local_ai(check_name, fake_error_context)
                
                html_content += f"""
                <div style="border: 1px solid #ddd; padding: 15px; margin-bottom: 20px; border-radius: 5px; box-shadow: 2px 2px 10px rgba(0,0,0,0.05);">
                    <h3 style="margin-top: 0; color: #2c3e50;">❌ {check_name}</h3>
                    <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                        <tr><td style="font-weight: bold; width: 120px;">Severity:</td><td style="color: red; font-weight: bold;">{severity}</td></tr>
                        <tr><td style="font-weight: bold;">Time:</td><td>{detected_at}</td></tr>
                        <tr><td style="font-weight: bold;">Details:</td><td>{details}</td></tr>
                    </table>
                    
                    <div style="background-color: #e8f4f8; border-left: 5px solid #2980b9; padding: 10px; margin-top: 15px;">
                        {ai_advice}
                    </div>
                </div>
                """

            # ADD HISTORY SECTION
            if outstanding_history:
                html_content += """
                <br>
                <h3 style="color: #666; border-bottom: 2px solid #ddd; padding-bottom: 5px;">📜 Outstanding Unresolved Issues</h3>
                <p style="font-size: 12px; color: #777;">Issues from last 30 days that <strong>have not been fixed</strong> in <code>data_fix_audit</code>.</p>
                <table style="width: 100%; border-collapse: collapse; border: 1px solid #ddd; font-size: 13px;">
                    <tr style="background-color: #f8f9fa;">
                        <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Check Name</th>
                        <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Severity</th>
                        <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Details</th>
                        <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Last Seen</th>
                    </tr>
                """
                for hist in outstanding_history:
                    h_name, h_sev, h_details, h_date = hist
                    sev_style = "color: red; font-weight: bold;" if str(h_sev).lower() == 'high' else "color: orange;" if str(h_sev).lower() == 'medium' else "color: green;"
                    html_content += f"""
                    <tr>
                        <td style="padding: 8px; border: 1px solid #ddd;">{h_name}</td>
                        <td style="padding: 8px; border: 1px solid #ddd; {sev_style}">{h_sev}</td>
                        <td style="padding: 8px; border: 1px solid #ddd;">{h_details}</td>
                        <td style="padding: 8px; border: 1px solid #ddd;">{h_date}</td>
                    </tr>
                    """
                html_content += "</table>"
            else:
                html_content += "<p><em>✅ All past issues have been resolved!</em></p>"
            
            html_content += """
                <p style="font-size: 11px; color: #999; margin-top: 30px; text-align: center;">
                    Generated by AI Data Pipeline
                </p>
            </body>
            </html>
            """

            msg = MIMEMultipart()
            msg['Subject'] = f"🚨 DQ Alert ({len(failures_today)} Critical) - {date.today()}"
            msg['From'] = EMAIL_CONFIG["sender"]
            msg['To'] = EMAIL_CONFIG["receiver"]
            msg.attach(MIMEText(html_content, 'html'))

            print("📧 Sending Smart Email...")
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                server.login(EMAIL_CONFIG["sender"], EMAIL_CONFIG["password"])
                server.send_message(msg)
            
            print("✅ Email Sent!")
        
        else:
            print("✅ No critical issues found today.")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    check_and_alert()


