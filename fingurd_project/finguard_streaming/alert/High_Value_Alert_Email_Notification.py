import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pyspark.sql import functions as F
from pyspark import pipelines as dp

# ==========================================
# Gmail account credentials
# App password is stored in Databricks secret scope
# ==========================================
EMAIL = "loluolafemiwa@gmail.com"
APP_PASSWORD = dbutils.secrets.get(scope="finguard-scope", key="gmail_api_key")

# ==========================================
# Email sending function
# ==========================================
def send_alert_email(row):
    """Send an email alert for a high-value transaction. Returns a status string."""
    # The alert table is built with a LEFT JOIN to customers, so customer_email
    # can be null — skip those instead of failing the whole batch.
    if not row.get("customer_email"):
        return "skipped: no customer email"

    subject = f"🚨 High Value Transaction Alert — {row['alert_id']}"

    body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333;">
        <h2 style="color: #cc0000;">High Value Transaction Alert From FINGUARD</h2>
        <p>Dear {row['customer_name']},</p>
        <p>A transaction on your account has exceeded your configured transaction limit.</p>
        <table style="border-collapse: collapse; width: 100%; margin-top: 15px;">
            <tr><td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Alert ID</td><td style="padding: 8px; border: 1px solid #ddd;">{row['alert_id']}</td></tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Transaction ID</td><td style="padding: 8px; border: 1px solid #ddd;">{row['transaction_id']}</td></tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Amount</td><td style="padding: 8px; border: 1px solid #ddd;">{row['transaction_amount']} {row['transaction_currency']}</td></tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Merchant Category</td><td style="padding: 8px; border: 1px solid #ddd;">{row['merchant_category']}</td></tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Transaction Type</td><td style="padding: 8px; border: 1px solid #ddd;">{row['transaction_type']}</td></tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Payment Channel</td><td style="padding: 8px; border: 1px solid #ddd;">{row['payment_channel']}</td></tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Location</td><td style="padding: 8px; border: 1px solid #ddd;">{row['city']}, {row['country']}</td></tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">International</td><td style="padding: 8px; border: 1px solid #ddd;">{'Yes' if row['is_international'] else 'No'}</td></tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Transaction Time</td><td style="padding: 8px; border: 1px solid #ddd;">{row['transaction_timestamp']}</td></tr>
            <tr><td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">Status</td><td style="padding: 8px; border: 1px solid #ddd;">{row['status']}</td></tr>
        </table>
        <p style="margin-top: 20px;">If this was not you, please contact support immediately.</p>
        <p style="color: #888; font-size: 12px;">Alert generated at {row['alert_timestamp']}</p>
    </body>
    </html>
    """

    msg = MIMEMultipart()
    msg["From"] = EMAIL
    msg["To"] = row['customer_email']
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=20) as server:
            server.login(EMAIL, APP_PASSWORD)
            server.send_message(msg)
        return "sent"
    except Exception as e:
        return f"FAILED: {type(e).__name__}: {e}"


# ==========================================
# ForEachBatch sink — sends an email for each alert row.
# The foreachBatch callback runs in a separate worker process on serverless
# compute, so print() output is not visible; every send attempt is recorded
# in a log table instead.
# ==========================================
@dp.foreach_batch_sink(name="high_value_alert_email_notification")
def high_value_alert_email_notification(df, batch_id):
    """Process each batch of high-value transaction alerts by sending email notifications."""
    # Convert Rows to dicts first — Row objects in the serverless worker
    # don't support .get() the way client-side dicts do.
    dicts = [r.asDict() for r in df.collect()]
    results = [(d["alert_id"], d["customer_email"], send_alert_email(d)) for d in dicts]
    if results:
        log_df = df.sparkSession.createDataFrame(
            [(a, e, s) for a, e, s in results],
            "alert_id string, customer_email string, status string",
        ).withColumn("logged_at", F.current_timestamp())
        log_df.write.mode("append").saveAsTable("finguard.gold.high_value_alert_email_log")


# ==========================================
# Append flow — reads the high-value transaction alert table as a stream
# and feeds it into the foreach_batch sink. SDP manages checkpointing
# and triggers automatically.
# ==========================================
@dp.append_flow(target="high_value_alert_email_notification")
def high_value_alert_email_flow():
    return spark.readStream.table("finguard.gold.high_value_transactions_alert")