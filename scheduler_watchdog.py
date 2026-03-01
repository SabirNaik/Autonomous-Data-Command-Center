import time
import os
from datetime import datetime

# --- CONFIGURATION ---
TARGET_TIME = "18:30"  # <--- Set your daily schedule here (24-hour format)
PIPELINE_PATH = r"C:\pg_cron_project\run_pipeline.bat"

def run_pipeline():
    print(f"\n🚀 TIME MATCHED! Launching Pipeline at {datetime.now().strftime('%H:%M:%S')}...")
    
    try:
        # Force the correct folder so Python finds the files
        os.chdir(os.path.dirname(PIPELINE_PATH)) 
        
        # Use 'start' to launch the visible batch file window
        os.system(f'start "" "{PIPELINE_PATH}"')
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print(f"=============================================")
    print(f"⏰ DAILY AUTOMATION WATCHDOG ACTIVE")
    print(f"👀 I will trigger the pipeline every day at: {TARGET_TIME}")
    print(f"=============================================")

    while True:
        # Get current time in HH:MM format
        now = datetime.now().strftime("%H:%M")
        
        if now == TARGET_TIME:
            run_pipeline()
            
            # CRITICAL: Wait 60 seconds so we don't run it twice in the same minute
            print(f"✅ Job sent! Waiting 60 seconds to reset...")
            time.sleep(60) 
            
            print(f"💤 Waiting for tomorrow at {TARGET_TIME}...")
            
        else:
            # Check the clock every 10 seconds
            time.sleep(10)