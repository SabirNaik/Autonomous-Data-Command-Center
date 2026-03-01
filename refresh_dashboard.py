import os
import time
import pyautogui
import psutil
from datetime import datetime

# --- CONFIGURATION ---
PBIX_PATH = r"C:\Users\Naiks\Desktop\pg_cron_automation_project.pbix"

# COORDINATES (Keep your working coordinates)
REFRESH_X, REFRESH_Y = 747, 127
SAVE_X, SAVE_Y       = 34, 16
CLOSE_X, CLOSE_Y     = 1892, 10

def kill_power_bi():
    """Closes Power BI if it's already open."""
    for proc in psutil.process_iter():
        try:
            if "PBIDesktop" in proc.name():
                proc.kill()
        except:
            pass

def open_and_refresh():
    print(f"📂 Opening Dashboard at {datetime.now().strftime('%H:%M:%S')}...")
    os.startfile(PBIX_PATH)
    
    # 1. WAIT FOR LOAD (Power BI is heavy, give it time)
    print("⏳ Waiting 45 seconds for Power BI to load...")
    time.sleep(45) 
    
    # 2. FORCE FOCUS (Click Center of Screen)
    print("🖱️ Clicking center to grab focus...")
    screen_width, screen_height = pyautogui.size()
    pyautogui.click(screen_width / 2, screen_height / 2)
    time.sleep(2)
    
    # 3. MAXIMIZE USING KEYBOARD MENU (Safer than Win+Up)
    # Alt+Space opens the window menu, 'x' selects Maximize
    print("🔲 Maximizing Window (Alt+Space -> X)...")
    pyautogui.hotkey('alt', 'space')
    time.sleep(1)
    pyautogui.press('x') 
    time.sleep(3) # Wait for animation

    # 4. CLICK REFRESH
    print(f"🖱️ Clicking Refresh at ({REFRESH_X}, {REFRESH_Y})...")
    pyautogui.moveTo(REFRESH_X, REFRESH_Y, duration=1) 
    pyautogui.click()
    
    # 5. WAIT FOR DATA UPDATE
    print("⏳ Waiting for data refresh (45 seconds)...")
    time.sleep(45)
    
    # 6. SAVE
    print(f"💾 Clicking Save at ({SAVE_X}, {SAVE_Y})...")
    pyautogui.click(SAVE_X, SAVE_Y)
    time.sleep(5)
    
    # 7. CLOSE
    print(f"❌ Clicking Close at ({CLOSE_X}, {CLOSE_Y})...")
    pyautogui.click(CLOSE_X, CLOSE_Y)
    
    print("✅ Process Complete!")

if __name__ == "__main__":
    kill_power_bi()
    open_and_refresh()              