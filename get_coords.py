import pyautogui
import time

print("❌ MOVE YOUR MOUSE OVER THE 'REFRESH' BUTTON NOW!")
print("3...")
time.sleep(1)
print("2...")
time.sleep(1)
print("1...")
time.sleep(1)

x, y = pyautogui.position()
print(f"✅ CAPTURED! X={x}, Y={y}")
print(f"Copy these numbers into your main script.")