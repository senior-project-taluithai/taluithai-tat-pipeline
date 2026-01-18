#!/usr/bin/env python
"""
Google Login Session Generator
==============================
ใช้เพื่อ login Google แล้ว save session สำหรับใช้กับ Google Maps scraper
"""

import os
from playwright.sync_api import sync_playwright

# Save session in the same directory as this script
SESSION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "google_session.json")


def generate_session():
    print("=" * 60)
    print("🔐 Google Login Session Generator")
    print("=" * 60)
    print()
    print("📋 Instructions:")
    print("   1. Browser จะเปิดขึ้นมา")
    print("   2. Login เข้า Google Account ของคุณ")
    print("   3. หลัง login เสร็จ กลับมากด Enter ที่ terminal นี้")
    print()
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
            ]
        )
        
        context = browser.new_context(
            locale='th-TH',
            timezone_id='Asia/Bangkok',
            viewport={'width': 1280, 'height': 800},
        )
        
        page = context.new_page()
        
        print("🌐 Opening Google login page...")
        page.goto("https://accounts.google.com/signin")
        
        print()
        print("⏳ รอให้คุณ login Google...")
        print("   หลัง login เสร็จแล้ว กด Enter ที่นี่")
        input("   >>> กด Enter เมื่อ login เสร็จแล้ว: ")
        
        context.storage_state(path=SESSION_FILE)
        print()
        print(f"✅ Session saved to: {SESSION_FILE}")
        
        browser.close()
    
    print()
    print("🎉 Done! ตอนนี้สามารถรัน scraper ได้แล้ว")
    print("   scraper จะใช้ session นี้อัตโนมัติ")


if __name__ == "__main__":
    generate_session()
