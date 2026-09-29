# ================= IMPORTS =================
import csv
import time
import json
import random
import pickle
import re
import os
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException


LOGIN_CSV = "login.csv"
HEADLESS = False
LOGIN_WAIT_TIMEOUT = 900   # longer wait for FB security redirects

# ================= DRIVER =================
# ================= DRIVER =================
def create_driver():

    options = webdriver.ChromeOptions()

    # ─────────────────────────────────────
    # HEADLESS MODE
    # ─────────────────────────────────────
    # Faster browser rendering
    # If Facebook causes issues:
    # comment this line temporarily
    # ─────────────────────────────────────
    if HEADLESS:
        options.add_argument("--headless=new")

    # ─────────────────────────────────────
    # BASIC SETTINGS
    # ─────────────────────────────────────
    options.add_argument("--window-size=1920,1080")

    # Anti-detection
    options.add_argument("--disable-blink-features=AutomationControlled")

    options.add_experimental_option(
        "excludeSwitches",
        ["enable-automation"]
    )

    options.add_experimental_option(
        "useAutomationExtension",
        False
    )

    # ─────────────────────────────────────
    # SPEED OPTIMIZATIONS
    # ─────────────────────────────────────

    # Disable GPU rendering
    options.add_argument("--disable-gpu")

    # Disable extensions
    options.add_argument("--disable-extensions")

    # Disable notifications
    options.add_argument("--disable-notifications")

    # Disable popup blocking
    options.add_argument("--disable-popup-blocking")

    # Reduce memory usage
    options.add_argument("--disable-dev-shm-usage")

    # Faster Chrome startup
    options.add_argument("--no-sandbox")

    # Disable syncing
    options.add_argument("--disable-sync")

    # Disable background apps
    options.add_argument("--disable-background-networking")

    # Disable default apps
    options.add_argument("--disable-default-apps")

    # Disable translate
    options.add_argument("--disable-translate")

    # Disable logging
    options.add_argument("--log-level=3")

    # Disable automation info bar
    options.add_argument("--disable-infobars")

    # Faster page loading
    options.page_load_strategy = "eager"

    # ─────────────────────────────────────
    # DISABLE IMAGES (BIG SPEED BOOST)
    # ─────────────────────────────────────
    prefs = {
        "profile.managed_default_content_settings.images": 1,

        # Optional additional speed boosts
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.cookies": 1,
        "profile.managed_default_content_settings.javascript": 1,
        "profile.managed_default_content_settings.plugins": 2,
        "profile.managed_default_content_settings.popups": 2,
        "profile.managed_default_content_settings.geolocation": 2,
        "profile.managed_default_content_settings.media_stream": 2,
    }

    options.add_experimental_option("prefs", prefs)

    # ─────────────────────────────────────
    # CREATE DRIVER
    # ─────────────────────────────────────
    driver = webdriver.Chrome(options=options)

    # ─────────────────────────────────────
    # REMOVE webdriver FLAG
    # Helps reduce bot detection
    # ─────────────────────────────────────
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });

            Object.defineProperty(navigator, 'platform', {
                get: () => 'MacIntel'
            });

            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });

            Object.defineProperty(navigator, 'plugins', {
                get: () => [1,2,3,4,5]
            });
            """
        },
    )

    return driver


# ================= COOKIE HELPERS =================
def safe_cookie_name(identifier):
    identifier = identifier.strip().lower()
    identifier = identifier.replace("@", "_at_").replace(".", "_")
    identifier = re.sub(r"[^a-z0-9_]+", "", identifier)
    return f"{identifier}_cookies.pkl"


def cookie_file(identifier):
    return Path(safe_cookie_name(identifier))


def save_cookies(driver, identifier):
    """Save cookies in both pickle and JSON formats"""
    try:
        if "facebook.com" not in driver.current_url.lower():
            print("[!] Not saving cookies — homepage not loaded")
            return

        cookies = driver.get_cookies()
        if not cookies:
            print("[!] No cookies to save")
            return

        pkl = cookie_file(identifier)

        # Save as pickle
        with open(pkl, "wb") as f:
            pickle.dump(cookies, f)
        print(f"[✓] Cookies saved (pickle): {pkl.name}")

        # Save as JSON
        json_file = pkl.with_suffix(".json")
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(cookies, f, indent=2)
        print(f"[✓] Cookies saved (JSON): {json_file.name}")
        
    except Exception as e:
        print(f"[!] Error saving cookies: {e}")


def session_active(driver):
    """Robust login detection"""
    try:
        names = [c['name'] for c in driver.get_cookies()]
    except:
        names = []

    url = driver.current_url.lower()

    if "c_user" in names or "xs" in names:
        return True

    if "login" not in url and "checkpoint" not in url:
        return True

    if "facebook.com/home" in url:
        return True

    return False


def load_cookies(driver, identifier):
    pkl = cookie_file(identifier)
    json_file = pkl.with_suffix(".json")

    if not pkl.exists() and not json_file.exists():
        return False

    source = pkl if pkl.exists() else json_file
    print(f"[i] Loading cookies from {source.name}")

    driver.get("https://www.facebook.com/")
    WebDriverWait(driver, 50).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    time.sleep(3)

    cookies = pickle.load(open(source, "rb")) if source.suffix == ".pkl" else json.load(open(source))

    for c in cookies:
        try:
            if "expiry" in c:
                c["expiry"] = int(c["expiry"])
            driver.add_cookie(c)
        except:
            continue

    driver.get("https://www.facebook.com/")
    time.sleep(6)

    return session_active(driver)


# ================= UTILS =================
def type_like_human(el, text):
    """Type text with randomized delays to mimic human behavior"""
    for i, ch in enumerate(text):
        # Random typing speed variation
        if ch.isupper():
            time.sleep(random.uniform(0.05, 0.1))  # Slight pause before uppercase
        
        el.send_keys(ch)
        
        # Variable delay between keystrokes
        if random.random() < 0.1:  # 10% chance of pause
            time.sleep(random.uniform(0.2, 0.5))
        else:
            time.sleep(random.uniform(0.05, 0.15))
        
        # Occasional longer pause (simulating thinking)
        if random.random() < 0.05 and i < len(text) - 1:
            time.sleep(random.uniform(0.3, 0.8))


def random_mouse_movement(driver):
    """Perform random mouse movements to appear more human-like"""
    try:
        actions = ActionChains(driver)
        x = random.randint(0, 1920)
        y = random.randint(0, 1080)
        actions.move_by_offset(x, y).perform()
        time.sleep(random.uniform(0.2, 0.5))
    except:
        pass


def safe_click_login(driver):
    try:
        driver.find_element(By.NAME, "login").click()
    except:
        driver.find_element(By.NAME, "pass").send_keys(Keys.ENTER)


def wait_for_login(driver, timeout=LOGIN_WAIT_TIMEOUT):
    print("[i] Waiting for login completion...")
    end = time.time() + timeout

    while time.time() < end:
        if session_active(driver):
            return True
        time.sleep(2)

    return False


# ================= LOGIN =================
def login(identifier, password):
    print(f"[•] Login start → {identifier}")
    driver = create_driver()

    # TRY COOKIE LOGIN
    if load_cookies(driver, identifier):
        print("[✓] Cookies loaded — proceeding without verification")
        time.sleep(random.uniform(3, 7))
        random_mouse_movement(driver)
        return driver

    print("[i] Cookie login incomplete — proceeding with manual login")

    driver.get("https://www.facebook.com/login")
    time.sleep(random.uniform(2, 4))

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.NAME, "email"))
        )
    except TimeoutException:
        if session_active(driver):
            return driver
        print("[!] Login page timeout")
        driver.quit()
        return None

    time.sleep(random.uniform(1, 2))
    random_mouse_movement(driver)
    
    email = driver.find_element(By.NAME, "email")
    passwd = driver.find_element(By.NAME, "pass")

    print(f"[i] Typing email/phone: {identifier[:3]}***")
    type_like_human(email, identifier)
    time.sleep(random.uniform(0.5, 1.5))
    
    print("[i] Typing password...")
    type_like_human(passwd, password)
    time.sleep(random.uniform(0.5, 1.5))

    random_mouse_movement(driver)
    safe_click_login(driver)
    print("[i] Login submitted — waiting for verification...")

    time.sleep(random.uniform(20, 30))

    if wait_for_login(driver):
        print("[✓] Login successful")
        time.sleep(random.uniform(3, 6))
        
        if session_active(driver):
            save_cookies(driver, identifier)
        return driver

    print("[!] Login not confirmed — waiting extra time...")
    time.sleep(random.uniform(20, 30))

    if session_active(driver):
        print("[✓] Login confirmed after delay")
        save_cookies(driver, identifier)
        return driver

    print("[!] Login verification not confirmed — continuing anyway")
    return driver


# ================= MAIN =================
def main():
    accounts = []

    try:
        with open(LOGIN_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                email = row.get("email", "").strip()
                phone = row.get("phoneno", "").strip()
                password = row.get("password", "").strip()
                if password and (email or phone):
                    identifier = email or phone
                    accounts.append((identifier, password))
                    print(f"[✓] Loaded account: {identifier[:3]}***")
    except FileNotFoundError:
        print(f"[!] {LOGIN_CSV} not found")
        return None
    except Exception as e:
        print(f"[!] Error reading {LOGIN_CSV}: {e}")
        return None

    if not accounts:
        print("[!] No valid accounts found in login.csv")
        return None

    identifier, password = accounts[0]
    print(f"\n[•] Starting login with: {identifier[:3]}***\n")

    driver = login(identifier, password)

    if driver:
        print("\n[✓] Driver ready for scraping\n")
        return driver   # ✅ IMPORTANT
    else:
        print("\n[!] Driver failed to initialize\n")
        return None


if __name__ == "__main__":
    driver = main()