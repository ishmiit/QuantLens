import pyotp, time, requests, config
import sys
import os, psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def get_access_token():
    auth_url = f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={config.API_KEY}&redirect_uri={config.REDIRECT_URI}"
    
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--incognito") 
    options.add_argument("--disable-blink-features=AutomationControlled") 
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 15)

    try:
        driver.get(auth_url)
        time.sleep(2)  # Delay after initial page load
        
        # 1. Mobile Number
        print("🔗 Step 1: Entering Mobile Number...")
        mobile_field = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "mobileNum")))
        mobile_field.send_keys(config.MOBILE_NO)
        time.sleep(1) # Small pause before clicking
        driver.find_element(By.ID, "getOtp").click()
        
        # Delay for UI to transition to OTP screen
        time.sleep(2) 

        # 2. TOTP Input (Simulating human typing)
        print("🔗 Step 2: Generating & Entering TOTP...")
        otp_field = wait.until(EC.presence_of_element_located((By.ID, "otpNum")))
        
        # Generate TOTP code
        totp_secret = os.getenv('UPSTOX_TOTP_SECRET')
        if not totp_secret:
            print("❌ CRITICAL: UPSTOX_TOTP_SECRET is missing from environment variables.")
            sys.exit(1)
            
        otp_value = pyotp.TOTP(totp_secret.replace(' ', '')).now()
        
        # Clear the field first just in case
        otp_field.clear()
        
        # Type each digit one-by-one with a tiny delay
        for digit in otp_value:
            otp_field.send_keys(digit)
            time.sleep(0.2) # 200ms delay between keystrokes
        
        print(f"⌨️ Typed TOTP: {otp_value}")
        time.sleep(1) # Wait for the 'Continue' button to wake up
        
        # Click the continue button
        wait.until(EC.element_to_be_clickable((By.ID, "continueBtn"))).click()

        # 3. Security PIN
        print("🔗 Step 3: Entering PIN...")
        pin_field = wait.until(EC.presence_of_element_located((By.ID, "pinCode")))
        
        upstox_pin = os.getenv('UPSTOX_PIN')
        if not upstox_pin:
            print("❌ CRITICAL: UPSTOX_PIN is missing from environment variables.")
            sys.exit(1)
            
        pin_field.send_keys(upstox_pin)
        time.sleep(1) # Small pause before clicking
        
        try:
            wait.until(EC.element_to_be_clickable((By.ID, "pinContinueBtn"))).click()
        except:
            pass # Ignore if it already submitted

        # 4. Handle Redirection & Code Capture
        print("🔗 Step 4: Waiting for Redirect & Auth Code...")
        
        wait.until(EC.url_contains("code="))
        
        import urllib.parse
        current_url = driver.current_url
        parsed_url = urllib.parse.urlparse(current_url)
        auth_code = urllib.parse.parse_qs(parsed_url.query)['code'][0]

        # 5. Token Exchange
        print("🔗 Step 5: Exchanging Code for Token...")
        
        token_url = 'https://api.upstox.com/v2/login/authorization/token'
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = {
            'code': auth_code,
            'client_id': config.API_KEY,
            'client_secret': config.API_SECRET,
            'redirect_uri': config.REDIRECT_URI,
            'grant_type': 'authorization_code'
        }
        
        res = requests.post(token_url, headers=headers, data=data).json()
        
        if 'access_token' in res:
            token = res['access_token']
            
            # 6. Save to Neon Database
            print("🔗 Step 6: Saving Token to Neon Database...")
            db_url = os.getenv("DATABASE_URL")
            if db_url:
                try:
                    conn = psycopg2.connect(db_url)
                    cur = conn.cursor()
                    cur.execute('''
                        CREATE TABLE IF NOT EXISTS system_config (
                            key VARCHAR(50) PRIMARY KEY,
                            value TEXT NOT NULL,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    cur.execute('''
                        INSERT INTO system_config (key, value, updated_at) 
                        VALUES ('UPSTOX_TOKEN', %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (key) DO UPDATE SET 
                            value = EXCLUDED.value,
                            updated_at = EXCLUDED.updated_at
                    ''', (token,))
                    conn.commit()
                    cur.close()
                    conn.close()
                    print("✅ SUCCESS: Token securely saved to database.")
                except Exception as e:
                    print(f"❌ Database error: {e}")
            else:
                print("⚠️ DATABASE_URL not found. Skipping database persistence.")

            return token
        else:
            print(f"❌ Token Exchange Failed: {res}")
            
    except Exception as e:
        print(f"❌ Automation Error: {e}")
        driver.save_screenshot('error_screenshot.png')
        sys.exit(1)
    finally:
        driver.quit()
    return None

if __name__ == "__main__":
    get_access_token()