import pyotp, time, requests, config
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
    # options.add_argument("--headless") 
    options.add_argument("--incognito") 
    options.add_argument("--disable-blink-features=AutomationControlled") 
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 15)

    try:
        driver.get(auth_url)
        time.sleep(2)  # Delay after initial page load
        
        # 1. Mobile Number
        print("🔗 Step 1: Entering Mobile Number...")
        mobile_field = wait.until(EC.element_to_be_clickable((By.ID, "mobileNum")))
        mobile_field.send_keys(config.MOBILE_NO)
        time.sleep(1) # Small pause before clicking
        driver.find_element(By.ID, "getOtp").click()
        
        # Delay for UI to transition to OTP screen
        time.sleep(2) 

        # 2. TOTP Input (Simulating human typing)
        print("🔗 Step 2: Generating & Entering TOTP...")
        otp_field = wait.until(EC.presence_of_element_located((By.ID, "otpNum")))
        
        # Generate TOTP code
        totp = pyotp.TOTP(config.TOTP_KEY.replace(" ", ""))
        otp_value = totp.now()
        
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
        print("🔗 Step 3: Entering Security PIN...")
        pin_field = wait.until(EC.presence_of_element_located((By.ID, "pinCode")))
        pin_field.send_keys(config.PIN)
        
        time.sleep(1) # Small pause before clicking
        driver.find_element(By.ID, "pinContinueBtn").click()

        # 4. Handle Redirection & Code Capture
        print("🔗 Step 4: Capturing Auth Code...")
        time.sleep(3) # Give extra time for the final redirect to finish
        
        wait.until(EC.url_contains("code="))
        
        current_url = driver.current_url
        auth_code = current_url.split("code=")[1].split("&")[0]
        driver.quit()

        # 5. Token Exchange
        print("🚀 Step 5: Exchanging Code for Access Token...")
        res = requests.post('https://api.upstox.com/v2/login/authorization/token', data={
            'code': auth_code, 
            'client_id': config.API_KEY,
            'client_secret': config.API_SECRET, 
            'redirect_uri': config.REDIRECT_URI,
            'grant_type': 'authorization_code'
        }).json()
        
        if 'access_token' in res:
            token = res['access_token']
            with open("token.txt", "w") as f:
                f.write(token)
            print("✅ SUCCESS: Token saved to token.txt")
            return token
        else:
            print(f"❌ Token Exchange Failed: {res}")
            
    except Exception as e:
        print(f"❌ Automation Error: {e}")
    finally:
        driver.quit()
    return None

if __name__ == "__main__":
    get_access_token()