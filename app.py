import os
import sys
import subprocess
import threading
import time
import pandas as pd
from flask import Flask, render_template, request, redirect, jsonify
from kiteconnect import KiteConnect
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Path Setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Standalone mode: .env is in the same folder as main.py
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Credentials
API_KEY = os.getenv("KITE_API_KEY")
API_SECRET = os.getenv("KITE_API_SECRET")
CLOUD_DATABASE_URL = os.getenv("CLOUD_DATABASE_URL")

app = Flask(__name__)

# Global status for long-running scripts (Added 'login')
script_status = {
    "login": {"status": "idle", "output": ""},
    "fetch": {"status": "idle", "output": ""},
    "features": {"status": "idle", "output": ""},
    "scoring": {"status": "idle", "output": ""},
    "sync": {"status": "idle", "output": ""}
}

def get_top_10():
    """Fetch Top 10 predictions from Cloud DB."""
    if not CLOUD_DATABASE_URL:
        return []
    try:
        engine = create_engine(CLOUD_DATABASE_URL)
        with engine.connect() as conn:
            # Added market_regime to query
            query = text("SELECT symbol, omre_score, signal, stop_loss, target_price, market_regime FROM predictions ORDER BY omre_score DESC LIMIT 10")
            df = pd.read_sql(query, conn)
            return df.to_dict(orient='records')
    except Exception as e:
        print(f"Error fetching Top 10: {e}")
        return []

@app.route('/')
def dashboard():
    token = os.getenv("KITE_ACCESS_TOKEN")
    status = "Active" if token else "Expired/Missing"
    top_10 = get_top_10()
    
    # Extract Market Regime from the first stock (it's global)
    market_regime = top_10[0]['market_regime'] if top_10 and 'market_regime' in top_10[0] else "Unknown"
    
    return render_template('dashboard.html', token_status=status, top_stocks=top_10, market_regime=market_regime)

@app.route('/automated_login', methods=['POST'])
def automated_login():
    data = request.json
    otp = data.get("otp")
    if not otp:
        return jsonify({"error": "No OTP provided"}), 400

    def automation_task():
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from webdriver_manager.chrome import ChromeDriverManager
        import urllib.parse as urlparse

        script_status["login"]["status"] = "running"
        script_status["login"]["output"] = "🚀 Starting FAST Automated Login...\n"

        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-images")
            chrome_options.add_argument("--blink-settings=imagesEnabled=false")
            chrome_options.page_load_strategy = 'eager'
            chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
            
            # In Docker/Render, use Chromium and Chromium-Driver
            if os.path.exists("/usr/bin/chromium"):
                chrome_options.binary_location = "/usr/bin/chromium"
                script_status["login"]["output"] += "🖥️ Using Chromium...\n"
                # Use fixed chromedriver path
                service = Service(executable_path="/usr/bin/chromedriver")
                script_status["login"]["output"] += "⚙️ Initializing Chromium driver...\n"
                driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                # Local Windows/Dev mode fallback
                from webdriver_manager.chrome import ChromeDriverManager
                script_status["login"]["output"] += "⚙️ Initializing local Chrome driver...\n"
                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            driver.set_page_load_timeout(10)  # Fast timeout
            kite = KiteConnect(api_key=API_KEY)
            login_url = kite.login_url()
            
            script_status["login"]["output"] += f"🔗 Navigating to Kite...\n"
            driver.get(login_url)

            # Step 1: User/Pass - BE FAST!
            script_status["login"]["output"] += "🔑 Step 1: Credentials...\n"
            user_field = WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.ID, "userid")))
            user_field.send_keys("ZAB106")
            driver.find_element(By.ID, "password").send_keys("OMRE@2025")
            driver.find_element(By.XPATH, "//button[@type='submit']").click()
            
            # Step 2: 2FA - IMMEDIATE entry, no delays!
            script_status["login"]["output"] += "📱 Step 2: 2FA Code...\n"
            # Wait for OTP field with short timeout
            otp_field = None
            try:
                otp_field = WebDriverWait(driver, 8).until(
                    EC.presence_of_element_located((By.XPATH, "//input[@type='number' or @type='text' or @type='tel']"))
                )
            except:
                # Fallback: find any input that appears after login
                time.sleep(1)
                inputs = driver.find_elements(By.TAG_NAME, "input")
                for inp in inputs:
                    if inp.is_displayed() and inp.get_attribute("type") in ["text", "number", "tel"]:
                        otp_field = inp
                        break
            
            if otp_field:
                otp_field.clear()
                otp_field.send_keys(otp)
                # Immediately click submit
                try:
                    submit_btn = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[@type='submit']"))
                    )
                    submit_btn.click()
                except:
                    driver.find_element(By.TAG_NAME, "button").click()
                script_status["login"]["output"] += "➡️ 2FA submitted!\n"
            else:
                raise Exception("Could not find OTP field")

            # Step 3: Wait for redirect - should be fast now
            script_status["login"]["output"] += "⏳ Step 3: Waiting for Redirect...\n"
            WebDriverWait(driver, 15).until(lambda d: "request_token=" in d.current_url)
            
            final_url = driver.current_url
            script_status["login"]["output"] += f"🎯 Got callback URL!\n"
            
            parsed = urlparse.urlparse(final_url)
            req_token = urlparse.parse_qs(parsed.query)['request_token'][0]
            
            script_status["login"]["output"] += f"✅ Token: {req_token[:8]}...\n"
            driver.quit()

            # Step 4: Swap for Access Token
            data = kite.generate_session(req_token, api_secret=API_SECRET)
            access_token = data["access_token"]
            os.environ["KITE_ACCESS_TOKEN"] = access_token
            # Update .env
            env_path = os.path.join(BASE_DIR, ".env")
            with open(env_path, 'r', encoding='utf-8') as f: lines = f.readlines()
            new_lines = [l if not l.startswith("KITE_ACCESS_TOKEN=") else f"KITE_ACCESS_TOKEN={access_token}\n" for l in lines]
            if not any(l.startswith("KITE_ACCESS_TOKEN=") for l in lines): new_lines.append(f"KITE_ACCESS_TOKEN={access_token}\n")
            with open(env_path, 'w', encoding='utf-8') as f: f.writelines(new_lines)
            
            script_status["login"]["status"] = "done"
            script_status["login"]["output"] += "💎 Access Token Updated. Starting Pipeline...\n"

            # AUTO TRIGGER CHAIN
            run_script_internal("fetch")
            # The others will be triggered via status polling or we can chain them here if we want synchronous (but better to stay async)
            # Actually, let's chain them for "One-Touch"
            
        except Exception as e:
            script_status["login"]["status"] = "failed"
            script_status["login"]["output"] += f"❌ Login Failed: {str(e)}\n"
            if 'driver' in locals(): driver.quit()

    threading.Thread(target=automation_task).start()
    return jsonify({"status": "started"})

def run_script_internal(script_id):
    """Helper to run scripts from backend."""
    # This is a bit recursive, but works for the chain
    if script_status[script_id]["status"] == "running": return
    # Trigger via the existing route logic
    threading.Thread(target=lambda: trigger_script_chain(script_id)).start()

def trigger_script_chain(script_id):
    # Use LOCAL scripts folder for standalone deployment
    scripts = {
        "fetch": os.path.join(BASE_DIR, "scripts", "daily_update.py"),
    }
    
    if script_id not in scripts:
        script_status[script_id]["status"] = "failed"
        script_status[script_id]["output"] = f"Unknown script: {script_id}"
        return
    
    script_status[script_id]["status"] = "running"
    script_status[script_id]["output"] = f"🚀 Starting {script_id}...\n"
    
    try:
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        
        cmd = [sys.executable, scripts[script_id]]
        # Use binary stdout to handle encoding errors manually
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=BASE_DIR, env=env)
        
        # Store process reference to allow stopping
        script_status[script_id]["process"] = process
        
        # Read byte by byte/line by line
        for line in iter(process.stdout.readline, b''):
            try:
                # Decode as utf-8, replace bad characters (like Windows emojis)
                decoded_line = line.decode('utf-8', errors='replace')
                script_status[script_id]["output"] += decoded_line
            except:
                pass
        
        process.wait()
        script_status[script_id]["process"] = None
        if process.returncode == 0:
            script_status[script_id]["status"] = "done"
            script_status[script_id]["output"] += "\n✅ Fetch complete!"
        else:
            # If terminated (negative return code on Unix, or status was set to stopped)
            if script_status[script_id]["status"] != "stopped":
                script_status[script_id]["status"] = "failed"
                script_status[script_id]["output"] += f"\n❌ Failed with code {process.returncode}"
    except Exception as e:
        script_status[script_id]["status"] = "failed"
        script_status[script_id]["output"] += f"\nError: {e}"
        script_status[script_id]["process"] = None

@app.route('/login')
def login():
    kite = KiteConnect(api_key=API_KEY)
    return redirect(kite.login_url())

@app.route('/callback')
def callback():
    request_token = request.args.get("request_token")
    if not request_token:
        return "No request_token found!", 400
    
    try:
        kite = KiteConnect(api_key=API_KEY)
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        access_token = data["access_token"]
        
        # AUTO-SAVE to .env
        os.environ["KITE_ACCESS_TOKEN"] = access_token
        env_path = os.path.join(BASE_DIR, ".env")
        with open(env_path, 'r', encoding='utf-8') as f: 
            lines = f.readlines()
        new_lines = []
        token_found = False
        for l in lines:
            if l.startswith("KITE_ACCESS_TOKEN="):
                new_lines.append(f"KITE_ACCESS_TOKEN={access_token}\n")
                token_found = True
            else:
                new_lines.append(l)
        if not token_found:
            new_lines.append(f"KITE_ACCESS_TOKEN={access_token}\n")
        with open(env_path, 'w', encoding='utf-8') as f: 
            f.writelines(new_lines)
        
        # AUTO-TRIGGER PIPELINE
        def run_pipeline():
            time.sleep(1)
            run_script_internal("fetch")
        threading.Thread(target=run_pipeline).start()
        
        # Redirect to dashboard with success message
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: 'Outfit', sans-serif; background: #0f172a; color: white; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }}
                .card {{ background: rgba(34, 197, 94, 0.1); border: 1px solid #22c55e; padding: 40px; border-radius: 16px; text-align: center; }}
                h2 {{ color: #22c55e; margin-bottom: 16px; }}
                p {{ color: #94a3b8; }}
            </style>
        </head>
        <body>
            <div class="card">
                <h2>✅ Login Successful!</h2>
                <p>Token saved. Pipeline starting automatically.</p>
                <p style="font-size: 0.8rem; margin-top: 20px;">This window will close in 2 seconds...</p>
            </div>
            <script>
                // Close popup after 2 seconds
                setTimeout(function() {{
                    window.close();
                }}, 2000);
            </script>
        </body>
        </html>
        """
    except Exception as e:
        return f"Error generating session: {e}", 500

@app.route('/api/top10')
def api_top10():
    """Return top 10 predictions from cloud database as JSON"""
    try:
        predictions = get_top_10()
        return jsonify(predictions)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/run/<script_id>')
def run_script(script_id):
    # Use LOCAL scripts folder
    scripts = {
        "fetch": os.path.join(BASE_DIR, "scripts", "daily_update.py"),
    }
    
    if script_id not in scripts:
        return jsonify({"error": "Invalid script"}), 400
    
    if script_status[script_id]["status"] == "running":
        return jsonify({"error": "Already running"}), 400
    
    def target():
        script_status[script_id]["status"] = "running"
        script_status[script_id]["output"] = f"Starting {script_id}...\n"
        
        try:
            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            
            cmd = [sys.executable, scripts[script_id]]
            process = subprocess.Popen(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT,
                cwd=BASE_DIR,
                env=env
            )
            script_status[script_id]["process"] = process
            
            for line in iter(process.stdout.readline, b''):
                try:
                    decoded_line = line.decode('utf-8', errors='replace')
                    script_status[script_id]["output"] += decoded_line
                except:
                    pass
            
            process.wait()
            script_status[script_id]["process"] = None
            if script_status[script_id]["status"] != "stopped":
                script_status[script_id]["status"] = "done" if process.returncode == 0 else "failed"
        except Exception as e:
            script_status[script_id]["status"] = "failed"
            script_status[script_id]["output"] += f"\nError: {e}"
            script_status[script_id]["process"] = None

    threading.Thread(target=target).start()
    return jsonify({"status": "started"})

@app.route('/stop/<script_id>')
def stop_script(script_id):
    if script_id not in script_status:
        return jsonify({"error": "Invalid script"}), 400
    
    process = script_status[script_id].get("process")
    if process:
        try:
            # Terminate the process tree (Windows friendly)
            import subprocess
            if sys.platform == 'win32':
                subprocess.run(['taskkill', '/F', '/T', '/PID', str(process.pid)], capture_output=True)
            else:
                process.terminate()
            
            script_status[script_id]["status"] = "stopped"
            script_status[script_id]["output"] += "\n🛑 Process stopped by user.\n"
            return jsonify({"status": "stopped"})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    return jsonify({"error": "No running process found"}), 400

@app.route('/status')
def get_status():
    # Create a clean version of the status to return as JSON
    # subprocess.Popen objects are not JSON serializable
    clean_status = {}
    for key, val in script_status.items():
        clean_status[key] = {k: v for k, v in val.items() if k != "process"}
    return jsonify(clean_status)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
