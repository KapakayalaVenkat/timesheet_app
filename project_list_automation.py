# timesheet_auto_update.py
import os
import sys
import time
import logging
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
 
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -----------------------
# CONFIG
# -----------------------
EXCEL_PATH = os.getenv("EXCEL_PATH", "utilization_template.xlsx")  # override via env if needed
OUTPUT_OK = "timesheet_success.csv"
OUTPUT_FAIL = "timesheet_failed.csv"
LOG_PATH = "timesheet_run.log"

REQUIRED_COLUMNS = ["employee_id", "date", "project_code", "man_days", "activity"]
DATE_FMT = "%Y-%m-%d"  # ISO

# -----------------------
# LOGGING
# -----------------------
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
logging.getLogger().addHandler(console)

# -----------------------
# LOAD SECRETS
# -----------------------
load_dotenv()
PORTAL_URL = os.getenv("HR_PORTAL_URL")
USERNAME = os.getenv("HR_USERNAME")
PASSWORD = os.getenv("HR_PASSWORD")

if not all([PORTAL_URL, USERNAME, PASSWORD]):
    logging.error("Please set HR_PORTAL_URL, HR_USERNAME, HR_PASSWORD in .env")
    sys.exit(1)

# -----------------------
# BROWSER SETUP
# -----------------------
def build_driver(headless: bool = True):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,1000")
    opts.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(60)
    return driver

# -----------------------
# PORTAL ACTIONS
# -----------------------
class Portal:
    def __init__(self, driver):
        self.d = driver
        self.wait = WebDriverWait(self.d, 30)

    def login(self):
        self.d.get(PORTAL_URL)

        # TODO: Replace the below selectors with real ones from your portal
        # Example:
        user_sel = (By.ID, "username")
        pass_sel = (By.ID, "password")
        btn_sel  = (By.CSS_SELECTOR, "button[type='submit']")

        self.wait.until(EC.presence_of_element_located(user_sel)).send_keys(USERNAME)
        self.d.find_element(*pass_sel).send_keys(PASSWORD)
        self.d.find_element(*btn_sel).click()

        # Wait for dashboard/home marker
        # Example: some element visible only after login
        # Change selector accordingly
        dashboard_marker = (By.CSS_SELECTOR, "[data-test='dashboard']")
        try:
            self.wait.until(EC.presence_of_element_located(dashboard_marker))
            logging.info("Login successful.")
        except TimeoutException:
            logging.warning("Login marker not found; continuing if the portal redirects differently.")

    def open_timesheet_form(self):
        # TODO: Navigate to timesheet page
        # Example path: click menu -> Timesheet
        # Replace selectors below with actual ones
        try:
            menu_sel = (By.LINK_TEXT, "Timesheet")
            self.wait.until(EC.element_to_be_clickable(menu_sel)).click()
        except TimeoutException:
            logging.error("Couldn't open Timesheet page. Update selectors.")
            raise

    @retry(
        retry=retry_if_exception_type((TimeoutException, NoSuchElementException, WebDriverException)),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        reraise=True
    )
    def submit_entry(self, row: dict):
        """
        row => {
          employee_id, date (YYYY-MM-DD), project_code, man_days (float), activity, remarks?
        }
        """
        # Ensure on timesheet page/form
        # Example: click "Add Entry"
        add_btn = (By.CSS_SELECTOR, "button[data-test='add-entry']")
        self.wait.until(EC.element_to_be_clickable(add_btn)).click()

        # Fill fields (replace with real selectors)
        emp_input   = (By.ID, "employeeId")
        date_input  = (By.ID, "date")
        proj_input  = (By.ID, "project")
        days_input  = (By.ID, "mandays")
        act_input   = (By.ID, "activity")
        rem_input   = (By.ID, "remarks")
        save_btn    = (By.CSS_SELECTOR, "button[data-test='save']")

        self.wait.until(EC.presence_of_element_located(emp_input)).clear()
        self.d.find_element(*emp_input).send_keys(str(row["employee_id"]))

        # Date field: sometimes needs ENTER after value
        date_el = self.d.find_element(*date_input)
        date_el.clear()
        date_el.send_keys(row["date"])
        date_el.send_keys(Keys.ENTER)

        self.d.find_element(*proj_input).clear()
        self.d.find_element(*proj_input).send_keys(str(row["project_code"]))

        self.d.find_element(*days_input).clear()
        self.d.find_element(*days_input).send_keys(str(row["man_days"]))

        self.d.find_element(*act_input).clear()
        self.d.find_element(*act_input).send_keys(str(row["activity"]))

        if "remarks" in row and pd.notna(row["remarks"]):
            self.d.find_element(*rem_input).clear()
            self.d.find_element(*rem_input).send_keys(str(row["remarks"]))

        self.d.find_element(*save_btn).click()

        # Confirm toast/snackbar
        toast = (By.CSS_SELECTOR, "[data-test='toast-success']")
        self.wait.until(EC.visibility_of_element_located(toast))
        logging.info(f"Submitted: {row}")

# -----------------------
# DATA LAYER
# -----------------------
def validate_frame(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    def coerce_date(s):
        try:
            return datetime.strptime(str(s), DATE_FMT).strftime(DATE_FMT)
        except Exception:
            raise ValueError(f"Bad date format (expected {DATE_FMT}): {s}")

    df = df.copy()
    df["date"] = df["date"].apply(coerce_date)
    df["man_days"] = pd.to_numeric(df["man_days"], errors="coerce")
    if df["man_days"].isna().any():
        bad = df[df["man_days"].isna()]
        raise ValueError(f"Non-numeric man_days found in rows: {bad.index.tolist()}")
    if (df["man_days"] < 0).any():
        bad = df[df["man_days"] < 0]
        raise ValueError(f"Negative man_days found in rows: {bad.index.tolist()}")
    return df

# -----------------------
# MAIN
# -----------------------
def main():
    try:
        df = pd.read_excel(EXCEL_PATH)
    except Exception as e:
        logging.error(f"Failed to read Excel: {e}")
        sys.exit(1)

    try:
        df = validate_frame(df)
    except Exception as e:
        logging.error(f"Validation error: {e}")
        sys.exit(1)

    success_rows, failed_rows = [], []

    driver = build_driver(headless=True)
    portal = Portal(driver)
    try:
        portal.login()
        portal.open_timesheet_form()

        for _, r in df.iterrows():
            row = r.to_dict()
            try:
                portal.submit_entry(row)
                success_rows.append(row)
                # Optional small delay to be gentle on server
                time.sleep(1)
            except Exception as e:
                logging.exception(f"Failed for row {row}: {e}")
                row["error"] = str(e)
                failed_rows.append(row)

    finally:
        driver.quit()

    # Write reports
    if success_rows:
        pd.DataFrame(success_rows).to_csv(OUTPUT_OK, index=False)
        logging.info(f"Wrote {OUTPUT_OK} ({len(success_rows)} rows).")
    if failed_rows:
        pd.DataFrame(failed_rows).to_csv(OUTPUT_FAIL, index=False)
        logging.warning(f"Wrote {OUTPUT_FAIL} ({len(failed_rows)} rows).")

    # Exit status
    if failed_rows and not success_rows:
        sys.exit(2)
    elif failed_rows:
        sys.exit(0)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
