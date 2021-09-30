import os
import csv
import time
import re
from datetime import datetime

from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
import requests
from google.cloud import bigquery

CHROME_OPTIONS = Options()
# if os.getenv("PYTHON_ENV") == "prod":
CHROME_OPTIONS.add_argument("--headless")
CHROME_OPTIONS.add_argument("--no-sandbox")
CHROME_OPTIONS.add_argument("--window-size=1920,1080")
CHROME_OPTIONS.add_argument("--disable-gpu")
CHROME_OPTIONS.add_argument("--disable-dev-shm-usage")
CHROME_OPTIONS.add_argument(
    f"""
    user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) 
    AppleWebKit/537.36 (KHTML, like Gecko)
    Chrome/87.0.4280.141 Safari/537.36
    """
)
CHROME_OPTIONS.add_experimental_option(
    "prefs",
    {
        "download.default_directory": "/tmp",
    },
)

BQ_CLIENT = bigquery.Client()
DATASET = "SBLA_Yotpo"
TABLE = "TotalRevenueSMS"

NOW = datetime.utcnow()


def get_report_request():
    """Get & intercept CSV request from their FE to BE

    Returns:
        str: getReport request URL
    """

    if os.getenv("PYTHON_ENV") == "dev":
        driver = webdriver.Chrome("./chromedriver", options=CHROME_OPTIONS)
    else:
        driver = webdriver.Chrome(options=CHROME_OPTIONS)
    driver.implicitly_wait(20)

    # Navtigate to URL
    driver.get("https://login.yotpo.com/?product=sms")
    print("Home")

    # Input username & pwd
    username = driver.find_elements_by_xpath(
        '//*[@id="yo-sign-in"]/div/div/div/form/div[1]/input'
    )[0]
    username.send_keys(os.getenv("USERNAME"))
    password = driver.find_elements_by_xpath(
        '//*[@id="yo-sign-in"]/div/div/div/form/div[2]/input'
    )[0]
    password.send_keys(os.getenv("Y_PWD"))
    print("Typed Login")

    # Click login
    login_button = driver.find_elements_by_xpath('//*[@id="login-button"]')[0]
    login_button.click()
    print("Login")

    # Wait for login
    time.sleep(10)

    # Navigate to Report
    driver.get("https://smsbump.yotpo.com/sms/reports")
    print("Navigate to Report")

    # Click generate report
    time.sleep(10)
    generate_report = driver.find_elements_by_xpath(
        "/html/body/communication-app-root/yo-layout/yo-base-layout/div/div/main/div/div[2]/communication-app-react-root/div[1]/main/div/div/div/div/div[2]/div[5]/div/div/button"
    )[0]
    generate_report.click()
    print("Generate Report")

    # Select Last 30 days
    time.sleep(10)
    last_30_days = driver.find_elements_by_xpath(
        '//*[@id="app-content"]/communication-app-react-root/div[1]/div/div/div/div[2]/div/div[1]/div[2]/select/option[3]'
    )[0]
    last_30_days.click()
    print("Selected Time")

    # Save & Export
    save_export = driver.find_elements_by_xpath(
        "/html/body/communication-app-root/yo-layout/yo-base-layout/div/div/main/div/div[2]/communication-app-react-root/div[1]/div/div/div/div[3]/button[2]"
    )[0]
    save_export.click()
    print("Export")

    # Intercept getReport request
    time.sleep(10)
    xhr_requests = [request.url for request in driver.requests if request.response]
    reports_request = [
        request for request in xhr_requests if "reports/getReports" in request
    ]
    print(reports_request)

    driver.quit()
    return reports_request[0]


def get_csv_url(request, attempt=0):
    """Get S3 blob from intercepted getReport request

    Args:
        request (str): getReport request URL
        attempt (int, optional): Recursive attempt. Defaults to 0.

    Raises:
        Exception: Too many attempt

    Returns:
        str: S3 Blob
    """

    with requests.get(request) as r:
        res = r.json()
    if not res["error"]:
        reports = res["data"]["reports"]
        report = sorted(reports, key=lambda x: x["id"], reverse=True)[0]["object_url"]
        assert report
        return report
    else:
        if attempt > 5:
            print(res)
            time.sleep(1)
            return get_csv_url(request, attempt + 1)
        else:
            raise Exception


def get_data(url):
    """Get data from CSV file that we intercepted

    Args:
        url (str): S3 Blob URL

    Returns:
        list: List of row
    """

    with requests.get(url) as r:
        res = r.content
    decoded_content = res.decode("utf-8")
    csv_lines = decoded_content.splitlines()
    cr = csv.DictReader(
        csv_lines[1:],
        fieldnames=csv_lines[0].split(","),
    )
    return [row for row in cr]


def transform(rows):
    """Transform the data to our liking. Transform column to their correct data representation.
    **THIS FUNCTION IS HARD-CODING

    Args:
        rows (list): List of row

    Returns:
        list: List of row transformed
    """

    transform_currency = (
        lambda x: round(float(re.sub(r"[^\d.]", "", x)), 8) if x != "--" else None
    )
    transform_percentage = (
        lambda x: round(float(x.strip("%")) / 100, 8) if x != "--" else None
    )
    transform_date = (
        lambda x: datetime.strptime(x, "%d.%m.%Y").strftime("%Y-%m-%d")
        if x != "--"
        else None
    )
    return [
        {
            "period": transform_date(row["Period"]),
            "total_cost": transform_currency(row["Total Cost"]),
            "total_revenue": transform_currency(row["Total Revenue"]),
            "total_roi": transform_percentage(row["Total ROI"]),
            "campaigns_cost": transform_currency(row["Campaigns Cost"]),
            "campaigns_revenue": transform_currency(row["Campaigns Revenue"]),
            "campaigns_roi": transform_percentage(row["Campaigns ROI"]),
            "flows_cost": transform_currency(row["Flows Cost"]),
            "flows_revenue": transform_currency(row["Flows Revenue"]),
            "flows_roi": transform_percentage(row["Flows ROI"]),
            "automations_cost": transform_currency(row["Automations Cost"]),
            "automations_revenue": transform_currency(row["Automations Revenue"]),
            "automations_roi": transform_percentage(row["Automations ROI"]),
            "_batched_at": NOW.isoformat(timespec="seconds"),
        }
        for row in rows
    ]


def load(rows):
    """Load data to stage table

    Args:
        rows (list): List of row

    Returns:
        int: Output rows
    """
    output_rows = (
        BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{TABLE}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=[
                    {"name": "period", "type": "DATE"},
                    {"name": "total_cost", "type": "NUMERIC"},
                    {"name": "total_revenue", "type": "NUMERIC"},
                    {"name": "total_roi", "type": "NUMERIC"},
                    {"name": "campaigns_cost", "type": "NUMERIC"},
                    {"name": "campaigns_revenue", "type": "NUMERIC"},
                    {"name": "campaigns_roi", "type": "NUMERIC"},
                    {"name": "flows_cost", "type": "NUMERIC"},
                    {"name": "flows_revenue", "type": "NUMERIC"},
                    {"name": "flows_roi", "type": "NUMERIC"},
                    {"name": "automations_cost", "type": "NUMERIC"},
                    {"name": "automations_revenue", "type": "NUMERIC"},
                    {"name": "automations_roi", "type": "NUMERIC"},
                    {"name": "_batched_at", "type": "TIMESTAMP"},
                ],
            ),
        )
        .result()
        .output_rows
    )
    update()
    return output_rows


def update():
    """Update the main table"""

    query = f"""
    CREATE OR REPLACE TABLE {DATASET}.{TABLE} AS
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY period ORDER BY _batched_at)
            AS row_num
        FROM {DATASET}._stage_{TABLE}
    ) WHERE row_num = 1"""
    BQ_CLIENT.query(query).result()


def main(request):
    report_request = get_report_request()
    csv_url = get_csv_url(report_request)
    rows = get_data(csv_url)
    response = {
        "table": TABLE,
        "num_processed": len(rows),
    }
    if len(rows):
        rows = transform(rows)
        response["output_rows"] = load(rows)
    print(response)
    return response
