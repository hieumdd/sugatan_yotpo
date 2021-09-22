import os
import csv
import time
import re
from datetime import datetime

from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
import requests
from google.cloud import bigquery

URL = "https://login.yotpo.com/?product=sms"

CHROME_OPTIONS = Options()
# if os.getenv("PYTHON_ENV") == "prod":
CHROME_OPTIONS.add_argument("--headless")
CHROME_OPTIONS.add_argument("--no-sandbox")
CHROME_OPTIONS.add_argument("--window-size=1366,768")
CHROME_OPTIONS.add_argument("--disable-gpu")
CHROME_OPTIONS.add_argument("--disable-dev-shm-usage")
CHROME_OPTIONS.add_argument(f"user-agent={UserAgent().random}")
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


def get_csv_url():
    """Get & intercept CSV request from their FE to BE

    Returns:
        string: S3 Blob URL
    """
    if os.getenv("PYTHON_ENV") == "dev":
        driver = webdriver.Chrome("./chromedriver", options=CHROME_OPTIONS)
    else:
        driver = webdriver.Chrome(options=CHROME_OPTIONS)
    driver.implicitly_wait(20)

    # Navtigate to URL
    driver.get(URL)

    # Input username & pwd
    username = driver.find_elements_by_xpath(
        '//*[@id="yo-sign-in"]/div/div/div/form/div[1]/input'
    )[0]
    username.send_keys(os.getenv("USERNAME"))
    password = driver.find_elements_by_xpath(
        '//*[@id="yo-sign-in"]/div/div/div/form/div[2]/input'
    )[0]
    password.send_keys(os.getenv("Y_PWD"))

    # Click login
    login_button = driver.find_elements_by_xpath('//*[@id="login-button"]')[0]
    login_button.click()

    # Navigate to Report
    time.sleep(5)
    report_menu_item = driver.find_elements_by_xpath('//*[@id="menu"]/li[9]/a/span[2]')[
        0
    ]
    report_menu_item.click()

    # Click generate report
    generate_report = driver.find_elements_by_xpath(
        "/html/body/communication-app-root/yo-layout/yo-base-layout/div/div/main/div/div[2]/communication-app-react-root/div[1]/main/div/div/div/div/div[2]/div[5]/div/div/button"
    )[0]
    generate_report.click()

    # Select Last 30 days
    last_30_days = driver.find_elements_by_xpath(
        '//*[@id="app-content"]/communication-app-react-root/div[1]/div/div/div/div[2]/div/div[1]/div[2]/select/option[3]'
    )[0]
    last_30_days.click()

    # Save & Export
    save_export = driver.find_elements_by_xpath(
        "/html/body/communication-app-root/yo-layout/yo-base-layout/div/div/main/div/div[2]/communication-app-react-root/div[1]/div/div/div/div[3]/button[2]"
    )[0]
    save_export.click()

    # Wait for export
    time.sleep(30)

    # Click export icon
    export_icon = driver.find_elements_by_xpath(
        '//*[@id="layout-parent"]/div/div/div/div[3]/div/div/div/div/div[2]/div/table/tbody/tr[1]/td[5]/div/button[1]'
    )[0]
    export_icon.click()

    # Wait for XHR request
    time.sleep(10)

    # Intercept requests
    xhr_requests = [request.url for request in driver.requests if request.response]

    # Get CSV from their backend response
    csv_url = [i for i in xhr_requests if "smsbump.s3" in i][0]
    driver.quit()
    return csv_url


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
    """Transform the data to ouR liking. Transform column to their correct data representation. **THIS FUNCTION IS HARD-CODING

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
    transform_datetime = lambda x: datetime.strptime(x, "%B %d %Y %H:%M").isoformat(
        timespec="seconds"
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
    csv_url = get_csv_url()
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
