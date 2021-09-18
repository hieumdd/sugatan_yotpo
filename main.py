import os
import json
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
TABLE = "CampaignPerformance"

NOW = datetime.utcnow()


def get_csv_url():
    """Get & intercept CSV request from their FE to BE

    Returns:
        string: S3 Blob URL
    """

    driver = webdriver.Chrome("./chromedriver", options=CHROME_OPTIONS)
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
    report_menu_item = driver.find_elements_by_xpath('//*[@id="menu"]/li[9]/a/span[2]')[
        0
    ]
    report_menu_item.click()

    # Click export icon
    export_icon = driver.find_elements_by_xpath(
        '//*[@id="layout-parent"]/div/div/div/div[3]/div/div/div/div/div[2]/div/table/tbody/tr[1]/td[5]/div/button[1]'
    )[0]
    export_icon.click()

    # Wait for XHR request
    time.sleep(5)

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
        fieldnames=(
            "Campaign Name",
            "Scheduled date",
            "Revenue",
            "Cost",
            "ROI",
            "Sent Msgs",
            "Clicks",
            "CTR",
            "CVR",
            "Orders",
            "AOV",
            "Unsubs %",
        ),
    )
    return [row for row in cr]


def transform(rows):
    """Transform the data to ouR liking. Transform column to their correct data representation. THIS FUNCTION IS HARD-CODING

    Args:
        rows (list): List of row

    Returns:
        list: List of row transformed
    """

    transform_currency = lambda x: float(re.sub(r"[^\d.]", "", x))
    transform_percentage = lambda x: float(x.strip("%")) / 100 if x != "--" else None
    transform_datetime = lambda x: datetime.strptime(x, "%B %d %Y %H:%M").isoformat(
        timespec="seconds"
    )
    return [
        {
            "campaign_name": row["Campaign Name"],
            "scheduled_date": transform_datetime(row["Scheduled date"]),
            "revenue": transform_currency(row["Revenue"]),
            "cost": transform_currency(row["Cost"]),
            "roi": transform_percentage(row["ROI"]),
            "sent_msgs": int(row["Sent Msgs"]),
            "clicks": row["Clicks"],
            "ctr": transform_percentage(row["CTR"]),
            "cvr": transform_percentage(row["CVR"]),
            "orders": int(row["Orders"]),
            "aov": transform_currency(row["AOV"]),
            "unsubs_rate": transform_percentage(row["Unsubs %"]),
            "_batched_at": NOW.isoformat(timespec='seconds'),
        }
        for row in rows
    ]


def load(rows):
    return BQ_CLIENT.load_table_from_json(
        rows,
        f"{DATASET}._stage_{TABLE}",
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            schema=[
                {"name": "campaign_name", "type": "STRING"},
                {"name": "scheduled_date", "type": "DATETIME"},
                {"name": "revenue", "type": "FLOAT"},
                {"name": "cost", "type": "FLOAT"},
                {"name": "roi", "type": "FLOAT"},
                {"name": "sent_msgs", "type": "INTEGER"},
                {"name": "clicks", "type": "STRING"},
                {"name": "ctr", "type": "FLOAT"},
                {"name": "cvr", "type": "FLOAT"},
                {"name": "orders", "type": "INTEGER"},
                {"name": "aov", "type": "FLOAT"},
                {"name": "unsubs_rate", "type": "FLOAT"},
                {"name": "_batched_at", "type": "TIMESTAMP"},
            ],
        ),
    ).result().output_rows

def update():
    query = f"""
    CREATE OR REPLACE TABLB {DATASET}.{TABLE} AS
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY campaign_name, scheduled_date ORDER BY _batched_at)
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
        response['output_rows'] = load(rows)
    return response


main({})
