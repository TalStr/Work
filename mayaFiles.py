#קסם אקטיב מניות חו"ל - D:\>python3 mayaFiles.py --fund 5112693
#קסם S&P 500 KTF מנוטרלת מט"ח - D:\>python3 mayaFiles.py --fund 5122957
import argparse
import datetime
import time
import os
import glob



parser = argparse.ArgumentParser(description='Download fund data')
parser.add_argument('--fund', type=str, required=True,
                    help='fund number')
args = parser.parse_args()

download_dir = "download"
script_path = os.path.dirname(os.path.realpath(__file__))
full_path = os.path.join(script_path, download_dir)
if not os.path.exists(full_path):
    os.makedirs(full_path)
profile_dir = "profile"
profile_path = os.path.join(script_path, profile_dir)

def downloadFile(fundNum):
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import ElementClickInterceptedException
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    # disable the USB service
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-usb-discovery")
    options.add_argument("--disable-device-discovery-notifications")
    options.add_argument("--log-level=3")
    options.add_argument(f"user-data-dir={profile_path}")
    prefs = {'download.default_directory': full_path}
    options.add_experimental_option('prefs', prefs)

    # instantiate the webdriver object
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()

    # open site
    driver.get(f"https://maya.tase.co.il/fund/{fundNum}?view=history")

    # if pop up close it
    try:
        driver.find_element(By.XPATH, "/html/body/div[1]/div/div/div/button").click()
    except:
        pass
    time.sleep(1)

    # Go to login page, if not logged in
    try:
        parent_element = driver.find_element(By.CSS_SELECTOR,'#wrapper > shared-tase-header > tase-header')
        shadow_root = driver.execute_script('return arguments[0].shadowRoot', parent_element)
        shadow_elements = shadow_root.find_elements(By.CSS_SELECTOR, 'header > div')
        for index, element in enumerate(shadow_elements):
            inner_element = element.find_element(By.XPATH, f".//*[text()='איזור אישי']")
            inner_element.click()
        # Enter login info
        driver.find_element(By.ID, "email-login").send_keys("tal@grfn.co.il")
        driver.find_element(By.ID, "password-login").send_keys("TalTul04")
        # Click login button
        driver.find_element(By.ID, "btn-login").click()
        # Go to history tab
        first_element = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "נתונים היסטוריים"))
        )
        first_element.click()
    except:
        pass

    # Custom time range
    second_element = WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.XPATH, "//*[@id='sidebar-filter-wrapper']/div[3]/div[7]"))
    )
    second_element.click()

    datefrom = driver.find_element(By.ID, "dateFrom")
    datefrom.clear()
    datefrom.send_keys("01-01-2010")
    dateto = driver.find_element(By.ID, "dateTo")
    dateto.clear()
    dateto.send_keys("01-01-2025")
    driver.find_element(By.XPATH, "//*[@id='sidebar-filter-wrapper']/div[7]/button[1]").click()

    button = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".mf-table_icon-03"))
        )

    button_clicked = False
    while not button_clicked:
        try:
            button.click()
            button_clicked = True
        except ElementClickInterceptedException:
            # If an exception occurs, wait for a short time before trying again
            time.sleep(0.5)

    button = WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.LINK_TEXT, "CSV"))
    )
    button.click()

    downloaded_file = None
    while downloaded_file is None:
        time.sleep(1)
        files = glob.glob(os.path.join(full_path, "fundhistory.csv"))
        if files:
            downloaded_file = files[0]
            formatFile(args.fund)


def formatFile(fundNum):
    import pandas as pd
    # Read in full CSV file
    df = pd.read_csv(rf"{full_path}\fundhistory.csv", usecols=[0, 1], names=['date', 'price'],
        header=0, skiprows=1)
    df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y %H:%M:%S').dt.date
    df = df.sort_values(by='date', ascending=True)
    out = pd.DataFrame(columns=["Date", args.fund])
    out['Date'] = df['date']
    out[args.fund] = df['price'].pct_change().fillna(method='bfill').fillna(0).apply(lambda x: f"{x * 100:.10f}%")
    out.to_csv(rf'{fundNum}({datetime.datetime.now().strftime("%Y-%m-%d")}).csv',index=False)
    os.remove(rf"{full_path}\fundhistory.csv")
if __name__ == '__main__':
    downloadFile(args.fund)
