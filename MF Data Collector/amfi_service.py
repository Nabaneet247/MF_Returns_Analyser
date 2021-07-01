import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from loguru import logger
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import UnexpectedAlertPresentException, \
    ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from mf_data_provider import get_list_of_all_schemes
from mf_data_saver import update_list_of_all_schemes


@logger.catch
def get_all_fund_house_and_scheme_names(refetch_schemes_list_from_amfi=False, max_tries_per_house=20):
    data = pd.DataFrame()
    if not refetch_schemes_list_from_amfi:
        try:
            return get_list_of_all_schemes()
        except Exception as e:
            logger.error('List of schemes not found. Gonna fetch from AMFI')
    fund_house_names = fetch_all_fund_house_names_from_amfi()
    logger.info('Fund houses found:\n{}', fund_house_names)
    driver = webdriver.Chrome()
    driver.minimize_window()

    index = 0

    for fund_house_name in fund_house_names:
        driver.get('https://www.amfiindia.com/net-asset-value/nav-history')
        fund_house_name_input_tag = driver.find_element_by_xpath(
            '/html/body/div[2]/div[3]/div/form/table[2]/tbody/tr[1]/td/div/span/input')
        fund_house_name_input_tag.clear()
        fund_house_name_input_tag.send_keys(fund_house_name)
        time.sleep(2)
        fund_house_name_input_tag.send_keys(Keys.ARROW_DOWN)
        fund_house_name_input_tag.send_keys(Keys.ENTER)
        scheme_names = []
        tries = 0
        while len(scheme_names) == 0:
            scheme_names = extract_scheme_names_from_html(driver.find_element_by_id('NavHisSCName'), 'NavHisSCName')
            tries += 1
            if tries > max_tries_per_house:
                break
            time.sleep(1)

        for scheme_name in scheme_names:
            data.at[index, 'Fund House Name'] = fund_house_name.strip()
            data.at[index, 'Scheme Name'] = scheme_name.strip()
            index += 1
        if len(scheme_names) == 0:
            logger.warning('{} had {} schemes', fund_house_name, str(len(scheme_names)))
        else:
            logger.info('{} had {} schemes', fund_house_name, str(len(scheme_names)))
    data = data.drop_duplicates()
    logger.info('A total of {} schemes were found on AMFI', len(data.index))
    update_list_of_all_schemes(data)
    return data


@logger.catch
def fetch_all_fund_house_names_from_amfi():
    url = 'https://www.amfiindia.com/net-asset-value/nav-history'
    page = requests.get(url)
    return get_select_tag_texts(page.content, 'id', 'NavHisMFName')[1:]


@logger.catch
def extract_scheme_names_from_html(select_tag, id_value):
    return get_select_tag_texts(select_tag.get_attribute('outerHTML'), 'id', id_value)[1:]


@logger.catch
def get_select_tag_texts(html_snippet, select_tag_identifier_type, select_tag_identifier_value):
    soup = BeautifulSoup(html_snippet, 'html5lib')
    return [str(x.text) for x in
            soup.find('select', attrs={select_tag_identifier_type: select_tag_identifier_value}).findAll('option')]


@logger.catch
def get_html_data_from_amfi(date_string, current_retries=0, retry_limit=3):
    loading_time_limit_in_seconds = 180
    # driver = webdriver.Firefox()
    driver = webdriver.Chrome()
    driver.minimize_window()
    driver.get('https://www.amfiindia.com/net-asset-value/nav-history')

    try:
        start_time = datetime.now()
        WebDriverWait(driver, loading_time_limit_in_seconds).until(EC.presence_of_element_located(
            (By.XPATH, '/html/body/div[2]/div[3]/div/form/table[2]/tbody/tr[1]/td/div/span/input')))
        duration = datetime.now() - start_time
        logger.info('AMFI Page was loaded in {} seconds', duration.total_seconds())

    except TimeoutException:
        logger.critical('Loading AMFI page timed out after {} seconds', loading_time_limit_in_seconds)
        driver.close()
        return  # todo

    try:
        # clicking radio button for 'All NAV's for a date'
        radio_button = driver.find_element_by_xpath(
            '/html/body/div[2]/div[3]/div/form/table[1]/tbody/tr[2]/td[1]/input')
        # driver.execute_script("arguments[0].click();", radio_button)
        radio_button.click()

        date_picker = driver.find_element_by_id('dp1359453629825')
        # date_picker.clear()
        date_picker.send_keys(date_string)
        radio_button.click()  # closes date picker menu

        go_button = driver.find_element_by_xpath('/html/body/div[2]/div[3]/div/form/table[2]/tbody/tr[8]/td/a')
        driver.execute_script("arguments[0].click();", go_button)

    except ElementClickInterceptedException:
        if current_retries < retry_limit:
            current_retries += 1
            logger.warning('Unable to click radio button. Retrying with same parameters....')
            driver.close()
            get_html_data_from_amfi(date_string=date_string, current_retries=current_retries + 1,
                                    retry_limit=retry_limit)
        else:
            logger.error('Got ElementClickInterceptedException')
            logger.error('Got exceptions for more than {} times. Giving up :(', retry_limit)
            driver.close()
            return  # todo
    except Exception as e:
        logger.error('Got unexpected error while fetching data from AMFI: {} - {}. \nGiving up :(', e, type(e))
        driver.close()
        return  # todo

    try:
        start_time = datetime.now()
        WebDriverWait(driver, loading_time_limit_in_seconds).until(
            EC.presence_of_element_located((By.ID, 'divExcelAll')))
        duration = datetime.now() - start_time
        logger.info('AMFI returned data table for date {} in {} seconds', date_string,
                    duration.total_seconds())

    except UnexpectedAlertPresentException:
        if current_retries < retry_limit:
            current_retries += 1
            logger.warning('Got unexpected alert window. Retrying with same parameters....')
            driver.close()
            get_html_data_from_amfi(date_string=date_string, current_retries=current_retries + 1,
                                    retry_limit=retry_limit)
        else:
            logger.error('Got UnexpectedAlertPresentException')
            logger.error('Got exceptions for more than {} times. Giving up :(', retry_limit)
            driver.close()
            return  # todo
    except TimeoutException:
        logger.critical('Loading data for date {} timed out after {} seconds', date_string,
                        loading_time_limit_in_seconds)
        driver.close()
        return  # todo

    html_data = driver.find_element_by_id('divExcelAll').get_attribute('innerHTML')
    driver.close()
    return html_data


if __name__ == '__main__':
    # last fetched on on 14-06-2021
    get_all_fund_house_and_scheme_names(refetch_schemes_list_from_amfi=True)
