# imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import datetime

from bs4 import BeautifulSoup

# selenium will be used to scrap dynamic content of the webpage, our data source of our data
from selenium import webdriver
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import json


url= "https://cycling.data.tfl.gov.uk"
dictionary_file= "links_dictionary.json"

def contents_downloader(**kwargs):
    cap = DesiredCapabilities().FIREFOX
    cap["marionette"] = False

    options = FirefoxOptions()
    options = webdriver.FirefoxOptions()
    options.log.level = "TRACE"
    options.add_argument('--no-sandbox')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    
    browser = webdriver.Firefox(capabilities=cap, executable_path=GeckoDriverManager().install(), options=options)
    browser.get(url)

    # wait until at least a single element of the table exists
    wait = WebDriverWait(browser, 20)
    wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/table/tbody/tr[1]/td[1]')))
    content= browser.page_source
    
    kwargs['ti'].xcom_push(key='html_content', value=content)
    

def links_extractor(**kwargs):
    task_instance= kwargs['ti']
    html_element= task_instance.xcom_pull(key='html_content', task_ids='download_contents_task')
    
    bsoup= BeautifulSoup(html_element, "html.parser")

    table= bsoup.find('table')
    tbody= table.find('tbody')
    folder_name= "usage-stats/"
    capture_files= False
    year= 2021
    filetype= 'csv'
    extracted_files= {}

    for row in tbody.find_all('tr'):
        columns= row.find_all('td')

        if capture_files == False:
            col_values= [col.text.strip() for col in columns]

            if col_values[0] == folder_name:
                capture_files= True
                continue

        else:
            col= columns[0]
            filename= col.text.strip()
            
            if not filename.endswith(f'{year}.{filetype}'):
                continue
            
            # extract the date (e.g 257JourneyDataExtract17Mar2021-23Mar2021.csv --> 23Mar2021)
            filename_without_extension= filename.replace(f'.{filetype}', '') 
            filename_last_date= filename_without_extension.split('-')[-1]
            extracted_files[filename_last_date]= col.a['href']
    
    kwargs['ti'].xcom_push(key="dictionary", value=extracted_files)


def dico_exporter(**kwargs):
    task_instance= kwargs['ti']
    links_dictionary= task_instance.xcom_pull(key="dictionary", task_ids="extract_links_task")
    
    # serialize json 
    links_json_object = json.dumps(links_dictionary, indent = 4)

    # save into a dico file
    with open(dictionary_file, 'w', encoding='utf-8') as f:
        f.write(links_json_object)



''' 
    TODO: Set the start date of the dag to the last Tuesday,
        this way the dag will run only once before its next running schedule 
        defined by the schedule_interval
        e.g Today is Wed 16th February 2022, the start is datetime(2022, 1, 15)
'''
default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 15), 
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="web_scraping_dag",
    schedule_interval="50 23 * * 2", # every Tuesday at 11:50pm
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['web', 'scraping', 'links', 'source'],
) as dag:

    download_web_contents_task = PythonOperator(
        task_id="download_contents_task",
        provide_context=True,
        python_callable=contents_downloader
    )

    extract_links_task = PythonOperator(
        task_id="extract_links_task",
        provide_context=True,
        python_callable=links_extractor,

    )

    export_links_task = PythonOperator(
        task_id="exporter_links_task",
        provide_context=True,
        python_callable=dico_exporter 
    )


    download_web_contents_task >> extract_links_task >> export_links_task