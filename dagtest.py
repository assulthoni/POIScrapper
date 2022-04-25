import os
import ast
import time
import psutil
from selenium import webdriver
from itertools import zip_longest
import pandas as pd
import random
from dotenv import load_dotenv
from tqdm import tqdm
import sqlalchemy as sa
random.seed(47)
load_dotenv()

POIs = ["school", "restaurant", "mall", "office", "mart", "halte", "mosque", "gas station"]

def parse_url(latitude, longitude, poi):
    zoom = "16z" # this is zoom level set to 200 m radius
    base_url = "https://maps.google.com/maps/search/"
    url = base_url + poi.replace(' ', '+') + "/"
    latlong = f"@{latitude},{longitude},{zoom}/"
    return url + latlong + '?hl=en'

def create_browser(headless=True, sandbox=False, disable_dev_shm=False):
    """
    return : chromedriver browser selenium
    """
    try:
        CHROME_PATH = os.getenv('CHROME_PATH')
        prefs = {
            "profile.managed_default_content_settings.images" : 2,
            # "profile.default_content_setting_values.geolocation" : 2
        }
        options = webdriver.ChromeOptions()
        
        options.add_experimental_option("prefs", prefs)
        if headless:
            options.add_argument('--headless')
        if sandbox:
            options.add_argument('--no-sandbox')
        if disable_dev_shm:
            options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--ignore-certificate-errors')
        browser = webdriver.Chrome(CHROME_PATH, options=options)
        return browser
    except Exception as e:
        print(e)
        return None

def kill_chrome():
    PROCNAME = "chromedriver" # or chromedriver or iedriverserver
    BROWSERNAME = "chrome"
    for proc in psutil.process_iter():
        # check whether the process name matches
        if proc.name() == PROCNAME or proc.name() == BROWSERNAME:
            print(f"kill the {PROCNAME} | {BROWSERNAME} process")
            proc.kill()

def zip_name_review(names, reviews, poi):
    list_data = []
    name_review = list(zip_longest(names, reviews))
    # print(name_review)
    for name, review in name_review:
        data = {}
        data['poi'] = poi
        data['name'] = name
        if not review:
            data['number_review'] = 0
            data['number_rating'] = 0
        else:
            review = review.strip()
            num_review = review[review.find("(")+1:review.find(")")]
            num_rating = review.replace(f'({num_review})', '')
            num_review = change_to_number(num_review)
            num_rating = change_to_number(num_rating)
            data['number_review'] = num_review if num_review else 0
            data['number_rating'] = num_rating if num_rating else 0
        # print(data)
        list_data.append(data)
    return list_data

def change_to_number(str_num):
    try:
        str_num = str_num.replace(',', '')
        return ast.literal_eval(str_num)
    except Exception as e:
        return None

def change_zoom(browser):
    change_js = """
    var selectBox = document.querySelector("settings-ui").shadowRoot.querySelector("#main").shadowRoot.querySelector("settings-basic-page").shadowRoot.querySelector("settings-appearance-page").shadowRoot.querySelector("#zoomLevel");
    var changeEvent = new Event("change");
    selectBox.value = arguments[0];
    selectBox.dispatchEvent(changeEvent);
    """
    browser.get("chrome://settings/")
    new_zoom = round(25 / 100, 2)
    browser.execute_script(change_js, new_zoom)
    return browser

def calculate_score(num_rating, num_review):
    if num_rating == 0:
        num_rating = 1
    if num_review == 0:
        num_review = 1
    avg_total = (num_review / 5 ) / (num_review / num_rating) * (num_review)
    return avg_total

def process(lat, long):
    headless = True
    browser = create_browser(headless)
    if not headless:
        browser = change_zoom(browser)
    all_data = []
    for poi in POIs:
        url = parse_url(lat, long, poi)
        browser.get(url)
        time.sleep(2)   
        names = browser.find_elements_by_class_name(os.getenv('CLASS_NAMES','qBF1Pd-haAclf'))
        reviews = browser.find_elements_by_class_name(os.getenv('CLASS_REVIEWS','OEvfgc-wcwwM-haAclf'))
        names = [name.text for name in names]
        reviews = [review.text for review in reviews]
        list_data = zip_name_review(names, reviews, poi)
        print(list_data)
        all_data.extend(list_data)
    browser.close()
    print(all_data)
    return all_data

def scrape_poi_billboard(lat, long,id=None, **kwargs):
    SQL_CONN_STRING='XXXXX' #change mysql conn string here
    engine = sa.create_engine(SQL_CONN_STRING)
    if lat is None or long is None:
        lat = kwargs['dag_run'].conf.get('lat', None)
        long = kwargs['dag_run'].conf.get('long', None) 
        id = kwargs['billboard_id'].conf.get('billboard_id', None)
    data = process(lat, long)
    df = pd.DataFrame(data)
    print(df.head())
    df['latBillboard'] = lat
    df['lonBillboard'] = long
    df['idBillboard'] = id
    df['avg_score'] = df[['number_rating', 'number_review']].apply(lambda rec: calculate_score(rec['number_rating'], rec['number_review']), axis=1)
    df.to_sql("poi_billboards", engine, if_exists="append")
    
def get_billboard():
    SQL_CONN_STRING='XYXYYXYX' # change mysql conn here
    engine = sa.create_engine(SQL_CONN_STRING)
    df_billboard = pd.read_sql_table('billboards', engine)
    for idx, row in df_billboard.iterrows():
        if row['id'] <= 48:
            continue
        try:
            scrape_poi_billboard(row['latitude1'], row['longitude1'], row['id'])
        except:
            continue
default_args = {
		'owner' : 'stickflow',
		'depends_on_past' :True,
		'email' :['ahmad.sulthoni@stickearn.com'],
		'email_on_failure': True,
		'email_on_retry': False,
		'retries': 3,
		'retry_delay': timedelta(minutes=5)
		}

dag = DAG("gmaps_poi_scraper",
    description="DAG to scrape POI with selenium",
    default_args=default_args,
	start_date=datetime(2021,9,1),
    schedule_interval=None,
    tags=['scrapper', 'poi', 'gmaps'],
    on_success_callback=cleanup_xcom)

with dag:
	t1 = DummyOperator(
		task_id='dummy_start'
	)
	t2 = PythonOperator(
		python_callable=scrape_poi_billboard,
		task_id='run_scrapper'
	)
	t1 >> t2

