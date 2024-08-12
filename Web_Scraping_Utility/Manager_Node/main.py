import pandas as pd
import requests
import os
import datetime
from fastapi import FastAPI, BackgroundTasks
import uvicorn
from sqlalchemy import create_engine,text
from dotenv import load_dotenv

load_dotenv()

pwd_value = str(os.environ.get('MSSQLS_PWD'))
pwd_str =f"Pwd={pwd_value};"
global conn
conn = "DRIVER={ODBC Driver 17 for SQL Server};Server=35.172.243.170;Database=luxurymarket_p4;Uid=luxurysitescraper;" + pwd_str
global engine
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % conn)

app = FastAPI()



SETTINGS_URL = "https://raw.githubusercontent.com/samuelshlyam/HTML_Gather_Settings/dev/settings.json"
current_directory = os.getcwd()

def fetch_settings():
    try:
        response = requests.get(os.getenv("SETTINGS_URL"))
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching settings: {e}")
        return None


# @app.post("/fetch_html")
# async def brand_batch_endpoint(brand_id: str, background_tasks: BackgroundTasks):
#     background_tasks.add_task(process_brand_batch, brand_id)
#     return {"message": "Notification sent in the background"}
def update_sql_job(job_id, resultUrl,logUrl,count):
    sql=(f"Update utb_BrandScanJobs Set ResultURL = '{resultUrl}',\n"
    f"HarverstLogURL = '{logUrl}',\n"
    f"HarvestCount =  {count},\n"
    f" HarvestEnd = getdate()\n"
    f" Where ID = {job_id}")
    if len(sql) > 0:
        ip = requests.get('https://api.ipify.org').content.decode('utf8')
        print('My public IP address is: {}'.format(ip))
        
        connection = engine.connect()
        sql = text(sql)
        print(sql)
        connection.execute(sql)
        connection.commit()
        connection.close()




@app.post("/job_complete")
async def brand_batch_endpoint(job_id: str, resultUrl:str,logUrl:str,count:int,background_tasks: BackgroundTasks):
    background_tasks.add_task(update_sql_job, job_id, resultUrl ,logUrl ,count)

    return {"message": "Notification sent in the background"}


@app.post("/submit_job")
async def brand_single(job_id: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(procces_brand_single, job_id)

    return {"message": "Notification sent in the background"}


def procces_brand_single(job_id):
    df=fetch_job_details(job_id)
    url = df.iloc[0, 1]
    brand_id = str(df.iloc[0,0])
    print(brand_id,url)
    print(f"Processing job {job_id} with URL: {url}")
    
    response_status = submit_job_post(job_id,brand_id,url)



    print(f"send request with {job_id}, Status: {response_status}")
    
def fetch_job_details(job_id):
    sql_query = f"Select BrandId, ScanUrl from utb_BrandScanJobs where ID = {job_id}"
    print(sql_query)
    df = pd.read_sql_query(sql_query, con=engine)
    engine.dispose()
    print(df)
    return df

def submit_job_post(job_id,brand_id,url):

    headers = {
        'accept': 'application/json',
        'content-type': 'application/x-www-form-urlencoded',
    }

    params = {
        'job_id': f"{job_id}",
        'brand_id':f"{brand_id}",
        'scan_url':f"{url}",
    }

    response = requests.post(f"{os.environ.get('AGENT_BASE_URL')}/run_html", params=params, headers=headers)
    return response.status_code

#KEEP
# def process_brand_batch(brand_id):
#     jsonData = fetch_settings()
#     data = jsonData.get(brand_id)
#     URL_LIST = data.get('URL_LIST')
#     brand_name = data.get('DIRECTORY')
#     time_stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
#     output_dir = os.path.join(current_directory, 'Outputs', brand_name, time_stamp)
#     print(f"Number of URLs: {len(URL_LIST)}")
#
#     os.makedirs(output_dir, exist_ok=True)
#
#     expanders = [PageExpander.remote(brand_id, url, output_dir) for url in URL_LIST]
#     results = ray.get([expander.start.remote() for expander in expanders])
#     print(results)

# start = datetime.datetime.now()
#
# # Fetch settings from the GitHub URL
#
#
# if jsonData is None:
#     print("Failed to fetch settings. Exiting.")
# else:
#     # Get the brand ID from user input
#     brand_id = input("Enter the brand ID to process: ")
#
#     if brand_id not in jsonData:
#         print(f"Brand ID '{brand_id}' not found in settings.")
#     else:
#         process_brand(expander, brand_id, jsonData[brand_id])
#
# end = datetime.datetime.now()
# print(f"\nProcessing completed.\nTime taken: {end - start}")

if __name__ == "__main__":
<<<<<<< HEAD
    uvicorn.run("main:app", port=8080, host="0.0.0.0" ,log_level="info")
=======
    uvicorn.run("main:app", port=8101, log_level="info")
>>>>>>> d17427f9c06e5637b770702550f7dbff1d742823
