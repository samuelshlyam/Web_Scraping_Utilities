import pandas as pd
import requests
import os
import traceback
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail,Personalization,To,Cc
import datetime
from fastapi import FastAPI, BackgroundTasks
import uvicorn
from sqlalchemy import create_engine,text


pwd_value = str(os.environ.get('MSSQLS_PWD'))
pwd_str =f"Pwd={pwd_value};"
global conn
conn = "DRIVER={ODBC Driver 17 for SQL Server};Server=35.172.243.170;Database=luxurymarket_p4;Uid=luxurysitescraper;" + pwd_str
global engine
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % conn)

app = FastAPI()

current_directory = os.getcwd()

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
    if response_status != 200:
        send_email(f"Request with job id {job_id} failed to send, Status: {response_status}")


def send_email(message_text, to_emails='nik@iconluxurygroup.com', subject="Error - HTML Step"):
    message_with_breaks = message_text.replace("\n", "<br>")

    html_content = f"""
                    <html>
                    <body>
                    <div class="container">
                        <!-- Use the modified message with <br> for line breaks -->
                        <p>Message details:<br>{message_with_breaks}</p>
                    </div>
                    </body>
                    </html>
                    """
    message = Mail(
        from_email='distrotool@iconluxurygroup.com',
        subject=subject,
        html_content=html_content
    )

    cc_recipient = 'notifications@popovtech.com'
    personalization = Personalization()
    personalization.add_cc(Cc(cc_recipient))
    personalization.add_to(To(to_emails))
    message.add_personalization(personalization)
    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(e)
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

if __name__ == "__main__":
    uvicorn.run("main:app", port=8080, host="0.0.0.0" ,log_level="info")

