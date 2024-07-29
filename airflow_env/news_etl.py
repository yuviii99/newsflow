import pandas as pd
import logging
import sqlite3
from newsapi import NewsApiClient
from datetime import datetime, timedelta, time
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import additional libraries

# Initialize NewsApiClient
news_api_key = "88caccdda52746a38028ad11b1d335bf"
news_api = NewsApiClient(api_key=news_api_key)

to_date = datetime.utcnow().date()
from_date  = to_date - timedelta(days=1)

# Intialize DAG object
dag = DAG(
    dag_id="news_etl",
    default_args={'start_date': datetime.combine(from_date, time(0, 0)), 'retries': 1},
    schedule_interval='@daily',
)

def extract_news_data(**kwargs):
    try: 
        result = news_api.get_everything(q="AI", language="en", from_param=from_date, to=to_date)
        logging.info("Connection is successful.")
        # Push the result to the XCom
        kwargs['task_instance'].xcom_push(key='extract_result', value=result["articles"])
    except:
        logging.error("Connection is unsuccessful.")
    
def clean_author_column(text):    
    try:
        return text.split(",")[0].title()
    except AttributeError:
        return "No Author"
    
def transform_news_data(**kwargs):
    article_list = []
    # Add the XCom pull logic to pull data from the XCom
    data = kwargs['task_instance'].xcom_pull(task_ids='extract_news', key='extract_result')

    # Logging message after the XCom pull

    for i in data:
        article_list.append([value.get("name", 0) if key == "source" else value for key, value in i.items() if key in ["author", "title", "publishedAt", "content", "url", "source"]])

    df = pd.DataFrame(article_list, columns=["Source", "Author Name", "News Title", "URL", "Date Published", "Content"])

    df["Date Published"] = pd.to_datetime(df["Date Published"]).dt.strftime('%Y-%m-%d %H:%M:%S')

    df["Author Name"] = df["Author Name"].apply(clean_author_column)
 
    #Add the XCom push logic to push data to the XCom
    kwargs['task_instance'].xcom_push(key='transform_df', value=df.to_json())

def load_news_data(**kwargs):
    with sqlite3.connect("/usercode/etl_news_data.sqlite") as connection:
        # Create a cursor within the context manager
        cursor = connection.cursor()

        # Create a table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS news_table (
                "Source" VARCHAR(30),
                "Author Name" TEXT,
                "News Title" TEXT,
                "URL" TEXT,
                "Date Published" TEXT,
                "Content" TEXT
            )
        ''')

        # Pull data from XCom
        data = kwargs['task_instance'].xcom_pull(task_ids='transform_news', key='transform_df')
     
        # Convert data into a DataFrame
        df = pd.read_json(data)

        # Logging message before loading data
        df.to_sql(name="news_table", con=connection, index=False, if_exists="append")

# Create Python operators

# Create depedencies