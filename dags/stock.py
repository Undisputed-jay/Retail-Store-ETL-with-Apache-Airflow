from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datacleaner import data_cleaner
import csv
import mysql.connector

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_arguments = {
    'owner': 'Ahmed Ayodele',
    'start_date': datetime(2023, 6, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def load_data_to_mysql():
    conn = mysql.connector.connect(
        host='mysql',
        user='airflow',
        password='airflow',
        database='db'
    )
    cursor = conn.cursor()
    
    with open('/opt/airflow/data/clean_store_transactions.csv', 'r') as file:
        csv_data = csv.reader(file)
        next(csv_data)  # Skip header row
        for row in csv_data:
            cursor.execute("INSERT INTO clean_store_transactions (STORE_ID, STORE_LOCATION, PRODUCT_CATEGORY, "
                           "PRODUCT_ID, MRP, CP, DISCOUNT, SP, DATE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                           row)
    
    conn.commit()
    cursor.close()
    conn.close()

def generate_profit_report():
    conn = mysql.connector.connect(
        host='mysql',
        user='airflow',
        password='airflow',
        database='db'
    )
    cursor = conn.cursor()

    # Query for location-wise profit transactions
    location_query = """
    SELECT `DATE`, STORE_LOCATION, ROUND((SUM(SP) - SUM(CP)), 2) AS location_profit
    FROM clean_store_transactions
    GROUP BY `DATE`, STORE_LOCATION
    ORDER BY location_profit DESC;
    """

    cursor.execute(location_query)
    location_rows = cursor.fetchall()

    # Generate location-wise profit transactions CSV
    location_file_path = '/opt/airflow/data/locationwise_profit_transactions.csv'
    with open(location_file_path, 'w', newline='') as location_file:
        csv_writer = csv.writer(location_file)
        csv_writer.writerow([i[0] for i in cursor.description])  # Write header row
        csv_writer.writerows(location_rows)

    # Query for store-wise profit transactions
    store_query = """
    SELECT `DATE`, STORE_ID, ROUND((SUM(SP) - SUM(CP)), 2) AS store_profit
    FROM clean_store_transactions
    GROUP BY `DATE`, STORE_ID
    ORDER BY store_profit DESC;
    """

    cursor.execute(store_query)
    store_rows = cursor.fetchall()

    # Generate store-wise profit transactions CSV
    store_file_path = '/opt/airflow/data/store_profit_transactions.csv'
    with open(store_file_path, 'w', newline='') as store_file:
        csv_writer = csv.writer(store_file)
        csv_writer.writerow([i[0] for i in cursor.description])  # Write header row
        csv_writer.writerows(store_rows)

    cursor.close()
    conn.close()


with DAG(
    'store_dag',
    default_args=default_arguments,
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id='check_if_file_exists',
        bash_command='shasum /opt/airflow/data/raw_store_transactions.csv',
        retries=2,
        retry_delay=timedelta(seconds=15)
    )

    t2 = PythonOperator(
        task_id='clean_csv',
        python_callable=data_cleaner
    )

    t3 = PythonOperator(
        task_id='create_table',
        python_callable=load_data_to_mysql
    )

    t4 = PythonOperator(
        task_id='load_data_to_mysql',
        python_callable=load_data_to_mysql
    )

    t5 = PythonOperator(
        task_id='generate_profit_report',
        python_callable=generate_profit_report
    )
    
    # to keep a log of files, incase we have multiple reports, this move the file and attache the date of yesterday to the file
    t6 = BashOperator(
        task_id = 'move_file1',
        bash_command = 
        'cat /opt/airflow/data/locationwise_profit_transactions.csv && mv /opt/airflow/data/locationwise_profit_transactions.csv /opt/airflow/data/locationwise_profit_transactions_%s.csv'
        % yesterday_date
    )
    
    t7 = BashOperator(
        task_id = 'move_file2',
        bash_command = 
        'cat /opt/airflow/data/store_profit_transactions.csv && mv /opt/airflow/data/store_profit_transactions.csv /opt/airflow/data/store_profit_transactions_%s.csv'
        % yesterday_date
    )
    
    
    t8 = EmailOperator(
        task_id = 'send_email',
        to = 'bayodele73@gmail.com',
        subject = 'Daily Report Generated',
        html_content = """ <hi> Congratulations! Your Store reports are ready.</h1> """,
        files = ['/opt/airflow/data/locationwise_profit_transactions_%s.csv' % yesterday_date, '/opt/airflow/data/store_profit_transactions_%s.csv' % yesterday_date]
    )
    # Send Email, this wont send because we dont have files for yesterday
    # t8 = EmailOperator(
        # task_id = 'send_email',
        # to = 'bayodele73@gmail.com',
        # subject = 'Daily Report Generated',
        # html_content = """ <hi> Congratulations! Your Store reports are ready.</h1> """,
        # files = ['/opt/airflow/data/locationwise_profit_transactions_%s.csv'% yesterday_date, '/opt/airflow/data/store_profit_transactions_%s.csv' % yesterday_date])

t1 >> t2 >> t3 >> t4 >> t5 >> [t6, t7] >> t8


