from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os


def extract_data(ti):

    # 1. extract masing-masing file csv
    # Path ke folder yang berisi file CSV
    folder_path = "/opt/airflow/dags/dataku" 

    # List untuk menyimpan DataFrame sementara
    dataframes_list = []

    # Loop melalui semua file dalam folder
    for filename in os.listdir(folder_path):
        # Cek apakah file berakhiran .csv
        if filename.endswith('.csv'):
            # Path lengkap ke file
            file_path = os.path.join(folder_path, filename)
        
            # Baca file CSV ke DataFrame
            df = pd.read_csv(file_path)
        
            # Tambahkan DataFrame ke dalam list
            dataframes_list.append(df)

            # Tampilkan informasi
            #print(f"File '{filename}' berhasil dimuat. Kolom: {list(df.columns)}")

    # Gabungkan semua DataFrame dengan kolom lengkap
    combined_df = pd.concat(dataframes_list, ignore_index=True, sort=False)

    # Simpan
    #combined_df.to_csv("combined_data.csv", index=False)
    print(combined_df.head())

    # push to next func
    ti.xcom_push(key='combine_df', value=combined_df.to_json())


def transform_data(ti):
    df_json = ti.xcom_pull(key='combine_df', task_ids = 'extract_task')
    df = pd.read_json(df_json)

    # mengisi kolom country yang kosong 
    df['country'] = df['country'].fillna('Indonesia')
    df['status'] = df['status'].fillna('FullTime')

    # ambil kolom yang dibutuhkan saja
    dfg = df[['first_name', 'last_name', 'month_salary', 'position', 'status', 'country']]

    ti.xcom_push(key='combine_dfg', value=dfg.to_json())


def load_data(ti):
    df_json = ti.xcom_pull(key='combine_dfg', task_ids='transform_task')
    df = pd.read_json(df_json)
    df.columns = df.columns.str.lower()
    
    # Get the PostgreSQL connection using PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='dbwarehouse')
    conn = postgres_hook.get_conn()
    
    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(225),
                last_name VARCHAR(225),
                month_salary INT,
                position VARCHAR(225),
                status VARCHAR(100),
                country VARCHAR(100)
            );
        """)

        #cur.execute("DELETE FROM spotify_tr;")
        conn.commit()

        # Using SQLAlchemy engine from PostgresHook
        engine = postgres_hook.get_sqlalchemy_engine()
        df.to_sql('employees', con=engine, if_exists='append', index=False)
    
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    
    finally:
        cur.close()
        conn.close()


default_args = {
    'owner' : 'diana',
    'start_date': datetime(2023,1,1),
    'retries': 1
}

dag = DAG(
    'etl-multicsv',
    default_args = default_args,
    description = "extract data from multi-csv",
    schedule_interval='@daily',
    max_active_runs = 1,
    catchup = False
)


extract_task = PythonOperator( 
    task_id = 'extract_task',
    python_callable = extract_data,
    dag=dag

)

transform_task = PythonOperator( 
    task_id = 'transform_task',
    python_callable = transform_data,
    dag=dag

)

load_task = PythonOperator( 
    task_id = 'load_task',
    python_callable = load_data,
    dag=dag

)

extract_task >> transform_task >> load_task