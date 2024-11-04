import pandas as pd
import psycopg2

# Fungsi untuk connect ke PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(
            database="guest",
            user="postgres",
            password="postgres"
        )
        return conn
    except Exception as e:
        print(f"Gagal konek ke database: {e}")
        return None

# 1. Extract: Mengambil data dari tabel 'employees'
def extract_data():
    conn = connect_to_db()
    query = "SELECT * FROM employees"
    df = pd.read_sql(query, conn)
    conn.close()
    print("Data berhasil diekstrak dari PostgreSQL.")
    return df

# 2. Transform: Menghitung gaji tahunan dan membuat kolom baru
def transform_data(df):
    df['annual_salary'] = df['monthly_salary'] * 12
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    df_transformed = df[['full_name', 'annual_salary']]
    print("Data berhasil ditransformasi.")
    return df_transformed

# 3. Load: Menyimpan data yang sudah ditransformasi ke tabel baru 'employees_transformed'
def load_data(df_transformed):
    conn = connect_to_db()
    cur = conn.cursor()

    # Buat tabel 'employees_transformed' jika belum ada
    cur.execute('''
    CREATE TABLE IF NOT EXISTS employees_transformed (
        full_name VARCHAR(100),
        annual_salary NUMERIC
    )
    ''')

    # Insert data ke tabel 'employees_transformed'
    for index, row in df_transformed.iterrows():
        cur.execute("INSERT INTO employees_transformed (full_name, annual_salary) VALUES (%s, %s)",
                    (row['full_name'], row['annual_salary']))

    conn.commit()
    cur.close()
    conn.close()
    print("Data berhasil dimuat ke PostgreSQL ke tabel 'employees_transformed'.")

# Menjalankan ETL
if __name__ == '__main__':
    # Extract data dari PostgreSQL
    df = extract_data()

    # Transform data (hitungan gaji tahunan dan membuat kolom full name)
    df_transformed = transform_data(df)

    # Load data yang sudah ditransformasi ke PostgreSQL
    load_data(df_transformed)
