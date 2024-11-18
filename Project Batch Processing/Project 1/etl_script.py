import os
import pandas as pd
import psycopg2
from psycopg2 import sql

# Konfigurasi database PostgreSQL
DB_HOST = 'localhost'
DB_NAME = 'mydatabase'
DB_USER = 'myuser'
DB_PASS = 'mypassword'

# Membaca data dari file CSV
#data = pd.read_csv('/dataku/sample_data.csv')
# Menggunakan path absolut untuk debugging
file_path = os.path.join(os.path.dirname(__file__), 'dataku', 'sample_data.csv')
print(f"Path ke file CSV: {file_path}")

data = pd.read_csv(file_path)

# Fungsi untuk memuat data ke PostgreSQL
def load_data(df):
    try:
        # Membuat koneksi ke database PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()

        # Membuat tabel jika belum ada
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sample_data (
                id SERIAL PRIMARY KEY,
                column1 TEXT,
                column2 TEXT
            )
        """)

        # Memasukkan data ke dalam tabel
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO sample_data (column1, column2)
                VALUES (%s, %s)
            """, (row['column1'], row['column2']))

        # Commit perubahan dan tutup koneksi
        conn.commit()
        cursor.close()
        conn.close()
        print("Data berhasil dimuat ke database")
    except Exception as e:
        print(f"Terjadi kesalahan: {e}")

# Memuat data
load_data(data)
