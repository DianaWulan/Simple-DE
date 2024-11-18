import requests
from bs4 import BeautifulSoup
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

# 1. Extract: Web Scraping Data dari Wikipedia
def extract_data():
    print('memulai ekstrak data')
    url = 'https://en.wikipedia.org/wiki/List_of_highest-grossing_films'
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        # Cari tabel yang memiliki class 'wikitable'
        table = soup.find_all('table', {'class': 'wikitable'})[0]

        # Ambil semua baris dari tabel
        rows = table.find_all('tr')

        # Ambil header dari tabel
        headers1 = [header.text.strip() for header in rows[0].find_all('th')]
        print(headers1)

        # Ambil data dari setiap baris tabel
        data = []
        for row in rows[1:]:
            cols = [col.text.strip() for col in row.find_all(['td','th'])]
            if cols:
                data.append(cols)
        print(data)
        
        # Konversi data menjadi DataFrame pandas
        df = pd.DataFrame(data, columns=headers1)
        print("Data berhasil diekstrak dari web.")
        # Simpan data dalam csv
        df.to_csv('List Movie Scrap')

        print('\n',df.head())
        return df
    else:
        print(f"Terjadi kesalahan saat mengakses halaman web: {response.status_code}")
        return 


# 2. Transform: Membersihkan dan mengolah data
def transform_data(df):
    # Ambil kolom judul film, pendapatan, dan tahun rilis
    df_transformed = df[['Title', 'Worldwide gross', 'Year']]

    # Bersihkan data, misalnya dengan menghapus karakter khusus dan mengubah tipe data
    df_transformed['Worldwide gross'] = df_transformed['Worldwide gross'].replace({'\$': '', ',': ''}, regex=True)
    df_transformed['Worldwide gross'] = pd.to_numeric(df_transformed['Worldwide gross'], errors='coerce')
    df_transformed['Year'] = pd.to_numeric(df_transformed['Year'], errors='coerce')

    # Hapus baris dengan nilai NaN
    df_transformed.dropna(inplace=True)
    print("Data berhasil ditransformasi.")
    return df_transformed

# 3. Load: Memuat data ke PostgreSQL
def load_data(df_transformed):
    conn = connect_to_db()
    if conn is not None:
        cur = conn.cursor()

        # Buat tabel jika belum ada
        cur.execute('''
        CREATE TABLE IF NOT EXISTS movies (
            title VARCHAR(255),
            worldwide_gross NUMERIC,
            year INTEGER
        )
        ''')

        # Insert data ke dalam tabel movies
        for index, row in df_transformed.iterrows():
            cur.execute("INSERT INTO movies (title, worldwide_gross, year) VALUES (%s, %s, %s)",
                        (row['Title'], row['Worldwide gross'], row['Year']))

        conn.commit()
        cur.close()
        conn.close()
        print("Data berhasil dimuat ke PostgreSQL.")
    else:
        print("Gagal memuat data ke PostgreSQL.")

# Menjalankan ETL
if __name__ == '__main__':
    # Extract data dari web
    df = extract_data()

    # Jika data berhasil diekstrak, lanjutkan proses ETL
    if df is not None:
        # Transform data (membersihkan dan mengubah format data)
        df_transformed = transform_data(df)

        # Load data yang sudah ditransformasi ke PostgreSQL
        load_data(df_transformed)
