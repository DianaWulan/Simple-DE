import requests
import pandas as pd
import psycopg2

# 1. Extract: Ambil data dari API
response = requests.get("https://jsonplaceholder.typicode.com/users")
data = response.json()

# Convert data dari JSON ke pandas DataFrame
df = pd.DataFrame(data)

# Simpan data mentah ke file CSV sebagai "backup" atau bagian dari data lake
df.to_csv('data_lake/users_raw.csv', index=False)
print("Data mentah berhasil diambil dari API dan disimpan ke data_lake/users_raw.csv")

# 2. Transform: Membersihkan dan memilih data
# Misalnya, kita hanya mengambil kolom name, email, dan city dari alamat pengguna
df_transformed = df[['name', 'email']]
df_transformed['city'] = df['address'].apply(lambda x: x['city'])

# Simpan data yang sudah ditransformasi
df_transformed.to_csv('data_lake/users_clean.csv', index=False)
print("Data berhasil ditransformasi dan disimpan di data_lake/users_clean.csv")

# 3. Load: Memuat data yang sudah bersih ke PostgreSQL
try:
    conn = psycopg2.connect(
        database="guest",
        user="postgres",
        password="postgres"
    )

    cur = conn.cursor()

    # Buat tabel jika belum ada
    cur.execute('''
    CREATE TABLE IF NOT EXISTS users (
        name VARCHAR(100),
        email VARCHAR(100),
        city VARCHAR(100)
    )
    ''')

    # Insert data ke dalam tabel users
    for index, row in df_transformed.iterrows():
        cur.execute("INSERT INTO users (name, email, city) VALUES (%s, %s, %s)",
                    (row['name'], row['email'], row['city']))

    # Commit perubahan
    conn.commit()
    cur.close()
    conn.close()
    print("Data berhasil dimuat ke PostgreSQL")

except Exception as e:
    print(f"Terjadi kesalahan: {e}")
