import pandas as pd
import psycopg2

# 1. Extract: Membaca data dari file CSV
df = pd.read_csv('employee.csv')

# 2. Transform: Menghapus baris dengan nilai null dan menambahkan kolom baru
df = df.dropna()
df['full_name'] = df['first_name'] + ' ' + df['last_name']

# 3. Load: Memuat data ke PostgreSQL
conn = psycopg2.connect(
    database="pegawai",
    user="postgres",
    password="postgres"
)

cur = conn.cursor()

# Iterasi baris DataFrame dan memasukkan ke tabel PostgreSQL
for index, row in df.iterrows():
    cur.execute("INSERT INTO employee (first_name,last_name,full_name, age, city) VALUES (%s,%s,%s, %s, %s)",
                (row['first_name'],row['last_name'],row['full_name'], row['age'], row['city']))

conn.commit()
cur.close()
conn.close()

print("Data berhasil dimuat ke PostgreSQL")
