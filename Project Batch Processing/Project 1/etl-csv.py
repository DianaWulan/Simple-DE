import pandas as pd
import psycopg2

# 1. Extract: Membaca data dari file CSV
df = pd.read_csv('dataku/NEWspotify_tracks.csv')

# 2. Transform: Menghapus baris dengan nilai null dan menambahkan kolom baru

# 3. Load: Memuat data ke PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5431,
    database="spotify",
    user="postgres",
    password="postgresql"
)

cur = conn.cursor()

# Iterasi baris DataFrame dan memasukkan ke tabel PostgreSQL
for index, row in df.iterrows():
    cur.execute("INSERT INTO spotify_tracks (name, genre, artists, album, popularity, duration_ms) VALUES (%s,%s,%s,%s,%s,%s)",
                (row['name'],row['genre'], row['artists'], row['album'], row['popularity'], row['duration_ms']))

conn.commit()
cur.close()
conn.close()

print("Data berhasil dimuat ke PostgreSQL")
