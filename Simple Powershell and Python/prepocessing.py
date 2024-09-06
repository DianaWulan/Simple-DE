import numpy
import pandas as pd

# Baca dataset
df = pd.read_csv('titanic.csv')

# Isi nilai yang hilang dengan rata-rata (contoh untuk kolom 'Age')
df['Age'].fillna(df['Age'].mean(), inplace=True)

# Hapus kolom yang tidak relevan (misalnya 'Cabin')
df.drop(columns=['Cabin'], inplace=True)

# Simpan data yang sudah diproses ke file baru
df.to_csv('titanic_processed.csv', index=False)
print("Data telah diproses dan disimpan ke 'titanic_processed.csv'.")