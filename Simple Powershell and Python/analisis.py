import pandas as pd

#baca data yg sudah diproses
df = pd.read_csv("titanic_processed.csv")

#hitung rata2 usia penumpang
umur_rata2 = round(df['Age'].mean())
print(f"rata-rata usia penumpang : {umur_rata2} tahun")

#hitung persentase penumpang yg selamat
penumpang_selamat = round(df["Survived"].mean()*100)
print(f"persentase penumpang yg selamat : {penumpang_selamat}%")

#hitung jumlah penumpang berdasarkan kelas tiket
class_penumpang = df['Pclass'].value_counts()
print(f"jumlah penumpang selamat : \n{class_penumpang}")