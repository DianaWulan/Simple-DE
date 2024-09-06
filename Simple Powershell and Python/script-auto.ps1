# Step 1: Unduh dataset
$dataUrl = "https://path_to_dataset/titanic.csv"
$dataFile = "titanic.csv"

if (Test-Path $dataFile) {
    Write-Output "File $dataFile sudah ada."
} else {
    Write-Output "Mengunduh dataset Titanic..."
    Invoke-WebRequest -Uri $dataUrl -OutFile $dataFile
    Write-Output "Pengunduhan selesai."
}

# Step 2: Proses data menggunakan Python
python "C:\Users\diana\PowerShell\prepocessing.py"
Write-Output "Data telah diproses."

# Step 3: Analisis data menggunakan Python
python "C:\Users\diana\PowerShell\analisis.py"
Write-Output "Data telah dianalisis."