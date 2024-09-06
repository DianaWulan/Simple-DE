# get url dataset titanic
$dataUrl = "https://github.com/datasciencedojo/datasets/blob/master/titanic.csv"
$dataFile = "titanic.csv"

# cek apakah file sudah ada
if (Test-Path $dataFile){
    write-output "File $dataFile sudah ada"
}else{
    write-output "mengunduh data..."
    Invoke-WebRequest -Uri $dataUrl -OutFile $dataFile
    write-output "pengunduhan selesai"
}