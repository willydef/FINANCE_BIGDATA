# Java 17
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"

# Python pour PySpark
$env:PYSPARK_PYTHON = "D:\FINANCE_BIGDATA\.venv\Scripts\python.exe"

Write-Host "FINANCE_BIGDATA : environnement configure avec succes."