@echo off
cd /d "%~dp0"
git remote remove origin 2>nul
git remote add origin https://github.com/sarayoussef2266/HDFS-And-Spark-Project.git
git push -u origin main --force
pause
