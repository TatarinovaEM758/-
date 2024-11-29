sudo apt update
sudo apt install openjdk-11-jdk

sudo apt install python3 python3-pip


wget https://downloads.apache.org/spark/spark-3.4.3/spark-3.4.3-bin-hadoop3.tgz


 tar xvf spark-3.4.3-bin-hadoop3.tgz
 
 
 sudo mv spark-3.4.3-bin-hadoop3 /opt/spark


bash   sudo mv spark-3.3.0-bin-hadoop3.2 /opt/spark   


nano ~/.bashrc

   
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin


source ~/.bashrc
   

pip install pyspark


python analyze_aeroflot.py
   


5. Загрузка данных.
 Создайте директорию для данных и загрузите исторические данные по акциям (например,
Apple):
 ```bash
mkdir ~/spark_data
 cd ~/spark_data
 wget -O AAPL.csv
https://query1.finance.yahoo.com/v7/finance/download/AAPL?period1=0&period2=9999999999&in
terval=1d&events=history&includeAdjustedClose=true
 ```
или скачать по ссылке необходимый файл в Downloads и скопировать фвайл в необходимую
директорию.
 ```bash
cp ~/Downloads/ AAPL.csv ~/spark_data
 ```
6. Запуск Spark и выполнение простейших операций:
 Запустите PySpark:
 ```bash
 pyspark
 ```
 В интерактивной оболочке PySpark выполните следующие операции:
 a. Загрузка данных.
 ```python
 df = spark.rea
