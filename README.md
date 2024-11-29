sudo apt update
sudo apt install openjdk-11-jdk

sudo apt install python3 python3-pip


wget https://downloads apache.org/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz
   tar -xvf spark-3.2.0-bin-hadoop3.2.tgz
   sudo mv spark-3.2.0-bin-hadoop3.2 /opt/spark


bash   sudo mv spark-3.3.0-bin-hadoop3.2 /opt/spark   


nano ~/.bashrc

   
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin


source ~/.bashrc
   

pip install pyspark


python analyze_aeroflot.py
   
