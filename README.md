********Assignment Pyspark********
Create Airflow DAGs with Pyspark with Expected Architecture:

![image](https://github.com/user-attachments/assets/f28683ba-5c8f-44ff-968e-0fe75016d4fc)


using template repo for docker-compose-spark.yml, docker-compose-airflow.yml , env and dockerfile 

**Create Spark Script** 🌍

Create Spark Script in path /spark-scripts/pyspark-retail-1.py 
setelah docker build dockerfile.spark maka jar driver potsgres sudah ke download dan tersimpan di container spark path /opt/bitnami/spark/jars/ 
bagian config spark : **_"spark.jars": "/opt/bitnami/spark/jars/postgresql-42.2.18.jar"_**,
melakukan proses agregasi 


**Create DAG Script** 🎯

Create DAG script in /dags/spark-dag-retail-1.py
Script ini mendefinisikan DAG Airflow yang menjalankan script PySpark untuk analisis retail. Setiap hari (@daily), DAG ini akan mengeksekusi tugas yang menghubungkan ke cluster Spark dan menjalankan aplikasi yang terhubung ke database PostgreSQL untuk analisis lebih lanjut


**Final Conclusion** 📊

saat proses menjalankan airflow masih belum bisa kehubung ke driver , driver jar sudah kedownload di container, config sudah dimasukkan ke script , masih mencari error dimana 



**Dependencies** 📚
Database: PostgreSQL
ETL Tool(orchestrator): Apache Airflow
Big data Porcessing: Spark
Libraries: airflow, datetime

About the Author 👩‍💻
Revo Berliana | 📧 Email: berlianarevo@Gmail.com | 🌐 LinkedIn: www.linkedin.com/in/revo-berliana-92232515a

