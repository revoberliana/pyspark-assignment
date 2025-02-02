******Assignment Pyspark******
Create Airflow DAGs with Pyspark with Expected Architecture:

![image](https://github.com/user-attachments/assets/f28683ba-5c8f-44ff-968e-0fe75016d4fc)


**Create Spark Script** 🌍
Create Spark Sciprt in path /spark-scripts/pyspark-retail-1.py 
setelah docker build dockerfile.spark maka jar potsgres dapat digunakan untuk jdbc dengan menyesuaikan di script
bagian config spark : **_"spark.jars": "/opt/bitnami/spark/jars/postgresql-42.2.18.jar"_**,
Script terkait DAG yang menjalankan analisis retail menggunakan Spark pada data di PostgreSQL.menggunakan SparkSubmitOperator untuk mengirimkan script PySpark ke cluster Spark untuk dijalankan.


**Create DAG Script** 🎯
Create DAG script in /dags/spark-dag-retail-1.py
Script ini mendefinisikan DAG Airflow yang menjalankan script PySpark untuk analisis retail. Setiap hari (@daily), DAG ini akan mengeksekusi tugas yang menghubungkan ke cluster Spark dan menjalankan aplikasi yang terhubung ke database PostgreSQL untuk analisis lebih lanjut


**Final Conclusion** 📊




**Dependencies** 📚
Database: PostgreSQL
ETL Tool(orchestrator): Apache Airflow
Big data Porcessing: Spark
Libraries: airflow, datetime

About the Author 👩‍💻
Revo Berliana | 📧 Email: berlianarevo@Gmail.com | 🌐 LinkedIn: www.linkedin.com/in/revo-berliana-92232515a

Feel free to reach out with any questions or feedback! 😊
