1. Run `main.py`
2. Run: ```ps docker exec -it spark-master spark-submit `
   --master spark://spark-master:7077 `
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
   jobs/spark_processor.py

```