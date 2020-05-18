# pyspark_sequence_to_orc
Pyspark script to convert sequence file to orc format

Mention the sequence file input_path and orc file output_path in the script and run it using following command

=====================================================

PYTHONSTARTUP=PySpark_data_validation.py pyspark2 --master yarn --deploy-mode client --executor-memory=4g --num-executors=3 --executor-cores=2 --driver-memory=2g --name "Analysis-Spark"

======================================================
