## **Esecuzione job MapReduce (tempo impiegato circa 7min):**

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar \
-mapper git/BigDataApplications/job1/MapReduce/mapper.py \
-reducer git/BigDataApplications/job1/MapReduce/reducer.py \
-input /user/elisacatena/input/merged_data1.csv \
-output /user/elisacatena/output/MapReduce/job1/stock_statistics1
