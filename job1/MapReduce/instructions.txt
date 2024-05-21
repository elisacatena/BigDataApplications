**Esecuzione primo job MapReduce (tempo impiegato circa 7min):**

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar \
    -input /user/elisacatena/input/historical_stock_prices.csv \
    -output /user/elisacatena/output/MapReduce/job1/stock_statistics \
    -mapper git/BigDataApplications/job1/MapReduce/mapper.py \
    -reducer git/BigDataApplications/job1/MapReduce/reducer.py \
    -file git/BigDataApplications/job1/MapReduce/mapper.py \
    -file git/BigDataApplications/job1/MapReduce/reducer.py 

**Esecuzione secondo job MapReduce (tempo impiegato circa 6sec):**

$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.3.4.jar \
    -input /user/elisacatena/output/MapReduce/job1/stock_statistics\
    -input /user/elisacatena/input/historical_stocks.csv \
    -output /user/elisacatena/output/MapReduce/job1/joined_output \
    -mapper git/BigDataApplications/job1/MapReduce/join_mapper.py \
    -reducer git/BigDataApplications/job1/MapReduce/join_reducer.py \
    -file git/BigDataApplications/job1/MapReduce/join_mapper.py \
    -file git/BigDataApplications/job1/MapReduce/join_reducer.py 
