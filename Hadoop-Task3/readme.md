#How-To Run#
Usage: LogAnalyzer <input path> <output path> <output format ('csv' (default) or 'snappy')>

Examples:

hadoop jar Task3-0.0.5-SNAPSHOT.jar BD.Hadoop.Task3.LogAnalyzer /input/task3/000000 /output/task3/000000-5

hadoop jar Task3-0.0.5-SNAPSHOT.jar BD.Hadoop.Task3.LogAnalyzer /input/task3/000000 /output/task3/000000-5 csv

hadoop jar Task3-0.0.5-SNAPSHOT.jar BD.Hadoop.Task3.LogAnalyzer /input/task3/000000 /output/task3/000000-5 snappy


#CSV results# (Screenshot 1)

![AVG and total ](./screenshots/1_csv_avg_total.png)


#Counters# (Screenshot 2)
Look at counters starting with BD.Hadoop.Task3.UserAgent

![Counters starts with BD.Hadoop.Task3.UserAgent](./screenshots/2_counters.png)


#Snappy files# (Screenshot 3)

How-To See:
hadoop fs -text /output/task3/000000-5/part-r-00000


![Snappy run command and result](./screenshots/3_snappy_results.png)