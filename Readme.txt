Link for project : http://webpages.uncc.edu/dshetty1/ 

K means Clustering

- Implemented K-means clustering using pyspark
- Kmeans.py contains the source code

The following commands are used to run the application:

1) Put the input files and Kmeans.py file in home folder of dsba hadoop cluster using winscp or using the following commands:
	scp Chicago_Crimes_updated.csv sbilgund@dsba-hadoop.uncc.edu:/users/sbilgund
        scp Kmeans.py sbilgund@dsba-hadoop.uncc.edu:/users/sbilgund
	
2) Put the input files in HDFS using the following commands:
	hdfs dfs -put Chicago_Crimes_updated.csv
	
3) Change the directory to the folder containing spark-submit:
	cd /usr/lib/spark/bin
	
4) Run Kmeans.py for the input files using the commands below:
	spark-submit /users/sbilgund/Kmeans.py Chicago_Crimes_updated.csv

5) The output is written to folder named ChicagoKmeansOutput. Copy from hdfs to home folder of dsba hadoop cluster using the following command:
        hdfs dfs -copyToLocal ChicagoKmeansOutput /users/sbilgund

Naive Bayes 

- Implemented Naive Bayes using pyspark
- NaiveBayes.py contains the source code
- The output of K means is written to multiple files but in the same order as input.

1) Merge K means output with Chicago_Crimes_updated.csv. 
2) The input to Naive Bayes are in parts named Naive-part-00000.csv, Naive-part-00001.csv, Naive-part-00002.csv
3) Put the input files and NaiveBayes.py file in home folder of dsba hadoop cluster using winscp or using the following commands:
	scp Naive-part-00000.csv sbilgund@dsba-hadoop.uncc.edu:/users/sbilgund
	scp Naive-part-00001.csv sbilgund@dsba-hadoop.uncc.edu:/users/sbilgund
	scp Naive-part-00002.csv sbilgund@dsba-hadoop.uncc.edu:/users/sbilgund
        scp NaiveBayes.py sbilgund@dsba-hadoop.uncc.edu:/users/sbilgund
4) Put the input files in HDFS using the following commands:
	hdfs dfs -put Naive-part-00000.csv
	hdfs dfs -put Naive-part-00001.csv
	hdfs dfs -put Naive-part-00002.csv
5) Change the directory to the folder containing spark-submit:
	cd /usr/lib/spark/bin
	
4) Run NaiveBayes.py for the input files using the commands below:
	spark-submit /users/sbilgund/NaiveBayes.py 5 11 20


Linear Regression

- Implemented Linear Regression using pyspark for 2013 and 2014 data
- LinearRegression.py contains the source code
- Prints the beta values
- The model is tested for 2015 data in TestModelRegression.xls

The following commands are used to run the application:

1) Put the input files and LinearRegression.py  file in home folder of dsba hadoop cluster using winscp or using the following commands:
	scp Chicago_Crimes_updated.csv sbilgund@dsba-hadoop.uncc.edu:/users/sbilgund
        scp LinearRegression.py sbilgund@dsba-hadoop.uncc.edu:/users/sbilgund
	
2) Put the input files in HDFS using the following commands:
	hdfs dfs -put Chicago_Crimes_updated.csv
	
3) Change the directory to the folder containing spark-submit:
	cd /usr/lib/spark/bin
	
4) Run Kmeans.py for the input files using the commands below:
	spark-submit /users/sbilgund/LinearRegression.py Chicago_Crimes_updated.csv




