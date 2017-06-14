# Crime Prediction
# Kmeans.py
#
# Standalone Python/Spark program to perform K means clustering.
# K = 50 
# Input data points are latitude and longitude
# output - writes to files in parts
#
# Usage: spark-submit Kmeans.py <inputdatafile>
# Example usage: spark-submit Kmeans.py Chicago_Crimes_updated.csv
#

import sys
import numpy as np
from pyspark import SparkContext
from numpy import array
from math import sqrt


# Given a latitude and longitude, calculate its distance from each centroid and find the cluster to
# which the point is closest to and return the key value (cluster number , [lat,long] )
def kmeans(line):
  latlong = np.array(line).astype('float')
  # initialize minimum distance to infinity
  min = float("inf")
  j = 1
  for i in centroidlist:
    dist = np.sqrt(sum((latlong - i) ** 2))	
    if dist < min:
	  min = dist
	  cluster = j
    j = j + 1
  return (cluster, latlong)
 
# Write data to CSV file.
def toCSVLine(data):
  return ','.join(str(d) for d in data) 

if __name__ == "__main__":
  if len(sys.argv) !=2:
    print >> sys.stderr, "Usage: spark-submit Kmeans.py <inputdatafile>"
    exit(-1)

  sc = SparkContext(appName="K means Clustering")

  # Input file has the historical data of crimes.
  # input = sc.textFile("Chicago_Crimes_updated.csv")
  input = sc.textFile(sys.argv[1])
  ###### Data Pre processing ##############
  # Get the header which indicates the names of the column
  header = input.first()
  inputWithoutHeader = input.filter(lambda line : line!= header)  
  rddRaw = inputWithoutHeader.map(lambda line: line.split(','))
  # delete the rows for which 21st cell (latitude) is empty. 21st column is 20th position
  rdd =  rddRaw.filter(lambda x: x[20] is not u'')
  # count final number of rows in rdd
  noOfRows = rdd.count()
  ######## K means Implementation ######################
  # Number of clusters which is decided from the elbow of WSSSE vs number of clusters plot
  k = 50
  # Number of iterations until centroids remain unchanged
  noOfIterations = 30
  # initialize centroid list with the random values from the data
  # fraction is expected size of the sample as a fraction of this rdd's size with replacement
  # get k+30 random rows. Extra rows because sample function doesnt guarantee the number of samples returned
  fraction = ((k+30)/(noOfRows*0.1)) * 0.1
  # rdd.sample(with replacement, fraction, seed)
  sample = rdd.sample(True, fraction, 2300)
  # out of the samples take first K rows. list rdd contains random k latitudes and longitudes
  list = sample.map(lambda line: (line[20], line[21])).take(k)
  centroidlist = np.array(list).astype('float')
  
  for i in range(noOfIterations):
    # Determine which point belongs to which cluster 
    rddCluster = rdd.map(lambda line: kmeans([line[20], line[21]]))
	# Calculate new set of centroids by grouping the points belonging to same cluster
	# and taking their average
    rddAgg = rddCluster.aggregateByKey((0,0), lambda a,b: (a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1]))
    rddAvg = rddAgg.mapValues(lambda v: v[0]/v[1])
    # http://stackoverflow.com/questions/29930110/calculating-the-averages-for-each-key-in-a-pairwise-k-v-rdd-in-spark-with-pyth
    # Re-initialize centroid list with average of cluster
    centroidlist = np.array(rddAvg.values().collect()).astype('float')
  
  # Final classification of the data points to the clusters
  rddFinal = rdd.map(lambda line: kmeans([line[20], line[21]]))
  rddFinal.collect()
  

  # write output to csv file. we get output in many parts but in same sequence as input
  lines = rddFinal.map(toCSVLine)
  lines.saveAsTextFile('ChicagoOut1')
  
  #hdfs dfs -copyToLocal ChicagoOut1.csv /users/sbilgund
  
  ##############################################
  #MLlib 
  #from pyspark.mllib.clustering import KMeans, KMeansModel
  #rddk = rdd.map(lambda line: (line[20], line[21])).collect()
  #kinput = np.array(rddk).astype('float')
  #model = KMeans.train(sc.parallelize(kinput), 7, maxIterations=14)
  #print("Final centers: " + str(model.clusterCenters))
  
  #sample = sc.parallelize(rdd.take(k))
  #list = sample.map(lambda line: (line[20], line[21])).collect()
  #centroidlist = np.array(list).astype('float')
  #model = KMeans.train(sc.parallelize(kinput), 7, maxIterations=0, initialModel = KMeansModel(centroidlist)
  
  ##############################################
  sc.stop()
