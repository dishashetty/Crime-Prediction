# Crime Prediction
# NaiveBayes.py
#
# Standalone Python/Spark program to perform prediction of crime occurence probablility
# given weekday, month and hour as input
#  Prints top 10 areas with highest probability of Crime occurence
# Usage: spark-submit NaiveBayes.py <weekDay> <month> <hour>
# weekday : 1 - sunday, 2 - monday and so on
# month : 1- Jan, 12- Dec
# hour : 1-24
# Example usage: spark-submit NaiveBayes.py 5 9 12
#
import sys
import numpy as np
from pyspark import SparkContext
from numpy import array
from math import sqrt

# Emits (cluster, [weekDay, month, hour])
# here weekDay takes value 0 if it doesn't match with user weekDay value, 1 otherwise
# Similarly values are assigned to month and hour
def countOccurrences(area, weekDay, month, hour):
  list = []	
  list.append(1) if weekDay == testWeekDay else list.append(0)
  list.append(1) if month == testMonth else list.append(0)
  list.append(1) if hour == testHour else list.append(0)
  return (area, np.array(list))
  
# Emits (posteriorProbab, area)
# For each cluster/area, we calculate the probablity of crime occurence given 
# weekDay, month and hour
def calculatePosteriorProbab(area, wmhValues):
  # weekday count for given area
  wCount = wmhValues[0]
  mCount = wmhValues[1]
  hCount = wmhValues[2]
  if area in areaDict:
    aCount = areaDict[area]
  probabWGivenA = float(wCount+alpha)/float(aCount+ (alpha*distinctWeekdays))
  probabMGivenA = float(mCount)/float(aCount+ (alpha*distinctMonths))
  probabHGivenA = float(hCount)/float(aCount+ (alpha*distinctHours))
  probabA = float(aCount)/noOfRows
  posteriorProbab = probabWGivenA * probabMGivenA * probabHGivenA * probabA
  return (posteriorProbab, area)
  


if __name__ == "__main__":
  if len(sys.argv) !=4:
    print >> sys.stderr, "spark-submit NaiveBayes.py <weekDay> <month> <hour>"
    exit(-1)

  sc = SparkContext(appName="Naive Bayes")
  # Reads modified output of K means in parts
  input = sc.textFile("Naive-part-00000.csv,Naive-part-00001.csv,Naive-part-00002.csv")
  rdd = input.map(lambda line: line.split(','))
  # 1419631
  noOfRows = rdd.count()
  
  testWeekDay = sys.argv[1]
  testMonth = sys.argv[2]
  testHour = sys.argv[3]
  alpha = 1
  
  # Calculate number of distinct weekdays in the input file
  distinctWeekdays = rdd.map(lambda line: line[1]).distinct().count()
  # Calculate number of distinct month in the input file
  distinctMonths = rdd.map(lambda line: line[2]).distinct().count()
  # Calculate number of distinct hours in the input file
  distinctHours = rdd.map(lambda line: line[3]).distinct().count()
  
  rddOcurrences = rdd.map(lambda line: countOccurrences(int(line[6]), int(line[1]), int(line[2]), int(line[3])))
  # Eg: (33, array([1061,  605,  448]))
  # Reduce by cluster number to add up the array [weekDay, month, hour]
  rddCountsWMH = rddOcurrences.reduceByKey(lambda x,y : x+y)
  
  # Area dictionary which tells us the number of points belonging to each cluster
  # Eg:defaultdict(<type 'int'>, {1: 24203, 2: 77, ..., 50: 29839})
  areaDict = rddOcurrences.countByKey()
  
  rdd3 = rddCountsWMH.map(lambda line: calculatePosteriorProbab(line[0], line[1]))
  # take top 10 values in descending order
  list = rdd3.takeOrdered(10, key = lambda x: -x[0])
  
  # Prints top 10 areas out of 50 with highest crime probability
  for coeff in list:
    print coeff
  
  sc.stop()
