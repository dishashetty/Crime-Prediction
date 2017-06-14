# Crime Prediction
# LinearRegression.py
#
# Standalone Python/Spark program to perform linear regression.
# Input - original file, district
# Output - beta values
#
# Usage: spark-submit LinearRegression.py <inputdatafile> <district>
# Example usage: spark-submit LinearRegression.py Chicago_Crimes_updated.csv 14
#

import sys
import numpy as np

from pyspark import SparkContext

def getXList(line):
  beta = np.zeros(12, dtype=float)
  beta[0]=1
  #Using the month value
  if line[0]<=11:
    beta[line[0]]=1
  m=""
  # Adding number to string matrix m
  for number in beta:
    m=m+str(number)+";"
  # Removing the last semicolon
  m = m[:-1]
  # Converting to matrix float type format
  return np.matrix(m).astype('float')
  
def matrixMultiplicationXX(matX):
  # X Transpose
  matXTranspose = matX.transpose()
  # X * X Transpose
  result = matX * matXTranspose
  return ('keyA', result)

def matrixMultiplicationXY(y,x):
  matY = np.asmatrix(y).astype('float')
  # X * Y
  result = x * matY
  return ('keyB', result)
  
if __name__ == "__main__":
  if len(sys.argv) !=3:
    print >> sys.stderr, "Usage: spark-submit LinearRegression.py <inputdatafile> <district>"
    exit(-1)

  sc = SparkContext(appName="Linear Regression")

  # Input file has the historical data of crimes.
  # input = sc.textFile("Chicago_Crimes_updated.csv")
  input = sc.textFile(sys.argv[1])
  district = float(sys.argv[2])
  header = input.first()
  rdd = input.filter(lambda line : line!= header)  

  rddData = rdd.map(lambda line: line.split(','))

  # Keep records which satisfy the condititon line[12]==district
  # Filtering for records with given district 
  rddDistrict = rddData.filter(lambda line: line[12]==str(district))
  rddYear = rddDistrict.filter(lambda line: line[18] in ('2013','2014'))
  rddYear.first()
  rddDate = rddYear.map(lambda line: line[3])
  rddMonthYear=rddDate.map(lambda line: ((int(line[:2]), int(line[6:10])),1))
  rddCount=rddMonthYear.reduceByKey(lambda x,y : x+y )
  rddXX = rddCount.map(lambda line:getXList(line[0]))
  rddXXResult = rddXX.map(matrixMultiplicationXX)
  
  # Calculating summation of X * X transpose
  rddFirstTerm = rddXXResult.reduceByKey(lambda x,y : x+y)
  rddFirstTerm.collect()

  rddXY = rddCount.map(lambda line: matrixMultiplicationXY(line[1],getXList(line[0])))
  rddSecondTerm = rddXY.reduceByKey(lambda x,y : x+y)
  rddSecondTerm.collect()
  
  # Retrieving the first matrix value
  matXX = rddFirstTerm.values().first()
  
  # Getting the inverse
  matXXInv = np.asmatrix(np.linalg.inv(matXX))
  
  # Retrieving the second matrix value
  matXY = rddSecondTerm.values().first()
  
  # Multiplying first term with second 
  matResult = matXXInv * matXY


  betavalue = np.squeeze(np.asarray(matResult))



# Print the linear regression coefficients in desired output format
  print "betavalue: "
  for coeff in betavalue:
    print coeff
  
  

