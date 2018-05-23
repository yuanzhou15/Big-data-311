from pyspark import SparkContext

def dayMap(idx, part):
  if idx == 0:
    part.next()

  import csv
  from datetime import datetime

  for row in csv.reader(part):
    try:
      day = datetime.strptime(row[1], '%m/%d/%Y %H:%M:%S %p').weekday()
      yield (day, 1)
    except:
      continue

def main(sc):
  FN = '311.csv'
  rdd = sc.textFile(FN, use_unicode=False)

  rdd.take(2)
  dayFreq = rdd.mapPartitionsWithIndex(dayMap) \
               .reduceByKey(lambda x, y: x+y) \
               .sortByKey()

  dayFreq.saveAsTextFile('dayFreq')

if __name__ == "__main__":
  sc = SparkContext()
  main(sc)
