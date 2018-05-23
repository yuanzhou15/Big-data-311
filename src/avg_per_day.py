from pyspark import SparkContext

def dateMap(idx, part):
  if idx == 0:
    part.next()

  import csv
  for row in csv.reader(part):
    [date, _, _] = row[1].split(' ')
    yield (date, 1)

def callsMap(part):
  for row in part:
    yield (1, (1, row[1]))


def main(sc):
  FN = '311.csv'
  rdd = sc.textFile(FN, use_unicode=False)

  callsPerDay = rdd.mapPartitionsWithIndex(dateMap) \
                   .reduceByKey(lambda x, y: x+y)

  daysCalls = callsPerDay.mapPartitions(callsMap) \
                         .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

  daysCalls.saveAsTextFile('callsPerDay')


if __name__ == "__main__":
  sc = SparkContext()
  main(sc)
