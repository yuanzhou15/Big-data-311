from pyspark import SparkContext

def monthMap(idx, part):
  if idx == 0:
    part.next()

  import csv
  for row in csv.reader(part):
    try:
      [date, _, _] = row[1].split(' ')
      [month, _, _] = date.split('/')
      yield (int(month)-1, 1)
    except:
      continue

def main(sc):
  FN = '311.csv'
  rdd = sc.textFile(FN, use_unicode=False)

  monthFreq = rdd.mapPartitionsWithIndex(monthMap) \
                 .reduceByKey(lambda x, y: x+y) \
                 .sortByKey()

  monthFreq.saveAsTextFile('monthFreq')

if __name__ == "__main__":
  sc = SparkContext()
  main(sc)
