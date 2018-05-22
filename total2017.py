from pyspark import SparkContext

def yearMap(idx, part):
  if idx == 0:
    part.next()

  import csv
  for row in csv.reader(part):
    try:
      [date, _, _] = row[1].split(' ')
      [_, _, year] = date.split('/')
      if year == "2017":
        yield (year, 1)

    except:
      continue

def main(sc):
  FN = "311.csv"
  rdd = sc.textFile(FN, use_unicode=False)

  total2017 = rdd.mapPartitionsWithIndex(yearMap) \
                 .reduceByKey(lambda x, y: x + y)

  total2017.saveAsTextFile("total2017")



if __name__ == "__main__":
  sc = SparkContext()
  main(sc)
