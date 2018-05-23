from pyspark import SparkContext

def dayMap(idx, part):
  if idx == 0:
    part.next()

  import csv
  for row in csv.reader(part):
    try:
      [date, time, AMPM] = row[1].split(' ')
      hour = int(time.split(':')[0])

      if AMPM == 'PM':
        if hour != 12:
          hour += 12
      else:
        if hour == 12:
          hour -= 12

      yield (hour, 1)
    except:
      continue

def main(sc):
  FN = '311.csv'
  rdd = sc.textFile(FN, use_unicode=False).cache()

  hourFreq = rdd.mapPartitionsWithIndex(dayMap) \
                .reduceByKey(lambda x, y: x+y) \
                .sortByKey()

  hourFreq.saveAsTextFile('hourFreq')


if __name__ == '__main__':
  sc = SparkContext()
  main(sc)
