from pyspark import SparkContext

def dayMap2010(idx, part):
  if idx == 0:
    part.next()

  import csv
  from datetime import datetime

  for row in csv.reader(part):
    try:
      [date, _, _] = row[1].split(' ')
      [_, _, year] = date.split('/')

      if year == '2010':
        day = datetime.strptime(row[1], '%m/%d/%Y %H:%M:%S %p').weekday()
        yield (day, 1)
    except:
      continue

def dayMap2011(idx, part):
  if idx == 0:
    part.next()

  import csv
  from datetime import datetime

  for row in csv.reader(part):
    try:
      [date, _, _] = row[1].split(' ')
      [_, _, year] = date.split('/')

      if year == '2011':
        day = datetime.strptime(row[1], '%m/%d/%Y %H:%M:%S %p').weekday()
        yield (day, 1)
    except:
      continue

def dayMap2012(idx, part):
  if idx == 0:
    part.next()

  import csv
  from datetime import datetime

  for row in csv.reader(part):
    try:
      [date, _, _] = row[1].split(' ')
      [_, _, year] = date.split('/')

      if year == '2012':
        day = datetime.strptime(row[1], '%m/%d/%Y %H:%M:%S %p').weekday()
        yield (day, 1)
    except:
      continue

def dayMap2013(idx, part):
  if idx == 0:
    part.next()

  import csv
  from datetime import datetime

  for row in csv.reader(part):
    try:
      [date, _, _] = row[1].split(' ')
      [_, _, year] = date.split('/')

      if year == '2013':
        day = datetime.strptime(row[1], '%m/%d/%Y %H:%M:%S %p').weekday()
        yield (day, 1)
    except:
      continue

def dayMap2014(idx, part):
  if idx == 0:
    part.next()

  import csv
  from datetime import datetime

  for row in csv.reader(part):
    try:
      [date, _, _] = row[1].split(' ')
      [_, _, year] = date.split('/')

      if year == '2014':
        day = datetime.strptime(row[1], '%m/%d/%Y %H:%M:%S %p').weekday()
        yield (day, 1)
    except:
      continue

def dayMap2015(idx, part):
  if idx == 0:
    part.next()

  import csv
  from datetime import datetime

  for row in csv.reader(part):
    try:
      [date, _, _] = row[1].split(' ')
      [_, _, year] = date.split('/')

      if year == '2015':
        day = datetime.strptime(row[1], '%m/%d/%Y %H:%M:%S %p').weekday()
        yield (day, 1)
    except:
      continue

def dayMap2016(idx, part):
  if idx == 0:
    part.next()

  import csv
  from datetime import datetime

  for row in csv.reader(part):
    try:
      [date, _, _] = row[1].split(' ')
      [_, _, year] = date.split('/')

      if year == '2016':
        day = datetime.strptime(row[1], '%m/%d/%Y %H:%M:%S %p').weekday()
        yield (day, 1)
    except:
      continue

def dayMap2017(idx, part):
  if idx == 0:
    part.next()

  import csv
  from datetime import datetime

  for row in csv.reader(part):
    try:
      [date, _, _] = row[1].split(' ')
      [_, _, year] = date.split('/')

      if year == '2017':
        day = datetime.strptime(row[1], '%m/%d/%Y %H:%M:%S %p').weekday()
        yield (day, 1)
    except:
      continue

def main(sc):
  FN = '311.csv'
  rdd = sc.textFile(FN, use_unicode=False).cache()

  dayFreq = rdd.mapPartitionsWithIndex(dayMap2010) \
               .reduceByKey(lambda x, y: x+y) \
               .sortByKey()

  dayFreq.saveAsTextFile('weekday2010')
  dayFreq = rdd.mapPartitionsWithIndex(dayMap2011) \
               .reduceByKey(lambda x, y: x+y) \
               .sortByKey()

  dayFreq.saveAsTextFile('weekday2011')
  dayFreq = rdd.mapPartitionsWithIndex(dayMap2012) \
               .reduceByKey(lambda x, y: x+y) \
               .sortByKey()

  dayFreq.saveAsTextFile('weekday2012')
  dayFreq = rdd.mapPartitionsWithIndex(dayMap2013) \
               .reduceByKey(lambda x, y: x+y) \
               .sortByKey()

  dayFreq.saveAsTextFile('weekday2013')
  dayFreq = rdd.mapPartitionsWithIndex(dayMap2014) \
               .reduceByKey(lambda x, y: x+y) \
               .sortByKey()

  dayFreq.saveAsTextFile('weekday2014')
  dayFreq = rdd.mapPartitionsWithIndex(dayMap2015) \
               .reduceByKey(lambda x, y: x+y) \
               .sortByKey()

  dayFreq.saveAsTextFile('weekday2015')
  dayFreq = rdd.mapPartitionsWithIndex(dayMap2016) \
               .reduceByKey(lambda x, y: x+y) \
               .sortByKey()

  dayFreq.saveAsTextFile('weekday2016')
  dayFreq = rdd.mapPartitionsWithIndex(dayMap2017) \
               .reduceByKey(lambda x, y: x+y) \
               .sortByKey()

  dayFreq.saveAsTextFile('weekday2017')


if __name__ == "__main__":
  sc = SparkContext()
  main(sc)
