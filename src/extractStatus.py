import sys
from pyspark import SparkContext

def extractStatus(partId, records):
    if partId==0:
        records.next()
    import csv
    reader = csv.reader(records)
    for row in reader:
        (status,count) = (row[19],1)
        yield(status,count)
        
def main(sc):
    NYC311 = "/user/ywu004/311.csv"
    Serv = sc.textFile(NYC311, use_unicode=False).cache()
    #list(enumerate(sat.first().split(',')))
        
    AgenScores = Serv.mapPartitionsWithIndex(extractStatus) \
                .reduceByKey(lambda accum, n: accum + n)
    AgenScores.collect()
    AgenScores.saveAsTextFile('311Status')  

if __name__ == '__main__':
    sc = SparkContext()
    main(sc)