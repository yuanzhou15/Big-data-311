import sys
from pyspark import SparkContext

def extractNYPDClosed(partId, records):
    if partId==0:
        records.next()
    import csv
    reader = csv.reader(records)
    for row in reader:
        if row[3] == 'NYPD' and row[19] == 'Closed':
            (lat,lon) = (row[50],row[51])
            yield(lat,lon)
        else:
            records.next()
        
def main(sc):
    NYC311 = "/user/ywu004/311.csv"
    Serv = sc.textFile(NYC311, use_unicode=False).cache()
    #list(enumerate(sat.first().split(',')))
        
    NYPDLoc = Serv.mapPartitionsWithIndex(extractNYPDClosed) 
    NYPDC = NYPDLoc.collect()
    NYPDC.saveAsTextFile('311NYPDClosed')

if __name__ == '__main__':
    sc = SparkContext()
    main(sc)