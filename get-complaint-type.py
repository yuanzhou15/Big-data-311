def get_complaint_type10(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if year in row[1]:
            yield row[5]
            

def iterate(year):
    comtype10 = requests.mapPartitionsWithIndex(get_complaint_type10(year))
    complaint10= comtype10.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()



def get_complaint_all(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        yield row[5]
        
comtypeall = requests.mapPartitionsWithIndex(get_complaint_all)
complaintall= comtypeall.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()