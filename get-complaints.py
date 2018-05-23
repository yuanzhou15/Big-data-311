def get_complaints(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if "2015" in row[1] and row[25] == "BROOKLYN":
            yield row[19]
            
allCom = requests.mapPartitionsWithIndex(get_complaints)
#allCom.take(10)
base_com = allCom.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()
#base_com.sort(key=lambda x: x[1])
#print(len(base_com))