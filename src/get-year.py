import pandas as pd

def get_time(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if "2018" in row[1]:
            yield row[25]

def output(rdd):      
	alltime = requests.mapPartitionsWithIndex(get_time)
	alltime.take(10)
	base_time = alltime.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect() 
	#print(base_time)


def csvRows(filename):
    with open(filename, 'r') as fi:
        reader = csv.DictReader(fi)
        for row in reader:
            yield row
float_list = []

for row in csvRows('311_Service_Requests_from_2010_to_Present.csv'):
    if '2010' in row['Created Date']:
        #float_list.append((float([row[38]]), float(row[39])))
        #print(type(float(row['Latitude'])))
        if row['Longitude'] == ''
        float_list.append((float(row['Latitude']), float(row['Longitude'])))