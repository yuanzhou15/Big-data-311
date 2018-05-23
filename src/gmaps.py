import gmaps
import gmaps.datasets
gmaps.configure(api_key="#") # My Google API key

fig = gmaps.figure()
fig.add_layer(gmaps.heatmap_layer(float_Loc))
fig

NYC_311 = '311_Service_Requests_from_2010_to_Present.csv'
requests = sc.textFile(NYC_311, use_unicode=False).cache()
#requests is a pyspark.rdd.RDD object

list(enumerate(requests.first().split(',')))

def get_Loc(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if row[39] == '':
            continue
        elif "2010" in row[1]:
            print(row[39])
            yield ((float(row[38]), float(row[39])))
        
allLoc = requests.mapPartitionsWithIndex(get_Loc)
final_list = allLoc.map(lambda x: x).collect()

def get_Loc(partId, list_of_records):
    lat_long = []
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if row[39] == '':
            continue
        elif "2010" in row[1]:
            print(row[39])
            lat_long.append((row[38], row[39]))

def change_to_float(list_of_tups):
    # This function will intake a list of tuples of str and make the strings into floats
    new_list = []
    for index in range(2024955):
        #print(list_of_tups[index][0])
        new_list.append((float(list_of_tups[index][0]), float(list_of_tups[index][1])))
    return new_list

float_Loc = change_to_float(loclist)
print("lenght", len(float_Loc))
print("\n")
print(float_Loc)

la_lo = a311_Service_Requests_from_2010_to_Present.csvllLoc.map(lambda x: float(x[39]))

import csv

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


new_list = []
with open('311_Service_Requests_from_2010_to_Present.csv') as fi:
    reader = csv.DictReader(fi)
    for row in reader:
        if row['Latitude'] == '':
            continue
        elif '2010' in row['Created Date']:
            new_list.append((float(row['Latitude']), float(row['Longitude'])))


def get_noise_loc17(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if row[39] == '':
            continue
        elif "2017" in row[1] and row[5]== 'Blocked Driveway':
            print(row[39])
            yield ((float(row[38]), float(row[39])))
            
def get_noise_loc18(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if row[39] == '':
            continue
        elif "2018" in row[1] and row[5]== 'Blocked Driveway':
            print(row[39])
            yield ((float(row[38]), float(row[39])))
        
complaints17 = requests.mapPartitionsWithIndex(get_noise_loc17)
final_list17 = complaints17.map(lambda x: x).collect()

complaints18 = requests.mapPartitionsWithIndex(get_noise_loc18)
final_list18 = complaints18.map(lambda x: x).collect()


fig2 = gmaps.figure()
fig2.add_layer(heatmap_layer17)
fig2.add_layer(heatmap_layer18)

