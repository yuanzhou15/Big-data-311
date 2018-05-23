def get-any-boro(parId, list_of_records):
    if partId ==0:
        list_of_records.next()
    import csv
    reader - csv.reader(list_of_records)
    for row in reader:
        if year

def get_com_boroQ(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if "2010" in row[1] and row[25] == 'QUEENS':
            yield row[19]

        
def get_com_boroBr(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if "2010" in row[1] and row[25] == 'BRONX':
            yield row[19]
            
def get_com_boroU(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if "2010" in row[1] and row[25] == 'Unspecified':
            yield row[19]
            
def get_com_boroSI(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if "2010" in row[1] and row[25] == 'STATEN ISLAND':
            yield row[19]
            
def get_com_boroM(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if "2010" in row[1] and row[25] == 'MANHATTAN':
            yield row[19]

def get_com_boroBk(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if "2010" in row[1] and row[25] == 'BROOKLYN':
            yield row[19]


Q2016 = requests.mapPartitionsWithIndex(get_com_boroQ)
Q2016f = Q2016.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()
print("Q2016f (Queens): ",Q2016f)

Br2016 = requests.mapPartitionsWithIndex(get_com_boroBr)
Br2016f = Br2016.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()
print("Br2016f (Bronx): ",Br2016f)

U2016 = requests.mapPartitionsWithIndex(get_com_boroU)
U2016f = U2016.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()
print("U2016f (Unspecified): ",U2016f)

SI2016 = requests.mapPartitionsWithIndex(get_com_boroSI)
SI2016f = SI2016.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()
print("SI2016f (SI): ",SI2016f)

M2016 = requests.mapPartitionsWithIndex(get_com_boroM)
M2016f = M2016.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()
print("M2016f (Manhatttan): ",M2016f)

Bk2016 = requests.mapPartitionsWithIndex(get_com_boroBk)
Bk2016f = Bk2016.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()
print("Bk2016f (Brooklyn): ",Bk2016f)