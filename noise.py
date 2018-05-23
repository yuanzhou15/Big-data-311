def get_noise_loc(partId, list_of_records):
    if partId == 0:
        list_of_records.next()
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        if row[39] == '':
            continue
        elif"2010" in row[1] and row[25] == "QUEENS" and 'Noise' in row[5]:
            print(row[39])
            yield ((float(row[38]), float(row[39])))
        
complaints = requests.mapPartitionsWithIndex(get_noise_loc)
#allCom.take(10)
final_list = complaints.map(lambda x: x).collect()
#base_com.sort(key=lambda x: x[1])
#print(len(base_com))