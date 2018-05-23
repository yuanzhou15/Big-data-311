def select_top(list_of_sorted):
    total_noise = 0
    grand_total = 0
    return_list = []
    total_types = 1
    for tup in list_of_sorted:
        grand_total+=tup[1]
        if 'Noise' in tup[0]:
            total_noise+=tup[1]
            continue
        else:
            total_types+= 1
            return_list.append(tup)
    print("total_noise", total_noise)
    print("grand_total", grand_total)
    print("Total complain types", total_types)
    return return_list
            
    
    
    
#test = select_com(complaint18).sort(key=lambda x: x[1])
selection = select_top(complaint17).sort(key=lambda x: x[1])
#selected = complaint16[-21:]

#final = partition_xy(selected)
#print(len(final[0]))
#print(final)



def select_top(list_of_sorted):
    total_noise = 0
    grand_total = 0
    return_list = []
    total_types = 1
    for tup in list_of_sorted:
        grand_total+=tup[1]
        if 'Noise' in tup[0]:
            total_noise+=tup[1]
            continue
        else:
            total_types+= 1
            return_list.append(tup)
    print("total_noise", total_noise)
    print("grand_total", grand_total)
    print("Total complain types", total_types)
    return list_of_sorted
select_top(complaintall)
short = complaintall[-23:]
#print(selection)

print(partition_xy(short))