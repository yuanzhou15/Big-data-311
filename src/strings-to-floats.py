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
