def partition_xy(input_list): 
    #given a list, return a tuple of x values and y values
    x = []
    y = []
    for element in range(len(input_list)):
        x.append(input_list[element][0])
        y.append(input_list[element][1])
    return [x, y]
