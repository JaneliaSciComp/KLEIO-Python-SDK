import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
import csv
import numpy as np
import os

names_index = {"initial" : 0,
       "created" : 2,
       "first_commit" : 3,
       "after_gc" : 5}

def filter_str(list_files, filter_str):
    result = []
    for f in list_files:
        if filter_str in f:
            result.append(f)
    return result
def filter_files(list_files,filters):
    if len(filters) ==0:
        return list_files
    if len(filters) == 1:
        return filter_str(list_files,filters[0])
    else:
        return filter_str(filter_files(list_files,filters[1:]),filters[0])

def get_size(file_name):
    file = open(file_name)
    csvreader = csv.reader(file)
    total = 0
    for row in csvreader:
        total = total +1
    file.close()
    return total
    
def get_header(file_name):
    file = open(file_name)
    csvreader = csv.reader(file)
    header = next(csvreader)[0].split(";")
    return header

def get_row_normalized(file_name,row_name):
    i = 0
    file = open(file_name)
    csvreader = csv.reader(file)
    idx = next(csvreader)[0].split(";").index(row_name)
    print("item index {} = {}".format(row_name,idx))
    x = []
    y = []
    first_val = float(next(csvreader)[0].split(";")[idx])
    print('first_val: {}'.format(first_val))
    for row in csvreader:
        i = i + 1
        itm = row[0].split(";")[idx]
        if not itm == '':
            x.append(i)
            y.append(first_val - float(itm))
    file.close()
    return np.array(x),np.array(y)

    
def plot_row(p, file_name,item,name = None,index_map={},index_range=None):
    if not item in get_header(file_name):
        print("Error {} not found in {}".format(item,file_name))
        return p

    if item == 'Remaining_space':
        x,row_i = get_row_normalized(file_name,item)
    else:
        x,row_i = get_row(file_name,item,index_map)
    if name == None:
        name = os.path.basename(file_name)
    if index_range is not None:
        print(index_range)
        x = x[index_range[0]:index_range[1]]
        row_i = row_i[index_range[0]:index_range[1]]
    p.plot(x, row_i, label = name)
    return p

def plot_all(title,files,column,filter_files,output_file,legend_map,index_map={},index_range = None):
    fig = plt.figure()
    fig.set_size_inches(20, 20)
    ax = plt.subplot(111)
    for f in files:
        if filter_files in f:
    #         print(f)
            plot_row(ax,f,column,name=legend_map[f],index_map=index_map,index_range=index_range)


    # ax.show()
    box = ax.get_position()
    ax.set_position([box.x0, box.y0 + box.height * 0.1,
                     box.width, box.height * 0.9])
    # plt.legend()
    plt.title(title)
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
              fancybox=True, shadow=True, ncol=1)
    plt.savefig(output_file ,dpi=100)
    plt.show()
    
def plot_all_multi(title,files,columns,filter_files,output_file,legend_map):
    fig = plt.figure()
    fig.set_size_inches(20, 20)
    ax = plt.subplot(111)
    for f in files:
        if filter_files in f:
            for column in columns:
                plot_row(ax,f,column,name="{}_{}".format(legend_map[f],column))


    # ax.show()
    box = ax.get_position()
    ax.set_position([box.x0, box.y0 + box.height * 0.1,
                     box.width, box.height * 0.9])
    # plt.legend()
    plt.title(title)
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
              fancybox=True, shadow=True, ncol=1)
    plt.savefig(output_file ,dpi=100)
    plt.show()
    
# Filter them
def get_all_length(list_files):
    result = []
    for f in list_files:
        size = get_size(f)
        result.append(size)
    return result
def show_length(list_files):
    for f in list_files:
        size = get_size(f)
#         print(f)
        print(size)

def filter_length(list_files,second_list, threshold_length):
    result = []
    result2 = []
    for i in range(len(list_files)):
        size = get_size(list_files[i])
        if size>threshold_length:
            result.append(list_files[i])
            result2.append(second_list[i])
            print("index: {}".format(i))
            print(list_files[i])
            print(size)
            print(get_size(second_list[i]))
#         else:
#             print(" Not {} {}".format(size,list_files[i]) )
    return result,result2

def print_file(file_name, names_index={}):
    file = open(file_name)
    csvreader = csv.reader(file)
    for row in csvreader:
        itm = row[0].split(";")
        for i in itm:
            if i in list(names_index.keys()):
                i = str(names_index[i])
            if i.isnumeric():
                i = "{:.0f}".format(int(i))
                print ("{:<5}".format(i), end = '\t')
            elif '.' in i:
                i = "{:.3f}".format(float(i))
                print ("{:<10}".format(i), end = '\t')
            elif "index" == i:
                print ("{:<5}".format(i), end = '\t')
            else:
                print ("{:<10}".format(i[:10]), end = '\t')
        print("\n")

def get_row(file_name,row_name,index_format={}):
    file = open(file_name)
    csvreader = csv.reader(file)
    idx = next(csvreader)[0].split(";").index(row_name)
    print("item index {} = {}".format(row_name,idx))
    x = []
    y = []
    for row in csvreader:
        itm = row[0].split(";")[idx]
        if not itm == '':
            val = float(itm)
            if val > 0:
                index = row[0].split(";")[0]
                if index in list(index_format.keys()):
                    index = index_format[index]
                else:
                    index = int(index)
                x.append(index)
                y.append(val)
    file.close()
    return np.array(x),np.array(y)