import csv
import json
import sys
import pyspark
from pyspark.sql import SparkSession

from pyproj import Transformer
from shapely.geometry import Point

def date_list(start,end,cbg):
    if start =='2019-03' or end == '2019-03': return [cbg,{},{},{}]
    elif start =='2019-10' or end == '2019-10': return [{},cbg,{},{}]
    elif start =='2020-03' or end == '2020-03': return [{},{},cbg,{}]
    elif start =='2020-10' or end == '2020-10': return [{},{},{},cbg]
    else: None

def merge_by_key(a,b):
    output = [{},{},{},{}]
    for i in range(len(a)):
        output[i].update(a[i])
        output[i].update(b[i])
    return output

def filter_cbg(dict_in,filter_list):
    output = []
    for dict_ in dict_in:
        if dict_ == {}: output.append('')
        else:
            dict_out = []
            for item in dict_:
                if item in filter_list:
                    dict_out.append((item,dict_[item]))
            if dict_out != []:  
                output.append(dict_out)
            else:
                output.append('')
    return output

def cbg_transfer(input,transfer_list):
    t = Transformer.from_crs(4326, 2263)
    if type(input) == list: 
        list_out = []
        for dict_ in input:
            if dict_ == '': list_out.append('')
            else:
                dict_out = []
                for item1 in dict_:
                    for item2 in transfer_list:
                        if item1[0] == item2[0]:
                            dict_out.append((t.transform(item2[1],item2[2]),item1[1]))
                list_out.append(dict_out)
        return list_out
    else:
        for item in transfer_list:
            if input == item[0]:
                return t.transform(item[1],item[2])

def distance(start_list,destination):
    output = []
    for item in start_list:
        if item == '':
            output.append('')
        else:
            distance_list=[]
            for start in item:
                distance_list.append((Point(start[0][0],start[0][1]).distance(Point(destination[0],destination[1]))/5280,start[1]))
            output.append(distance_list)
    return output

def mean(input):
    output = []
    for item in input:
        if item == '': output.append('')
        else:
            sum = 0
            count = 0
            for i in item:
                sum += i[0] * i[1]
                count += i[1]
            if count != 0:
                output.append(str(round(sum/count,2)))
    return output

def median(input):
    output = []
    for item in input:
        if item == '': output.append('')
        else:
            ls = []
            for i in item: ls.append(i[0] * i[1])
            ls_len = len(ls)
            ls.sort()
            index = (ls_len - 1)//2
            if (ls_len % 2): 
                output.append(str(round(ls[index],2)))
            else: 
                output.append(str(round((ls[index]+ls[index+1])/2,2)))
    return output
            
if __name__ == "__main__":
    sc = pyspark.SparkContext.getOrCreate()
    spark = SparkSession(sc)
    
    rdd_pattern = sc.textFile('weekly-patterns-nyc-2019-2020-sample.csv')
    #rdd_pattern = sc.textFile('/tmp/bdm/weekly-patterns-nyc-2019-2020').map(lambda x: next(csv.reader([x])))
    header = rdd_pattern.first()
    rdd_pattern = rdd_pattern.filter(lambda row : row != header) 
    rdd_filter = sc.textFile('nyc_supermarkets.csv')
    rdd_task1 = rdd_pattern.map(lambda x: [x[0], '-'.join(x[12].split('T')[0].split('-')[:2]), '-'.join(x[13].split('T')[0].split('-')[:2]), x[18], json.loads(x[19])])

    filter_list = rdd_filter.map(lambda x: x.split(',')[-2]).collect()
    rdd_task1 = rdd_task1.filter(lambda x: x[0] in filter_list)

    rdd_task2 = rdd_task1.map( lambda x: (x[3],date_list(x[1],x[2],x[4]))).filter(lambda x: x[1] is not None).reduceByKey(lambda x,y: merge_by_key(x,y))

    rdd_cbg = sc.textFile('nyc_cbg_centroids.csv')
    header2 = rdd_cbg.first()
    rdd_cbg = rdd_cbg.filter(lambda row : row != header2) 
    cbg_filter = rdd_cbg.map(lambda x: x.split(',')[0]).collect()

    rdd_task3 = rdd_task2.map(lambda x: [x[0],filter_cbg(x[1],cbg_filter)])

    rdd_cbg_list = rdd_cbg.map(lambda x: [x.split(',')[0],x.split(',')[1],x.split(',')[2]]).collect()

    rdd_task4 = rdd_task3.map(lambda x: [x[0],cbg_transfer(x[0],rdd_cbg_list),cbg_transfer(x[1],rdd_cbg_list)])

    rdd_task4 = rdd_task4.map(lambda x: [x[0],distance(x[2],x[1])])

    rdd_task5 = rdd_task4.map(lambda x: [x[0],mean(x[1])])

    df_out = rdd_task5.map(lambda x: [str(x[0]),str(x[1][0]),str(x[1][1]) ,str(x[1][2]),str(x[1][3])])\
            .toDF(['cbg_fips', '2019-03' , '2019-10' , '2020-03' , '2020-10'])\
            .sort('cbg_fips', ascending = True)

    df_out.coalesce(1).write.options(header='true').csv(sys.argv[1])